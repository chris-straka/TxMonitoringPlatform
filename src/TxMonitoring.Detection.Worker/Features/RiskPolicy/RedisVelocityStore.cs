using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;

using Microsoft.Extensions.Options;

using StackExchange.Redis;

using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// Sliding-window velocity over Redis sorted sets, observed atomically.
/// </summary>
/// <remarks>
/// One observation mutates several structures (add, trim, count, sum, watermark, TTL).
/// Separate commands would let concurrent replicas interleave into torn windows, so the
/// whole step is one Lua script: it executes atomically server-side in a single round-trip.
/// Windows use OccurredAt, so draining an outage never manufactures a burst from old
/// events; an event older than the watermark is OutOfOrder and is never added.
/// </remarks>
public sealed partial class RedisVelocityStore(
    IOptions<VelocityOptions> velocityOptions,
    ITransactionHistoryRepository history,
    RedisConnection connection,
    TimeProvider timeProvider,
    WorkerMetrics metrics,
    ILogger<RedisVelocityStore> logger
) : IVelocityStore
{
    private const long Window1mMs = 60_000;
    private const long Window5mMs = 300_000;

    // Keys are interpolated from validated identities only; anything outside the admission
    // charset degrades instead of risking key injection.
    [GeneratedRegex("^[A-Za-z0-9._:-]+$")]
    private static partial Regex AccountKeyRegex();

    [GeneratedRegex("^[A-Z]{3}$")]
    private static partial Regex CurrencyKeyRegex();

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _rebuildLocks = new();

    /// <summary>
    /// KEYS: watermark string. ARGV: candidate ms, ttl sec. Advances the watermark only
    /// upward, atomically: a stale rebuild page can never regress it.
    /// </summary>
    internal const string WatermarkScript =
        """
        local wm = redis.call('GET', KEYS[1])
        if (wm == false) or (tonumber(ARGV[1]) > tonumber(wm)) then
          redis.call('SET', KEYS[1], ARGV[1])
        end
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        """;

    /// <summary>
    /// KEYS: events zset, amounts hash, watermark string, rebuilt flag.
    /// ARGV: member, occurred ms, amount, 1m window ms, 5m window ms, now ms, ttl sec,
    /// rebuilt-complete flag. Returns {status, count1m, sum5m}: status 0 added, 1 out-of-order.
    /// </summary>
    internal const string ObserveScript =
        """
        if tonumber(ARGV[2]) < tonumber(redis.call('GET', KEYS[3]) or '-1') then
          return {1, 0, 0}
        end
        redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
        local trim_before = tonumber(ARGV[6]) - tonumber(ARGV[5])
        local old = redis.call('ZRANGEBYSCORE', KEYS[1], 0, trim_before)
        if #old > 0 then
          redis.call('ZREM', KEYS[1], unpack(old))
          redis.call('HDEL', KEYS[2], unpack(old))
        end
        local wm = redis.call('GET', KEYS[3])
        if (wm == false) or (tonumber(ARGV[2]) > tonumber(wm)) then
          redis.call('SET', KEYS[3], ARGV[2])
        end
        if ARGV[8] == '1' then
          redis.call('SET', KEYS[4], '1')
        end
        redis.call('EXPIRE', KEYS[1], ARGV[7])
        redis.call('EXPIRE', KEYS[2], ARGV[7])
        redis.call('EXPIRE', KEYS[3], ARGV[7])
        redis.call('EXPIRE', KEYS[4], ARGV[7])
        local count = redis.call('ZCOUNT', KEYS[1], tonumber(ARGV[6]) - tonumber(ARGV[4]), ARGV[6])
        local members = redis.call('ZRANGEBYSCORE', KEYS[1], tonumber(ARGV[6]) - tonumber(ARGV[5]), ARGV[6])
        local sum = 0
        for _, m in ipairs(members) do
          sum = sum + tonumber(redis.call('HGET', KEYS[2], m))
        end
        return {0, count, sum}
        """;

    public async Task<VelocitySnapshot> ObserveAsync(
        string accountId,
        string currency,
        Guid messageId,
        long amountMinor,
        DateTimeOffset occurredAt,
        CancellationToken cancellationToken
    )
    {
        if (!AccountKeyRegex().IsMatch(accountId) || !CurrencyKeyRegex().IsMatch(currency))
        {
            return Degraded("identity outside the key charset");
        }

        var multiplexer = await connection.GetAsync();
        if (multiplexer is null)
        {
            return Degraded("no Redis connection");
        }

        var occurredMs = ToEpochMs(occurredAt);
        var member = $"{occurredMs}:{messageId:D}";
        var baseKey = $"vel:{accountId}:{currency}";
        var db = multiplexer.GetDatabase();

        try
        {
            // Fast path: skip the add when the event predates the watermark.
            var watermark = await db.StringGetAsync($"{baseKey}:wm");
            if (watermark.HasValue && occurredMs < (long)watermark)
            {
                return VelocitySnapshot.OutOfOrder();
            }

            var caughtUp = await EnsureRebuiltAsync(
                db,
                baseKey,
                accountId,
                currency,
                cancellationToken
            );

            var nowMs = ToEpochMs(timeProvider.GetUtcNow());
            var result = (RedisResult[])await db.ScriptEvaluateAsync(
                ObserveScript,
                [$"{baseKey}:events", $"{baseKey}:amounts", $"{baseKey}:wm", $"{baseKey}:rebuilt"],
                [member, occurredMs, amountMinor, Window1mMs, Window5mMs, nowMs, velocityOptions.Value.KeyTtlSeconds, caughtUp ? 1 : 0]
            );

            if ((long)result[0] == 1)
            {
                // The watermark advanced between the pre-check and the script: the event is
                // genuinely older than what this window already absorbed.
                return VelocitySnapshot.OutOfOrder();
            }

            if (!caughtUp)
            {
                return VelocitySnapshot.Rebuilding();
            }

            return VelocitySnapshot.Available((long)result[1], (long)result[2]);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Degraded(ex.Message);
        }
    }

    /// <summary>
    /// Materializes a cold window from Postgres through the repository, one account/currency
    /// at a time. Returns true when the window is fully caught up (fewer rows than the
    /// limit); a capped load leaves the rebuilt flag unset so the next event resumes with
    /// Rebuilding instead of serving a partial window as Available.
    /// </summary>
    private async Task<bool> EnsureRebuiltAsync(
        IDatabase db,
        string baseKey,
        string accountId,
        string currency,
        CancellationToken cancellationToken
    )
    {
        if (await db.KeyExistsAsync($"{baseKey}:rebuilt"))
        {
            return true;
        }

        var gate = _rebuildLocks.GetOrAdd(baseKey, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken);
        try
        {
            if (await db.KeyExistsAsync($"{baseKey}:rebuilt"))
            {
                return true;
            }

            var started = Stopwatch.GetTimestamp();
            var rows = await history.LoadRecentAsync(
                accountId,
                currency,
                velocityOptions.Value.LazyRebuildLimit,
                cancellationToken
            );
            var complete = rows.Count < velocityOptions.Value.LazyRebuildLimit;

            var nowMs = ToEpochMs(timeProvider.GetUtcNow());
            var floorMs = nowMs - Window5mMs;
            long watermark = -1;

            // Only in-window rows materialize; older history warms the detector path instead.
            var batch = db.CreateBatch();
            var batchTasks = new List<Task>(rows.Count * 2 + 3);
            foreach (var (messageId, occurredAt, amountMinor) in rows)
            {
                var occurredMs = ToEpochMs(occurredAt);
                if (occurredMs < floorMs)
                {
                    continue;
                }
                var member = $"{occurredMs}:{messageId:D}";
                batchTasks.Add(batch.SortedSetAddAsync($"{baseKey}:events", member, occurredMs));
                batchTasks.Add(batch.HashSetAsync($"{baseKey}:amounts", member, amountMinor));
                watermark = Math.Max(watermark, occurredMs);
            }

            if (watermark >= 0)
            {
                batchTasks.Add(
                    batch.ScriptEvaluateAsync(
                        WatermarkScript,
                        [$"{baseKey}:wm"],
                        [watermark, velocityOptions.Value.KeyTtlSeconds]
                    )
                );
            }

            var ttl = TimeSpan.FromSeconds(velocityOptions.Value.KeyTtlSeconds);
            batchTasks.Add(batch.KeyExpireAsync($"{baseKey}:events", ttl));
            batchTasks.Add(batch.KeyExpireAsync($"{baseKey}:amounts", ttl));
            batchTasks.Add(batch.KeyExpireAsync($"{baseKey}:wm", ttl));
            batch.Execute();
            await Task.WhenAll(batchTasks);

            var elapsedMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            if (elapsedMs > velocityOptions.Value.RebuildMarkMs)
            {
                logger.LogWarning(
                    "Velocity rebuild for {AccountId}/{Currency} took {ElapsedMs:F0}ms over budget; resuming on the next event.",
                    accountId,
                    currency,
                    elapsedMs
                );
                return false;
            }

            if (complete)
            {
                await db.StringSetAsync(
                    $"{baseKey}:rebuilt",
                    "1",
                    TimeSpan.FromSeconds(velocityOptions.Value.KeyTtlSeconds)
                );
            }

            return complete;
        }
        finally
        {
            gate.Release();
        }
    }

    private VelocitySnapshot Degraded(string reason)
    {
        metrics.VelocityUnavailable.Add(1);
        logger.LogDebug("Velocity degraded ({Reason}); scoring from remaining signals.", reason);
        return VelocitySnapshot.Unavailable();
    }

    private static long ToEpochMs(DateTimeOffset value) =>
        (value.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond;
}
