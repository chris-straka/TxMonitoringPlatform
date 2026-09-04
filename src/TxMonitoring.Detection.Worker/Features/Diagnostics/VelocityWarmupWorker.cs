using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using StackExchange.Redis;

using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// Best-effort startup acceleration for velocity windows: pages the most recent rows
/// through the repository shape and materializes in-window events per identity. It sets
/// no rebuilt flags, so each key's first touch still completes through the lazy path and
/// serves Rebuilding until genuinely caught up.
/// </summary>
public sealed class VelocityWarmupWorker(
    IDbContextFactory<AppDbContext> contextFactory,
    RedisConnection connection,
    IOptions<VelocityOptions> velocityOptions,
    TimeProvider timeProvider,
    ILogger<VelocityWarmupWorker> logger
) : BackgroundService
{
    private const long Window5mMs = 300_000;

    private sealed record WarmRow(
        Guid Id,
        Guid MessageId,
        string AccountId,
        string Currency,
        DateTimeOffset OccurredAt,
        long AmountMinor,
        DateTimeOffset DetectedAt
    );

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var multiplexer = await connection.GetAsync();
        if (multiplexer is null)
        {
            logger.LogInformation("Skipping velocity warmup: Redis is unreachable.");
            return;
        }

        var options = velocityOptions.Value;
        await using var db = await contextFactory.CreateDbContextAsync(stoppingToken);

        var loaded = 0;
        Guid? lastId = null;
        DateTimeOffset? lastDetectedAt = null;

        // Keyset pages through detection order; each page is one bounded query, never a scan.
        while (loaded < options.LazyRebuildLimit && !stoppingToken.IsCancellationRequested)
        {
            IQueryable<MonitoredTransaction> query = db.Transactions;
            if (lastId is not null)
            {
                var lastIdValue = lastId.Value;
                var lastDetectedAtValue = lastDetectedAt!.Value;
                query = query.Where(r =>
                    r.DetectedAt < lastDetectedAtValue
                    || (r.DetectedAt == lastDetectedAtValue && r.Id.CompareTo(lastIdValue) < 0)
                );
            }

            var page = await query
                .OrderByDescending(r => r.DetectedAt)
                .ThenByDescending(r => r.Id)
                .Take(options.WarmRebuildPageSize)
                .Select(r => new WarmRow(
                    r.Id,
                    r.MessageId,
                    r.AccountId,
                    r.Currency,
                    r.OccurredAt,
                    r.AmountMinor,
                    r.DetectedAt
                ))
                .ToListAsync(stoppingToken);

            if (page.Count == 0)
            {
                break;
            }

            await MaterializeAsync(multiplexer.GetDatabase(), page, options, stoppingToken);

            loaded += page.Count;
            lastId = page[^1].Id;
            lastDetectedAt = page[^1].DetectedAt;

            if (page.Count < options.WarmRebuildPageSize)
            {
                break;
            }
        }

        logger.LogInformation("Velocity warmup materialized {Count} recent rows.", loaded);
    }

    private async Task MaterializeAsync(
        IDatabase db,
        IReadOnlyList<WarmRow> page,
        VelocityOptions options,
        CancellationToken cancellationToken
    )
    {
        var nowMs =
            (timeProvider.GetUtcNow().Ticks - DateTime.UnixEpoch.Ticks)
            / TimeSpan.TicksPerMillisecond;
        var floorMs = nowMs - Window5mMs;
        var ttl = TimeSpan.FromSeconds(options.KeyTtlSeconds);

        var watermarks = new Dictionary<string, long>();
        var batch = db.CreateBatch();
        var tasks = new List<Task>();

        foreach (var row in page)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var occurredMs =
                (row.OccurredAt.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks)
                / TimeSpan.TicksPerMillisecond;
            if (occurredMs < floorMs)
            {
                continue;
            }

            var baseKey = $"vel:{row.AccountId}:{row.Currency}";
            var member = $"{occurredMs}:{row.MessageId:D}";
            tasks.Add(batch.SortedSetAddAsync($"{baseKey}:events", member, occurredMs));
            tasks.Add(batch.HashSetAsync($"{baseKey}:amounts", member, row.AmountMinor));
            tasks.Add(batch.KeyExpireAsync($"{baseKey}:events", ttl));
            tasks.Add(batch.KeyExpireAsync($"{baseKey}:amounts", ttl));
            tasks.Add(batch.KeyExpireAsync($"{baseKey}:wm", ttl));
            watermarks[baseKey] = Math.Max(
                watermarks.TryGetValue(baseKey, out var current) ? current : -1,
                occurredMs
            );
        }

        foreach (var (baseKey, watermark) in watermarks)
        {
            tasks.Add(
                batch.ScriptEvaluateAsync(
                    RedisVelocityStore.WatermarkScript,
                    [baseKey + ":wm"],
                    [watermark, options.KeyTtlSeconds]
                )
            );
        }

        batch.Execute();
        await Task.WhenAll(tasks);
    }
}
