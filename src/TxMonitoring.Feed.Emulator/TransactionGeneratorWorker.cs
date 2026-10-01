using System.Threading.Channels;

using Microsoft.Extensions.Options;

using TxMonitoring.Feed.Emulator.Configuration;
using TxMonitoring.Feed.Emulator.Infrastructure;
using TxMonitoring.Shared;

namespace TxMonitoring.Feed.Emulator;

/// <summary>
/// Generates transactions into the channel without touching the network.
/// </summary>
public class TransactionGeneratorWorker(
    Channel<TransactionSubmission> channel,
    IOptions<TransactionGeneratorOptions> generatorOptions,
    FeedMetrics metrics,
    ILogger<TransactionGeneratorWorker> logger
) : BackgroundService
{
    private readonly TransactionGeneratorOptions options = generatorOptions.Value;

    // One container simulates many accounts. A container per account would cost 50-100 MB of
    // .NET runtime each with no monitoring benefit.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Replicas own disjoint account ranges.
        var firstAccount = options.ReplicaId!.Value * options.AccountCount;

        var accounts = Enumerable
            .Range(firstAccount, options.AccountCount)
            .Select(n => $"ACCT-{n:D4}")
            .ToArray();

        logger.LogInformation(
            "Generation started. Accounts {First}..{Last} every {Interval}s ({Scenario}).",
            accounts[0],
            accounts[^1],
            EffectiveIntervalSeconds(options.IntervalSeconds, options.Scenario),
            options.Scenario
        );

        await Task.WhenAll(accounts.Select(id => RunAccountAsync(id, stoppingToken)));
    }

    private async Task RunAccountAsync(string accountId, CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(
            EffectiveIntervalSeconds(options.IntervalSeconds, options.Scenario)
        );
        using var timer = new PeriodicTimer(interval);

        // Monotonic and used downstream to detect gaps. It resets on restart; a real origin
        // would persist it.
        long seq = 0;

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            seq++;

            // Only jitter is random. The pattern derives from seq so a scenario repeats run to run.
            var noise = Random.Shared.NextDouble();

            var data = new TransactionSubmission(
                // v7 rather than v4: time-ordered keys append to the rightmost page of the
                // Postgres unique index instead of dirtying and splitting random pages.
                MessageId: Guid.CreateVersion7().ToString(),
                AccountId: accountId,
                SequenceNumber: seq,
                // The contract carries whole milliseconds, and the gateway rejects finer precision.
                OccurredAt: TransactionTime.TruncateToMilliseconds(DateTimeOffset.UtcNow),
                AmountMinor: AmountFor(options.Scenario, seq, noise),
                Currency: "CAD"
            );

            metrics.Acquired.Add(1);

            // Drop rather than wait when the channel is full.
            if (!channel.Writer.TryWrite(data))
                metrics.Dropped.Add(1);
        }
    }

    /// <summary>
    /// Per-account cadence for a scenario. Burst scenarios cap the interval at 2s (30 events/min),
    /// enough to trip the 20/min count rule. Other scenarios keep the configured cadence, which
    /// stays below every velocity threshold.
    /// </summary>
    internal static int EffectiveIntervalSeconds(int intervalSeconds, GeneratorScenario scenario) =>
        scenario is GeneratorScenario.Burst or GeneratorScenario.Combined
            ? Math.Min(intervalSeconds, TransactionGeneratorOptions.MaxBurstIntervalSeconds)
            : intervalSeconds;

    /// <summary>
    /// Deterministic amount schedule in integer minor units. The pattern derives from the
    /// sequence number, and <paramref name="noise"/> in [0,1) only jitters within each band.
    /// Pure so tests can assert the steady ceiling stays below every review threshold.
    /// </summary>
    internal static long AmountFor(GeneratorScenario scenario, long seq, double noise) =>
        scenario switch
        {
            // $10-$50: below absolute (250k), count/sum velocity, and detector surprise.
            GeneratorScenario.Steady => 1_000 + (long)(noise * 4_000),
            // $3,000-$5,000: fires amount.absolute_high alone, which is below the review score.
            GeneratorScenario.HighAmount => 300_000 + (long)(noise * 200_000),
            // Baseline $20-$40 with every 12th at $200-$400: isolates the per-account
            // detector spike below the absolute threshold.
            GeneratorScenario.AccountSpike => seq % 12 == 0
                ? 20_000 + (long)(noise * 20_000)
                : 2_000 + (long)(noise * 2_000),
            // $80-$120 at burst cadence: ~30 events/min trips the count rule and the
            // five-minute sum (~1.5M) trips the sum rule.
            GeneratorScenario.Burst => 8_000 + (long)(noise * 4_000),
            // Burst cadence with every 10th at $4,000: amount and velocity families combine.
            GeneratorScenario.Combined => seq % 10 == 0
                ? 400_000
                : 8_000 + (long)(noise * 4_000),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
        };
}
