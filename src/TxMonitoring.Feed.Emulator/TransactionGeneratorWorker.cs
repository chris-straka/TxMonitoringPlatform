using System.Threading.Channels;

using Microsoft.Extensions.Options;

using TxMonitoring.Feed.Emulator.Configuration;
using TxMonitoring.Feed.Emulator.Infrastructure;
using TxMonitoring.Shared;

namespace TxMonitoring.Feed.Emulator;

/// <summary>
/// LOOP 1: acquisition. Generates transactions and never touches the network.
/// Acts as the producer in the Producer/Consumer (P/C pattern).
///
/// IOptions&lt;T&gt; can only be injected from a built service provider (builder.Build())
/// </summary>
public class TransactionGeneratorWorker(
    Channel<TransactionSubmission> channel,
    IOptions<TransactionGeneratorOptions> generatorOptions,
    FeedMetrics metrics,
    ILogger<TransactionGeneratorWorker> logger
) : BackgroundService
{
    private readonly TransactionGeneratorOptions options = generatorOptions.Value;

    // One container simulates many accounts; a 1:1 container:account mapping would cost
    // 50-100MB of .NET runtime per account with no monitoring benefit.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // 0, 4, 8 (account count = 4)
        var firstAccount = options.ReplicaId!.Value * options.AccountCount;

        // Range(start, count) not (start, end)
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
        // Everything in here is per account, not per docker container
        var interval = TimeSpan.FromSeconds(
            EffectiveIntervalSeconds(options.IntervalSeconds, options.Scenario)
        );
        using var timer = new PeriodicTimer(interval);

        // Monotonic and used downstream to detect gaps. Restarting the generator resets
        // seq; a real origin would persist it.
        long seq = 0;

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            seq++;

            // Bounded jitter only; the pattern itself derives from seq so a scenario is
            // repeatable run to run.
            var noise = Random.Shared.NextDouble();

            var data = new TransactionSubmission(
                // v4 (Guid.NewGuid()) has random keys while v7 has time-based keys
                // Random keys dirty a different page in PG's DB index for every insert
                // If an arbitary page is full and a new random key needs to go in there
                // PG's DB index will split that page in half to add that random key
                // Time-based keys all land on the index's rightmost page, which stays cached.
                MessageId: Guid.CreateVersion7().ToString(),
                AccountId: accountId,
                SequenceNumber: seq,
                // Event time, truncated: the contract carries whole milliseconds and the
                // gateway rejects sub-millisecond precision as malformed.
                OccurredAt: TransactionTime.TruncateToMilliseconds(DateTimeOffset.UtcNow),
                AmountMinor: AmountFor(options.Scenario, seq, noise),
                Currency: "CAD"
            );

            metrics.Acquired.Add(1);

            // Non-blocking by design, if the buffer is full, drop the transaction.
            if (!channel.Writer.TryWrite(data))
                metrics.Dropped.Add(1);
        }
    }

    /// <summary>
    /// Per-account cadence for a scenario. Burst scenarios compress a slow configured
    /// cadence to 2s (30 events/min), which clears the 20/min count rule; every other
    /// scenario keeps the configured cadence, which stays below all velocity thresholds.
    /// </summary>
    internal static int EffectiveIntervalSeconds(int intervalSeconds, GeneratorScenario scenario) =>
        scenario is GeneratorScenario.Burst or GeneratorScenario.Combined
            ? Math.Min(intervalSeconds, TransactionGeneratorOptions.MaxBurstIntervalSeconds)
            : intervalSeconds;

    /// <summary>
    /// Deterministic amount schedule in integer minor units. The pattern derives from the
    /// sequence number; <paramref name="noise"/> (in [0,1)) only jitters within the
    /// documented bounds. Pure for testability: the steady ceiling is asserted below
    /// every review threshold.
    /// </summary>
    internal static long AmountFor(GeneratorScenario scenario, long seq, double noise) =>
        scenario switch
        {
            // $10-$50: below absolute (250k), count/sum velocity, and detector surprise.
            GeneratorScenario.Steady => 1_000 + (long)(noise * 4_000),
            // $3,000-$5,000: fires amount.absolute_high alone (sub-threshold evidence).
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
