using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Feed.Emulator;
using TxMonitoring.Feed.Emulator.Configuration;
using TxMonitoring.Shared;

using Xunit.Abstractions;

namespace TxMonitoring.Detection.Tests;

/// <summary>
/// Replays every generator scenario through the production spike detector and risk policy, with
/// velocity windows computed exactly as the Redis Lua script does (inclusive [now - window, now],
/// with now = OccurredAt as in live steady state). Prints, per scenario, how often each rule fires
/// and how often review is recommended, then asserts each scenario exercises the rules its
/// documentation names. Kafka, Postgres, and the Redis round-trip are excluded.
/// </summary>
public sealed class ScenarioReplayTests(ITestOutputHelper output)
{
    private const int Accounts = 4; // docker-compose.yaml: Generator__AccountCount
    private const int IntervalSeconds = 5; // docker-compose.yaml: Generator__IntervalSeconds
    private const int EventsPerAccount = 600;

    [Theory]
    [InlineData(GeneratorScenario.Steady)]
    [InlineData(GeneratorScenario.HighAmount)]
    [InlineData(GeneratorScenario.AccountSpike)]
    [InlineData(GeneratorScenario.Burst)]
    [InlineData(GeneratorScenario.Combined)]
    public async Task Scenario_fires_the_rules_it_names(GeneratorScenario scenario)
    {
        var stats = await ReplayAsync(scenario);

        output.WriteLine(stats.ToString());

        // Warm-up (the first detector window per account) is excluded from the rates.
        switch (scenario)
        {
            case GeneratorScenario.Steady:
                // The spike detector's tail fires on a few percent of steady events, but a lone
                // spike scores 40 < 60: the policy needs corroboration before it alerts.
                Assert.Equal(0, stats.Reviews);
                Assert.Equal(0, stats.Fired(RiskPolicyEvaluator.AbsoluteHighRule));
                Assert.Equal(0, stats.Fired(RiskPolicyEvaluator.CountRule));
                Assert.Equal(0, stats.Fired(RiskPolicyEvaluator.SumRule));
                break;
            case GeneratorScenario.HighAmount:
                // Absolute-high (35) fires every time; four of them in five minutes pass the 1M
                // sum (40), so the run reviews even though no single rule would.
                Assert.Equal(stats.Scored, stats.Fired(RiskPolicyEvaluator.AbsoluteHighRule));
                Assert.Equal(stats.Scored, stats.Fired(RiskPolicyEvaluator.SumRule));
                Assert.Equal(0, stats.Fired(RiskPolicyEvaluator.CountRule));
                Assert.Equal(stats.Scored, stats.Reviews);
                break;
            case GeneratorScenario.AccountSpike:
                // Every 12th event is a 10x spike; the detector must see most of them, and a lone
                // spike (40) stays below review.
                Assert.True(
                    stats.ScheduledSpikesFlagged >= stats.ScheduledSpikes * 0.8,
                    stats.ToString()
                );
                Assert.Equal(0, stats.Reviews);
                Assert.Equal(0, stats.Fired(RiskPolicyEvaluator.AbsoluteHighRule));
                break;
            case GeneratorScenario.Burst:
                // The count rule fires from the first scored event; the sum rule waits for the
                // five-minute window to fill (~100 events at 2s), and review follows it.
                Assert.Equal(stats.Scored, stats.Fired(RiskPolicyEvaluator.CountRule));
                Assert.True(stats.Fired(RiskPolicyEvaluator.SumRule) > stats.Scored * 0.8);
                Assert.True(stats.Reviews > stats.Scored * 0.8, stats.ToString());
                break;
            case GeneratorScenario.Combined:
                Assert.Equal(stats.Scored, stats.Fired(RiskPolicyEvaluator.CountRule));
                Assert.True(stats.Fired(RiskPolicyEvaluator.AbsoluteHighRule) > 0);
                Assert.True(stats.Reviews > stats.Scored * 0.95, stats.ToString());
                break;
        }
    }

    private static async Task<ReplayStats> ReplayAsync(GeneratorScenario scenario)
    {
        using var detector = new AmountSpikeDetector(
            Path.Combine(AppContext.BaseDirectory, "model.zip")
        );
        var policy = new RiskPolicyOptions();
        var hash = RiskPolicyHash.Compute(policy);
        var interval = TimeSpan.FromSeconds(
            TransactionGeneratorWorker.EffectiveIntervalSeconds(IntervalSeconds, scenario)
        );
        var random = new Random(20261005);
        var start = DateTimeOffset.Parse("2026-10-05T00:00:00Z");
        var stats = new ReplayStats(scenario);
        static Task<IReadOnlyList<long>> NoHistory(CancellationToken _) =>
            Task.FromResult<IReadOnlyList<long>>([]);

        var windows = Enumerable
            .Range(0, Accounts)
            .Select(_ => new Queue<(DateTimeOffset, long)>())
            .ToArray();

        for (long seq = 1; seq <= EventsPerAccount; seq++)
        {
            for (var account = 0; account < Accounts; account++)
            {
                var accountId = $"ACCT-{account:D4}";
                var occurredAt = start + interval * seq;
                var amount = TransactionGeneratorWorker.AmountFor(scenario, seq, random.NextDouble());

                var spike = await detector.InspectAsync(accountId, "CAD", amount, NoHistory, CancellationToken.None);
                var velocity = Observe(windows[account], occurredAt, amount);
                var decision = RiskPolicyEvaluator.Evaluate(
                    new TransactionEvent(
                        Guid.CreateVersion7().ToString("D"),
                        accountId,
                        seq,
                        occurredAt,
                        occurredAt,
                        amount,
                        "CAD"
                    ),
                    spike,
                    velocity,
                    policy,
                    hash
                );

                if (seq > AmountSpikeDetector.HistoryLength)
                {
                    // AccountSpike schedules a 10x amount on every 12th event.
                    var scheduledSpike = scenario == GeneratorScenario.AccountSpike && seq % 12 == 0;
                    stats.Record(decision, scheduledSpike);
                }
            }
        }

        return stats;
    }

    // Mirrors ObserveScript: add, trim at now - 5m, count [now - 1m, now], sum [now - 5m, now].
    private static VelocitySnapshot Observe(
        Queue<(DateTimeOffset At, long Amount)> window,
        DateTimeOffset now,
        long amount
    )
    {
        window.Enqueue((now, amount));
        while (window.Peek().At <= now - TimeSpan.FromMinutes(5) && window.Peek().At != now)
            window.Dequeue();

        var count1m = window.Count(e => e.At >= now - TimeSpan.FromMinutes(1));
        var sum5m = window.Where(e => e.At >= now - TimeSpan.FromMinutes(5)).Sum(e => e.Amount);
        return VelocitySnapshot.Available(count1m, sum5m);
    }

    private sealed class ReplayStats(GeneratorScenario scenario)
    {
        private readonly Dictionary<string, int> _fired = new(StringComparer.Ordinal);

        public int Scored { get; private set; }
        public int Reviews { get; private set; }
        public int ScheduledSpikes { get; private set; }
        public int ScheduledSpikesFlagged { get; private set; }

        public int Fired(string rule) => _fired.GetValueOrDefault(rule);

        public void Record(RiskDecision decision, bool scheduledSpike)
        {
            Scored++;
            if (scheduledSpike)
            {
                ScheduledSpikes++;
                if (decision.Reasons.Any(r => r.Code == RiskPolicyEvaluator.AccountSpikeRule))
                    ScheduledSpikesFlagged++;
            }
            if (decision.ReviewRecommended)
                Reviews++;
            foreach (var reason in decision.Reasons)
                _fired[reason.Code] = Fired(reason.Code) + 1;
        }

        public override string ToString() =>
            $"{scenario,-12} scored {Scored,5}: review {Reviews * 100.0 / Scored,5:F1}% | "
            + $"absolute {Fired(RiskPolicyEvaluator.AbsoluteHighRule) * 100.0 / Scored,5:F1}% "
            + $"spike {Fired(RiskPolicyEvaluator.AccountSpikeRule) * 100.0 / Scored,5:F1}% "
            + $"count {Fired(RiskPolicyEvaluator.CountRule) * 100.0 / Scored,5:F1}% "
            + $"sum {Fired(RiskPolicyEvaluator.SumRule) * 100.0 / Scored,5:F1}%"
            + (ScheduledSpikes > 0
                ? $" | scheduled spikes flagged {ScheduledSpikesFlagged}/{ScheduledSpikes}"
                : "");
    }
}
