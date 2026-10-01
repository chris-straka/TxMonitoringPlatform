using System.Diagnostics;

using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Shared;

using Xunit.Abstractions;

namespace TxMonitoring.Detection.Tests;

/// <summary>
/// In-process policy throughput over a mix of quiet, absolute-high, and review cases. Asserts
/// repeatability and prints decisions/s. Detector warm-up, Redis, Kafka, and Postgres are
/// excluded, so the number is a ceiling, not an end-to-end figure.
/// </summary>
public sealed class PolicyThroughputTests
{
    private const int Decisions = 200_000;

    private readonly ITestOutputHelper output;

    public PolicyThroughputTests(ITestOutputHelper output)
    {
        this.output = output;
    }

    [Fact]
    public void Policy_evaluation_throughput_with_repeatability_guard()
    {
        var policy = new RiskPolicyOptions();
        var hash = RiskPolicyHash.Compute(policy);
        var velocity = VelocitySnapshot.Unavailable();

        var first = Evaluate(2_500, policy, hash, velocity);
        var sw = Stopwatch.StartNew();
        var totalScore = 0L;
        for (var i = 0; i < Decisions; i++)
        {
            // Cycle the rule mix: quiet, absolute-high, spike review.
            var amount = (i % 3) switch
            {
                0 => 2_500,
                1 => 300_000,
                _ => 400_000,
            };
            var spike = (i % 3) == 2
                ? new AmountSpikeResult(true, 8.5, 0.001, 25, "bench")
                : new AmountSpikeResult(false, 0.5, 0.4, 25, "bench");
            totalScore += Evaluate(amount, policy, hash, velocity, spike).RiskScore;
        }
        sw.Stop();

        // Compared field by field because Reasons is a list, and record equality would compare
        // its reference.
        var repeat = Evaluate(2_500, policy, hash, velocity);
        Assert.Equal(first.RiskScore, repeat.RiskScore);
        Assert.Equal(first.ReviewRecommended, repeat.ReviewRecommended);
        Assert.Equal(first.PolicyVersion, repeat.PolicyVersion);
        Assert.Equal(first.PolicyConfigurationHash, repeat.PolicyConfigurationHash);
        Assert.Equal(
            first.Reasons.Select(r => (r.Code, r.Metric, r.ObservedValue, r.Threshold, r.Points)),
            repeat.Reasons.Select(r => (r.Code, r.Metric, r.ObservedValue, r.Threshold, r.Points))
        );
        Assert.True(totalScore > 0);

        var perSecond = Decisions / sw.Elapsed.TotalSeconds;
        output.WriteLine(
            $"policy: {Decisions} decisions in {sw.Elapsed.TotalSeconds:F2}s = "
            + $"{perSecond:F0} decisions/s "
            + $"({sw.Elapsed.TotalNanoseconds / Decisions:F0} ns/decision, "
            + $"hash {hash[..12]}…)"
        );
    }

    private static RiskDecision Evaluate(
        long amountMinor,
        RiskPolicyOptions policy,
        string hash,
        VelocitySnapshot velocity,
        AmountSpikeResult? spike = null
    )
    {
        var @event = new TransactionEvent(
            Guid.CreateVersion7().ToString("D"),
            "ACCT-0001",
            42,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            amountMinor,
            "CAD"
        );
        return RiskPolicyEvaluator.Evaluate(
            @event,
            spike ?? new AmountSpikeResult(false, 0.5, 0.4, 25, "bench"),
            velocity,
            policy,
            hash
        );
    }
}
