using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Tests;

public sealed class RiskPolicyTests
{
    [Fact]
    public void Steady_event_scores_zero_and_needs_no_review()
    {
        // The default generator scenario after detector warm-up: small amount, no spike,
        // no velocity evidence. This is the below-threshold proof against live config.
        var decision = Evaluate(
            amountMinor: 2_500,
            spike: NoSpike(historyCount: 25),
            velocity: VelocitySnapshot.Unavailable()
        );

        Assert.Equal(0, decision.RiskScore);
        Assert.False(decision.ReviewRecommended);
        Assert.Empty(decision.Reasons);
        Assert.Equal("risk-policy-v1", decision.PolicyVersion);
        Assert.NotEmpty(decision.PolicyConfigurationHash);
    }

    [Fact]
    public void Absolute_high_alone_persists_evidence_below_the_threshold()
    {
        var decision = Evaluate(
            amountMinor: 300_000,
            spike: NoSpike(historyCount: 25),
            velocity: VelocitySnapshot.Unavailable()
        );

        Assert.Equal(35, decision.RiskScore);
        Assert.False(decision.ReviewRecommended);
        var reason = Assert.Single(decision.Reasons);
        Assert.Equal("amount.absolute_high", reason.Code);
        Assert.Equal("amount_minor", reason.Metric);
        Assert.Equal("300000", reason.ObservedValue);
        Assert.Equal("250000", reason.Threshold);
        Assert.Equal(35, reason.Points);
    }

    [Fact]
    public void Absolute_plus_warm_spike_reaches_review_through_the_family_cap()
    {
        var decision = Evaluate(
            amountMinor: 400_000,
            spike: Spike(historyCount: 25, rawScore: 8.5),
            velocity: VelocitySnapshot.Unavailable()
        );

        // 35 + 40 = 75, capped at the 70 amount-family contribution.
        Assert.Equal(70, decision.RiskScore);
        Assert.True(decision.ReviewRecommended);
        Assert.Equal(
            ["amount.absolute_high", "amount.account_spike"],
            decision.Reasons.Select(r => r.Code)
        );
    }

    [Fact]
    public void Velocity_burst_reaches_review_without_any_amount_signal()
    {
        var decision = Evaluate(
            amountMinor: 2_500,
            spike: NoSpike(historyCount: 25),
            velocity: VelocitySnapshot.Available(count1m: 30, amount5mMinor: 1_500_000)
        );

        // 40 + 40 = 80, capped at the 70 velocity-family contribution.
        Assert.Equal(70, decision.RiskScore);
        Assert.True(decision.ReviewRecommended);
    }

    [Fact]
    public void Cold_detector_history_cannot_fire_the_spike_rule()
    {
        var decision = Evaluate(
            amountMinor: 2_500,
            spike: Spike(historyCount: 5, rawScore: 99.0),
            velocity: VelocitySnapshot.Unavailable()
        );

        Assert.Equal(0, decision.RiskScore);
        Assert.False(decision.ReviewRecommended);
    }

    [Fact]
    public void Nonfinite_detector_scores_never_fire_the_spike_rule()
    {
        var decision = Evaluate(
            amountMinor: 2_500,
            spike: new AmountSpikeResult(
                true,
                double.NaN,
                double.PositiveInfinity,
                25,
                "test-version"
            ),
            velocity: VelocitySnapshot.Unavailable()
        );

        Assert.Equal(0, decision.RiskScore);
    }

    [Fact]
    public void Combined_event_fires_reasons_in_fixed_rule_order()
    {
        var decision = Evaluate(
            amountMinor: 400_000,
            spike: Spike(historyCount: 30, rawScore: 9.1),
            velocity: VelocitySnapshot.Available(count1m: 30, amount5mMinor: 1_500_000)
        );

        Assert.Equal(
            [
                "amount.absolute_high",
                "amount.account_spike",
                "velocity.count_1m",
                "velocity.amount_5m",
            ],
            decision.Reasons.Select(r => r.Code)
        );
        Assert.Equal(100, decision.RiskScore);
        Assert.True(decision.ReviewRecommended);
    }

    [Fact]
    public void Score_exactly_at_the_threshold_is_review_recommended()
    {
        var policy = DefaultPolicy();
        policy.ReviewThreshold = 35;

        // 300_000 trips amount.absolute_high for exactly 35 points.
        var decision = RiskPolicyEvaluator.Evaluate(
            SteadyEvent() with { AmountMinor = 300_000 },
            NoSpike(historyCount: 25),
            VelocitySnapshot.Unavailable(),
            policy,
            RiskPolicyHash.Compute(policy)
        );

        Assert.Equal(35, decision.RiskScore);
        Assert.True(decision.ReviewRecommended);
    }

    [Fact]
    public void Policy_hash_is_stable_but_any_effective_change_changes_it()
    {
        var baseline = RiskPolicyHash.Compute(DefaultPolicy());
        Assert.Equal(baseline, RiskPolicyHash.Compute(DefaultPolicy()));
        Assert.Equal(64, baseline.Length);

        string Mutate(Action<CurrencyRiskRules> change)
        {
            var mutated = DefaultPolicy();
            change(mutated.RulesByCurrency["CAD"]);
            return RiskPolicyHash.Compute(mutated);
        }

        Assert.NotEqual(baseline, Mutate(r => r.AbsoluteHighAmount++));
        Assert.NotEqual(baseline, Mutate(r => r.AbsoluteHighPoints++));
        Assert.NotEqual(baseline, Mutate(r => r.AccountSpikePoints++));
        Assert.NotEqual(baseline, Mutate(r => r.Count1mThreshold++));
        Assert.NotEqual(baseline, Mutate(r => r.Count1mPoints++));
        Assert.NotEqual(baseline, Mutate(r => r.Amount5mThreshold++));
        Assert.NotEqual(baseline, Mutate(r => r.Amount5mPoints++));

        var capped = DefaultPolicy();
        capped.AmountFamilyCap++;
        Assert.NotEqual(baseline, RiskPolicyHash.Compute(capped));

        var relabeled = DefaultPolicy();
        relabeled.PolicyVersion = "risk-policy-v1-renamed";
        Assert.NotEqual(baseline, RiskPolicyHash.Compute(relabeled));
    }

    [Fact]
    public void Velocity_evidence_requires_both_fields_and_unavailable_carries_none()
    {
        var available = VelocitySnapshot.Available(30, 1_500_000);
        Assert.Equal(VelocityStatus.Available, available.Status);
        Assert.Equal(30, available.Count1m);
        Assert.Equal(1_500_000, available.Amount5mMinor);

        foreach (
            var degraded in new[]
            {
                VelocitySnapshot.Unavailable(),
                VelocitySnapshot.OutOfOrder(),
                VelocitySnapshot.Rebuilding(),
            }
        )
        {
            Assert.Null(degraded.Count1m);
            Assert.Null(degraded.Amount5mMinor);
        }
    }

    private static RiskDecision Evaluate(
        long amountMinor,
        AmountSpikeResult spike,
        VelocitySnapshot velocity
    )
    {
        var policy = DefaultPolicy();
        return RiskPolicyEvaluator.Evaluate(
            SteadyEvent() with { AmountMinor = amountMinor },
            spike,
            velocity,
            policy,
            RiskPolicyHash.Compute(policy)
        );
    }

    private static TransactionEvent SteadyEvent() =>
        new(
            Guid.CreateVersion7().ToString("D"),
            "ACCT-0001",
            42,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            2_500,
            "CAD"
        );

    private static AmountSpikeResult NoSpike(int historyCount) =>
        new(false, 0.5, 0.4, historyCount, "test-version");

    private static AmountSpikeResult Spike(int historyCount, double rawScore) =>
        new(true, rawScore, 0.001, historyCount, "test-version");

    private static RiskPolicyOptions DefaultPolicy() => new();
}
