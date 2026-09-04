using System.Globalization;
using System.Text.Json.Serialization;

using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// One fired rule, in the table's fixed rule order, with its metric, invariant observed
/// value, threshold, and awarded points. Amounts stay base-10 strings so aggregates that
/// exceed JavaScript's safe integer still round-trip exactly.
/// </summary>
public sealed record RiskReason(
    [property: JsonPropertyName("code")] string Code,
    [property: JsonPropertyName("metric")] string Metric,
    [property: JsonPropertyName("observedValue")] string ObservedValue,
    [property: JsonPropertyName("threshold")] string Threshold,
    [property: JsonPropertyName("points")] int Points
);

/// <summary>
/// The immutable policy decision for one transaction: a transparent ranking score, not a
/// calibrated fraud probability. ReviewRecommended means "emit a monitoring alert".
/// </summary>
public sealed record RiskDecision(
    int RiskScore,
    bool ReviewRecommended,
    IReadOnlyList<RiskReason> Reasons,
    string PolicyVersion,
    string PolicyConfigurationHash
);

/// <summary>
/// Deterministic v1 policy: correlated amount signals (absolute, account spike) sum within
/// one family cap, correlated velocity signals (count, sum) within another, and the two
/// family contributions add with checked arithmetic capped at 100. Every fired reason is
/// persisted even below the alert threshold.
/// </summary>
public static class RiskPolicyEvaluator
{
    public const string AbsoluteHighRule = "amount.absolute_high";
    public const string AccountSpikeRule = "amount.account_spike";
    public const string CountRule = "velocity.count_1m";
    public const string SumRule = "velocity.amount_5m";

    public static RiskDecision Evaluate(
        TransactionEvent @event,
        AmountSpikeResult spike,
        VelocitySnapshot velocity,
        RiskPolicyOptions policy,
        string policyConfigurationHash
    )
    {
        if (!policy.RulesByCurrency.TryGetValue(@event.Currency, out var rules))
        {
            throw new ArgumentException(
                $"No risk rules configured for currency {@event.Currency}.",
                nameof(@event)
            );
        }

        var reasons = new List<RiskReason>(4);

        // Fixed rule order: absolute, spike, count, sum.
        if (@event.AmountMinor >= rules.AbsoluteHighAmount)
        {
            reasons.Add(
                new RiskReason(
                    AbsoluteHighRule,
                    "amount_minor",
                    @event.AmountMinor.ToString(CultureInfo.InvariantCulture),
                    rules.AbsoluteHighAmount.ToString(CultureInfo.InvariantCulture),
                    rules.AbsoluteHighPoints
                )
            );
        }

        // The spike rule needs a warm detector history: a cold reference window cannot
        // distinguish a real deviation from an uninitialized baseline.
        if (
            spike.IsSpike
            && spike.HistoryCount >= AmountSpikeDetector.HistoryLength
            && double.IsFinite(spike.RawScore)
            && double.IsFinite(spike.PValue)
        )
        {
            reasons.Add(
                new RiskReason(
                    AccountSpikeRule,
                    "detector_raw_score",
                    spike.RawScore.ToString("G17", CultureInfo.InvariantCulture),
                    "warm_history:" + AmountSpikeDetector.HistoryLength.ToString(
                        CultureInfo.InvariantCulture
                    ),
                    rules.AccountSpikePoints
                )
            );
        }

        // Velocity rules need available evidence. Anything else (unavailable, out-of-order,
        // rebuilding) is missing evidence, never zero counts: the rules simply do not fire.
        if (velocity.Status == VelocityStatus.Available)
        {
            if (velocity.Count1m!.Value >= rules.Count1mThreshold)
            {
                reasons.Add(
                    new RiskReason(
                        CountRule,
                        "velocity_count_1m",
                        velocity.Count1m.Value.ToString(CultureInfo.InvariantCulture),
                        rules.Count1mThreshold.ToString(CultureInfo.InvariantCulture),
                        rules.Count1mPoints
                    )
                );
            }

            if (velocity.Amount5mMinor!.Value >= rules.Amount5mThreshold)
            {
                reasons.Add(
                    new RiskReason(
                        SumRule,
                        "velocity_amount_5m_minor",
                        velocity.Amount5mMinor.Value.ToString(CultureInfo.InvariantCulture),
                        rules.Amount5mThreshold.ToString(CultureInfo.InvariantCulture),
                        rules.Amount5mPoints
                    )
                );
            }
        }

        var amountFamily = Math.Min(
            reasons.Where(r => r.Code is AbsoluteHighRule or AccountSpikeRule).Sum(r => r.Points),
            policy.AmountFamilyCap
        );
        var velocityFamily = Math.Min(
            reasons.Where(r => r.Code is CountRule or SumRule).Sum(r => r.Points),
            policy.VelocityFamilyCap
        );
        var score = checked(Math.Min(amountFamily + velocityFamily, 100));

        return new RiskDecision(
            score,
            score >= policy.ReviewThreshold,
            reasons,
            policy.PolicyVersion,
            policyConfigurationHash
        );
    }
}
