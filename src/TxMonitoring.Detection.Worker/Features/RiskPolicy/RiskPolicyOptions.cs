using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// Per-currency thresholds for the risk policy. The values are illustrative: they exercise
/// the demo scenarios and are not derived from loss data.
/// </summary>
public sealed class CurrencyRiskRules
{
    [Range(1, TransactionLimits.MaxAmountMinor)]
    public long AbsoluteHighAmount { get; set; } = 250_000;

    [Range(0, 100)]
    public int AbsoluteHighPoints { get; set; } = 35;

    [Range(0, 100)]
    public int AccountSpikePoints { get; set; } = 40;

    [Range(1, int.MaxValue)]
    public int Count1mThreshold { get; set; } = 20;

    [Range(0, 100)]
    public int Count1mPoints { get; set; } = 40;

    [Range(1, long.MaxValue)]
    public long Amount5mThreshold { get; set; } = 1_000_000;

    [Range(0, 100)]
    public int Amount5mPoints { get; set; } = 40;
}

public sealed class RiskPolicyOptions : IValidatableObject
{
    public const string Section = "RiskPolicy";

    [Required(AllowEmptyStrings = false)]
    public string PolicyVersion { get; set; } = "risk-policy-v1";

    /// <summary>Review is recommended at or above this score.</summary>
    [Range(0, 100)]
    public int ReviewThreshold { get; set; } = 60;

    /// <summary>Cap on the combined absolute-plus-spike amount contribution.</summary>
    [Range(0, 200)]
    public int AmountFamilyCap { get; set; } = 70;

    /// <summary>Cap on the combined count-plus-sum velocity contribution.</summary>
    [Range(0, 200)]
    public int VelocityFamilyCap { get; set; } = 70;

    [Required]
    public Dictionary<string, CurrencyRiskRules> RulesByCurrency { get; set; } = new()
    {
        ["CAD"] = new(),
    };

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        // Rules must cover exactly the accepted currencies. A gap or an extra entry is a
        // configuration bug.
        var configured = RulesByCurrency.Keys.ToHashSet(StringComparer.Ordinal);
        if (!configured.SetEquals(TransactionCurrencies.Supported))
        {
            yield return new ValidationResult(
                $"RiskPolicy rules must cover exactly the supported currencies: {string.Join(",", TransactionCurrencies.Supported)}.",
                [nameof(RulesByCurrency)]
            );
        }

        foreach (var (currency, rules) in RulesByCurrency)
        {
            var results = new List<ValidationResult>();
            if (
                !Validator.TryValidateObject(
                    rules,
                    new ValidationContext(rules),
                    results,
                    validateAllProperties: true
                )
            )
            {
                foreach (var result in results)
                {
                    yield return new ValidationResult(
                        $"{currency}: {result.ErrorMessage}",
                        result.MemberNames.Select(m => $"{nameof(RulesByCurrency)}[{currency}].{m}")
                    );
                }
            }
        }
    }
}

/// <summary>
/// Content hash of the effective policy configuration.
/// <see cref="RiskPolicyOptions.PolicyVersion"/> is not enough on its own because operators
/// can change configuration without changing the label. Tests check that every threshold,
/// point, cap, transform, and alert-threshold change alters the hash.
/// </summary>
public static class RiskPolicyHash
{
    public static string Compute(RiskPolicyOptions policy)
    {
        var builder = new StringBuilder();
        builder.Append(policy.PolicyVersion).Append('|');
        builder.Append(policy.ReviewThreshold.ToString(CultureInfo.InvariantCulture)).Append('|');
        builder.Append(policy.AmountFamilyCap.ToString(CultureInfo.InvariantCulture)).Append('|');
        builder.Append(policy.VelocityFamilyCap.ToString(CultureInfo.InvariantCulture));

        // Fixed field order and sorted currency keys make the hash independent of dictionary
        // ordering.
        foreach (var currency in policy.RulesByCurrency.Keys.OrderBy(k => k, StringComparer.Ordinal))
        {
            var rules = policy.RulesByCurrency[currency];
            builder
                .Append('|')
                .Append(currency)
                .Append(':')
                .Append(rules.AbsoluteHighAmount.ToString(CultureInfo.InvariantCulture))
                .Append(',')
                .Append(rules.AbsoluteHighPoints.ToString(CultureInfo.InvariantCulture))
                .Append(',')
                .Append(rules.AccountSpikePoints.ToString(CultureInfo.InvariantCulture))
                .Append(',')
                .Append(rules.Count1mThreshold.ToString(CultureInfo.InvariantCulture))
                .Append(',')
                .Append(rules.Count1mPoints.ToString(CultureInfo.InvariantCulture))
                .Append(',')
                .Append(rules.Amount5mThreshold.ToString(CultureInfo.InvariantCulture))
                .Append(',')
                .Append(rules.Amount5mPoints.ToString(CultureInfo.InvariantCulture));
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }
}
