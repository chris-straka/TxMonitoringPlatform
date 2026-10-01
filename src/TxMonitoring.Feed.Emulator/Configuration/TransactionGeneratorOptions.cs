using System.ComponentModel.DataAnnotations;

namespace TxMonitoring.Feed.Emulator.Configuration;

public class GatewayOptions
{
    public const string Section = "Gateway";

    [Required(AllowEmptyStrings = false)]
    [Url]
    public string Url { get; set; } = string.Empty;
}

/// <summary>
/// Deterministic load scenarios. The default steady cadence stays below every velocity
/// threshold, so the demo starts quiet and each other scenario exercises a named rule.
/// </summary>
public enum GeneratorScenario
{
    /// <summary>
    /// 1k–5k minor units at the configured cadence. Below the absolute, spike (after warm-up),
    /// count, and sum thresholds, so no review is expected.
    /// </summary>
    Steady,

    /// <summary>
    /// 300k–500k minor units at the configured cadence. Fires amount.absolute_high only, which
    /// alone scores below review, so the evidence is persisted without an alert.
    /// </summary>
    HighAmount,

    /// <summary>
    /// 2k–4k baseline with every 12th event at 20k–40k. The spike stays below the absolute
    /// threshold so only the per-account detector fires.
    /// </summary>
    AccountSpike,

    /// <summary>
    /// 8k–12k minor units at up to 2s per-account cadence. Trips both velocity rules (count and
    /// five-minute sum).
    /// </summary>
    Burst,

    /// <summary>
    /// Burst cadence with every 10th event at 400k: amount and velocity families combine.
    /// </summary>
    Combined,
}

public class TransactionGeneratorOptions : IValidatableObject
{
    public const string Section = "Generator";

    [Range(1, 100_000)]
    public int AccountCount { get; set; }

    /// <summary>
    /// Per-account cadence in seconds. Burst scenarios cap it at
    /// <see cref="MaxBurstIntervalSeconds"/>.
    /// </summary>
    [Range(1, 3_600)]
    public int IntervalSeconds { get; set; }

    public const int MaxBurstIntervalSeconds = 2;

    // Channel capacity before TryWrite starts dropping. The configured value is in
    // appsettings.json and compose.
    [Range(1, 10_000_000)]
    public int BufferCapacity { get; set; }

    [Required]
    [Range(0, 10_000)]
    public int? ReplicaId { get; set; }

    public GeneratorScenario Scenario { get; set; } = GeneratorScenario.Steady;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (!Enum.IsDefined(Scenario))
        {
            yield return new ValidationResult(
                $"Generator:Scenario '{Scenario}' is not a known scenario.",
                [nameof(Scenario)]
            );
        }
    }
}
