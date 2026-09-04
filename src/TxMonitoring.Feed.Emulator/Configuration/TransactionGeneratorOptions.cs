using System.ComponentModel.DataAnnotations;

namespace TxMonitoring.Feed.Emulator.Configuration;

public class GatewayOptions
{
    public const string Section = "Gateway";

    // These annotations run at runtime (after environment vars are provided)
    [Required(AllowEmptyStrings = false)]
    [Url]
    public string Url { get; set; } = string.Empty;
}

/// <summary>
/// Deterministic load scenarios. Amounts stay inside documented bounds and the default
/// steady cadence stays below every velocity threshold, so the demo starts quiet and each
/// scenario exercises a named rule or combination.
/// </summary>
public enum GeneratorScenario
{
    /// <summary>
    /// 1k–5k minor units at the configured cadence. Below the absolute, spike (after
    /// warm-up), count, and sum thresholds: no review expected.
    /// </summary>
    Steady,

    /// <summary>
    /// 300k–500k minor units at the configured cadence. Fires amount.absolute_high only,
    /// which is below the review threshold alone: sub-threshold evidence is persisted.
    /// </summary>
    HighAmount,

    /// <summary>
    /// 2k–4k baseline with every 12th event at 20k–40k. The spike stays below the absolute
    /// threshold to isolate the per-account detector signal.
    /// </summary>
    AccountSpike,

    /// <summary>
    /// 8k–12k minor units at up to 2s per-account cadence. Trips both velocity rules
    /// (count and five-minute sum) for a velocity-family alert.
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
    /// Per-account cadence in seconds. Burst scenarios compress this down to
    /// <see cref="MaxBurstIntervalSeconds"/>; the default steady cadence stays below
    /// every velocity threshold.
    /// </summary>
    [Range(1, 3_600)]
    public int IntervalSeconds { get; set; }

    public const int MaxBurstIntervalSeconds = 2;

    // Number of submissions in the Channel before TryWrite starts dropping them.
    // This is only a validation range, see appsettings.json / compose for the configured value
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
