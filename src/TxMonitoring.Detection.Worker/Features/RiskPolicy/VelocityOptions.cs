using System.ComponentModel.DataAnnotations;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

public sealed class VelocityOptions : IValidatableObject
{
    public const string Section = "Velocity";

    /// <summary>
    /// StackExchange.Redis configuration string. Redis is derived cache state: when it is
    /// unreachable every observation degrades to Unavailable instead of failing startup.
    /// </summary>
    public string Configuration { get; set; } = "localhost:6379";

    [Range(100, 30_000)]
    public int ConnectTimeoutMs { get; set; } = 5_000;

    [Range(100, 30_000)]
    public int SyncTimeoutMs { get; set; } = 5_000;

    /// <summary>Index TTL: 65 minutes covers the 5-minute window with eviction headroom.</summary>
    [Range(600, 86_400)]
    public int KeyTtlSeconds { get; set; } = 3_900;

    /// <summary>Maximum Postgres rows materialized into one cold window.</summary>
    [Range(1, 100_000)]
    public int LazyRebuildLimit { get; set; } = 1_000;

    /// <summary>Rows per repository page during a rebuild.</summary>
    [Range(1, 10_000)]
    public int WarmRebuildPageSize { get; set; } = 500;

    /// <summary>
    /// Time budget per rebuild page: a single cold key yields after this long so one
    /// rebuild cannot monopolize the consumer loop; the next event resumes it.
    /// </summary>
    [Range(1, 10_000)]
    public int RebuildMarkMs { get; set; } = 500;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (string.IsNullOrWhiteSpace(Configuration))
        {
            yield return new ValidationResult(
                "Velocity:Configuration must not be empty; point it at Redis or keep the localhost default for dev.",
                [nameof(Configuration)]
            );
        }
    }
}
