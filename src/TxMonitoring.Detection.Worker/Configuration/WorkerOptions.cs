using System.ComponentModel.DataAnnotations;

namespace TxMonitoring.Detection.Worker.Configuration;

public class KafkaOptions : IValidatableObject
{
    public const string Section = "Kafka";
    public const int MaximumReadinessTimeoutSeconds = 30;

    [Required(AllowEmptyStrings = false)]
    public string BootstrapServers { get; set; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    [StringLength(200)]
    public string GroupId { get; set; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    [RegularExpression("^[A-Za-z0-9._-]{1,249}$")]
    public string EventsTopic { get; set; } = string.Empty;

    // Web.Api reads the topic name from its own config. A hardcoded name here could drift from
    // it and leave the dashboard silently listening to an unused topic.
    [Required(AllowEmptyStrings = false)]
    [RegularExpression("^[A-Za-z0-9._-]{1,249}$")]
    public string AlertsTopic { get; set; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    [RegularExpression("^[A-Za-z0-9._-]{1,249}$")]
    public string DeadLetterTopic { get; set; } = string.Empty;

    [Range(1, MaximumReadinessTimeoutSeconds)]
    public int ReadinessTimeoutSeconds { get; set; }

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        var topics = new[] { EventsTopic, AlertsTopic, DeadLetterTopic };
        if (topics.Distinct(StringComparer.Ordinal).Count() != topics.Length)
        {
            yield return new ValidationResult(
                "Kafka event, alert, and dead-letter topics must be distinct.",
                [nameof(EventsTopic), nameof(AlertsTopic), nameof(DeadLetterTopic)]
            );
        }
    }
}

public class ConsumerOptions
{
    public const string Section = "Consumer";

    [Range(1, 3_600)]
    public int BaseBackoffSeconds { get; set; }

    [Range(1, 3_600)]
    public int MaxBackoffSeconds { get; set; }
}

public class OutboxOptions
{
    public const string Section = "Outbox";

    [Range(10, 60_000)]
    public int IdleDelayMs { get; set; }

    [Range(1, 3_600)]
    public int BaseBackoffSeconds { get; set; }

    [Range(1, 3_600)]
    public int MaxBackoffSeconds { get; set; }

    // Only acknowledged rows expire. Pending rows remain until Kafka accepts them.
    [Range(1, 87_600)]
    public int PublishedRetentionHours { get; set; }

    [Range(5, 3_600)]
    public int MaintenanceIntervalSeconds { get; set; }
}
