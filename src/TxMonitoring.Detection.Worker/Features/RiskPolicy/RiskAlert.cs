using System.Text.Json.Serialization;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// Velocity evidence as published. Aggregates can exceed JavaScript's safe integer range even
/// though a single AmountMinor cannot, so both values travel as base-10 strings. Null when
/// velocity was unavailable.
/// </summary>
public sealed record VelocityEvidence(
    [property: JsonPropertyName("count1m")] string Count1m,
    [property: JsonPropertyName("amount5mMinor")] string Amount5mMinor
);

public sealed record DetectorEvidence(
    [property: JsonPropertyName("isSpike")] bool IsSpike,
    [property: JsonPropertyName("rawScore")] double RawScore,
    [property: JsonPropertyName("pValue")] double PValue,
    [property: JsonPropertyName("historyCount")] int HistoryCount,
    [property: JsonPropertyName("detectorVersion")] string DetectorVersion
);

/// <summary>
/// The versioned risk-alerts payload. Changing a shape or meaning needs a schema version
/// decision and updated golden fixtures. SequenceNumber and AmountMinor stay JSON numbers
/// because their maxima are JavaScript-safe. The UI maps reason codes to fixed phrases, and
/// trace context travels in Kafka headers.
/// </summary>
public sealed record RiskAlert(
    [property: JsonPropertyName("messageId")] string MessageId,
    [property: JsonPropertyName("accountId")] string AccountId,
    [property: JsonPropertyName("sequenceNumber")] long SequenceNumber,
    [property: JsonPropertyName("occurredAt")] DateTimeOffset OccurredAt,
    [property: JsonPropertyName("receivedAt")] DateTimeOffset ReceivedAt,
    [property: JsonPropertyName("detectedAt")] DateTimeOffset DetectedAt,
    [property: JsonPropertyName("amountMinor")] long AmountMinor,
    [property: JsonPropertyName("currency")] string Currency,
    [property: JsonPropertyName("riskScore")] int RiskScore,
    [property: JsonPropertyName("reviewRecommended")] bool ReviewRecommended,
    [property: JsonPropertyName("reasons")] IReadOnlyList<RiskReason> Reasons,
    [property: JsonPropertyName("velocityStatus")] VelocityStatus VelocityStatus,
    [property: JsonPropertyName("velocity")] VelocityEvidence? Velocity,
    [property: JsonPropertyName("detector")] DetectorEvidence Detector,
    [property: JsonPropertyName("policyVersion")] string PolicyVersion,
    [property: JsonPropertyName("policyConfigurationHash")] string PolicyConfigurationHash
)
{
    [JsonPropertyName("schemaVersion")]
    public int SchemaVersion { get; init; } = 1;
}
