using System.Text.Json.Serialization;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// Velocity evidence as published: aggregates can exceed JavaScript's safe integer even
/// when one AmountMinor cannot, so both values travel as invariant base-10 strings and
/// parse with checked long/BigInt conversions. Null when velocity was unavailable.
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
/// The closed, versioned risk-alerts payload. Changing a shape or meaning requires a
/// schema-version decision and updated golden fixtures. SequenceNumber and the individual
/// AmountMinor stay JSON numbers (their maxima are JavaScript-safe); human-readable
/// phrases come from a deterministic reason-code map in the UI, never free-form AI advice.
/// Trace context stays in Kafka headers, not in this value.
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
