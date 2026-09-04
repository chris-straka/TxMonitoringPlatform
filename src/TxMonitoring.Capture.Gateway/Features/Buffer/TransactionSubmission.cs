namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// What the feed generator sends the gateway: the same six business fields as the
/// protobuf TransactionRecord. W3C trace context arrives as an HTTP header.
/// Risk is computed by detection; no producer-provided field is named RiskScore.
/// </summary>
public record TransactionSubmission(
    string MessageId,
    string AccountId,
    long SequenceNumber,
    DateTimeOffset OccurredAt, // event time
    long AmountMinor, // integer minor units
    string Currency
);
