namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// What the feed generator sends the gateway: the six business fields of the protobuf
/// TransactionRecord. Trace context arrives as an HTTP header. Detection computes risk, so
/// there is no producer-supplied score.
/// </summary>
public record TransactionSubmission(
    string MessageId,
    string AccountId,
    long SequenceNumber,
    DateTimeOffset OccurredAt, // event time
    long AmountMinor, // integer minor units
    string Currency
);
