namespace TxMonitoring.Feed.Emulator;

/// <summary>
/// What this generator posts to the gateway: the six business fields of the protobuf
/// TransactionRecord. The gateway's TransactionSubmission is canonical. This copy exists so
/// the emulator does not reference the gateway project.
/// </summary>
public record TransactionSubmission(
    string MessageId,
    string AccountId,
    long SequenceNumber,
    DateTimeOffset OccurredAt, // event time
    long AmountMinor, // integer minor units
    string Currency
);
