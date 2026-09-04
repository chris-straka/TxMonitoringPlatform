namespace TxMonitoring.Feed.Emulator;

/// <summary>
/// What this generator POSTs to the gateway: the same six business fields as the
/// protobuf TransactionRecord. The gateway's TransactionSubmission is canonical; this
/// mirror exists because the emulator must not reference the gateway project.
/// </summary>
public record TransactionSubmission(
    string MessageId,
    string AccountId,
    long SequenceNumber,
    DateTimeOffset OccurredAt, // event time
    long AmountMinor, // integer minor units, never floats
    string Currency
);
