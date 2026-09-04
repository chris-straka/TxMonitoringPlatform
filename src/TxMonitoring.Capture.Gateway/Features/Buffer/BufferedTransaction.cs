namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// One transaction parked on the gateway's local disk until ingestion confirms it.
/// </summary>
/// <remarks>
/// Two clocks stamp every transaction: OccurredAt from the origin, BufferedAt from this gateway.
/// Only OccurredAt travels on, and ingestion stamps its own ReceivedAt on arrival.
/// Without event time, transactions drained after an outage would all look simultaneous.
/// Amounts are integer minor units; no floats in the money path.
/// </remarks>
public class BufferedTransaction
{
    // EF will map `Id` to SQLite's INTEGER PRIMARY KEY rowid (autoincrements)
    // Can't use GUID for rowid because it's not an int and can't autoincrement
    // The oldest buffered transactions will hold the smallest Id (what we use to drain)
    public long Id { get; set; }

    // Origin mints this idempotency key. The live-table unique index catches a retry while queued;
    // SettledMessage catches one that arrives after this row has been removed.
    public string MessageId { get; set; } = string.Empty;

    // SHA-256 over the immutable origin content. MessageId selects the record; the
    // fingerprint proves a retry carries the same content. Same ID plus different
    // content is a conflict and never overwrites the first event.
    public byte[] PayloadFingerprint { get; set; } = [];
    public int FingerprintVersion { get; set; }

    // Opaque synthetic account key, forwarded untouched; the Kafka partition key downstream.
    public string AccountId { get; set; } = string.Empty;

    // Origin assigned, monotonic per AccountId, forwarded untouched to ingestion
    public long SequenceNumber { get; set; }

    // EVENT TIME -> origin's clock at generation, normalised to UTC before custody
    public DateTimeOffset OccurredAt { get; set; }

    // BUFFER TIME, gateway's clock when the transaction hit disk
    // BufferedAt - OccuredAt = generation -> durability
    // time in emulator channel, Polly retries, network, gateway fsync
    public DateTimeOffset BufferedAt { get; set; }

    // The W3C traceparent of the delivering request
    // The generator's own trace ends at the 202, storing its id outlives the request
    // The uploader links this as part of its batch span when draining
    // Nullable because a transaction can arrive unsampled
    public string? TraceParent { get; set; }

    // Attempted debit amount in integer minor units.
    public long AmountMinor { get; set; }

    // Uppercase three-letter code; validated against the shared accepted set.
    public string Currency { get; set; } = string.Empty;
}
