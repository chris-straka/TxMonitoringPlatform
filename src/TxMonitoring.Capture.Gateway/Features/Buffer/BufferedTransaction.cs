namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// One transaction held on the gateway's local disk until ingestion settles it.
/// </summary>
/// <remarks>
/// OccurredAt comes from the origin and BufferedAt from this gateway. Only OccurredAt is sent
/// upstream. Without event time, transactions drained after an outage would all look
/// simultaneous.
/// </remarks>
public class BufferedTransaction
{
    // Maps to SQLite's INTEGER PRIMARY KEY rowid, so the oldest row has the smallest Id and the
    // uploader drains in Id order.
    public long Id { get; set; }

    // Idempotency key minted by the origin. The unique index catches a retry while the row is
    // queued. SettledMessage catches one that arrives after the row is removed.
    public string MessageId { get; set; } = string.Empty;

    // SHA-256 over the immutable origin content. A retry with the same MessageId but a different
    // fingerprint is a conflict and does not overwrite this row.
    public byte[] PayloadFingerprint { get; set; } = [];
    public int FingerprintVersion { get; set; }

    // Opaque account key, forwarded unchanged. Downstream it is the Kafka partition key.
    public string AccountId { get; set; } = string.Empty;

    // Assigned by the origin, monotonic per AccountId.
    public long SequenceNumber { get; set; }

    // Event time from the origin's clock, normalised to UTC on admission.
    public DateTimeOffset OccurredAt { get; set; }

    // Gateway clock when the row was written. BufferedAt - OccurredAt covers time in the
    // generator's channel, HTTP retries, the network, and the fsync.
    public DateTimeOffset BufferedAt { get; set; }

    // W3C traceparent of the delivering request. The generator's trace ends at the 202, so the
    // uploader links this from its batch span. Null when the request was not traced.
    public string? TraceParent { get; set; }

    // Attempted debit in integer minor units.
    public long AmountMinor { get; set; }

    // Uppercase three-letter code from the shared accepted set.
    public string Currency { get; set; } = string.Empty;
}
