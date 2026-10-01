namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// A transaction ingestion rejected permanently, kept briefly for operator inspection.
/// </summary>
/// <remarks>
/// This is not a retry queue. Keeping the payload makes admission drift diagnosable without
/// letting one poison transaction block newer ones. Rows are bounded by age and count.
/// </remarks>
public sealed class QuarantinedTransaction
{
    public string MessageId { get; set; } = string.Empty;
    public byte[] PayloadFingerprint { get; set; } = [];
    public int FingerprintVersion { get; set; }
    public long OriginalQueueId { get; set; }
    public string AccountId { get; set; } = string.Empty;
    public long SequenceNumber { get; set; }
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset BufferedAt { get; set; }
    public string? TraceParent { get; set; }
    public long AmountMinor { get; set; }
    public string Currency { get; set; } = string.Empty;

    public DateTimeOffset RejectedAt { get; set; }
    public string RejectionCode { get; set; } = string.Empty;
    public string RejectionReason { get; set; } = string.Empty;
}
