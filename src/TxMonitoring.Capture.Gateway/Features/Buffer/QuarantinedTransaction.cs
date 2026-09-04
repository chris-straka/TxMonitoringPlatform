namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// A transaction ingestion declared permanently invalid, retained briefly for operator inspection.
/// </summary>
/// <remarks>
/// Quarantine is not another delivery queue: ingestion has explicitly said it will never accept
/// this MessageId. Keeping the original payload makes admission drift diagnosable without letting
/// one poison transaction block newer ones. Rows are bounded by both age and count.
/// </remarks>
public sealed class QuarantinedTransaction
{
    public string MessageId { get; set; } = string.Empty;
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
