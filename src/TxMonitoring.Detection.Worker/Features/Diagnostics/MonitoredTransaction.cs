using Microsoft.EntityFrameworkCore;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// A monitored transaction stored in Postgres: the immutable observed event plus the
/// locally reproducible decision evidence. This is not a tamper-evident or regulatory
/// audit trail. Amounts are bigint; risk-decision columns arrive with the policy port.
/// </summary>
[Index(nameof(MessageId), IsUnique = true)]
[Index(nameof(AccountId), nameof(OccurredAt))]
public class MonitoredTransaction
{
    // See the feed generator for why v7 is better for the index
    public Guid Id { get; set; } = Guid.CreateVersion7();

    // idempotency key (generator -> gateway -> ingestion -> kafka -> worker)
    public Guid MessageId { get; set; }
    public string AccountId { get; set; } = string.Empty;
    public long SequenceNumber { get; set; }
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset ReceivedAt { get; set; }
    public DateTimeOffset DetectedAt { get; set; } = DateTimeOffset.UtcNow;

    // Attempted debit amount in integer minor units.
    public long AmountMinor { get; set; }
    public string Currency { get; set; } = string.Empty;

    public bool IsAnomaly { get; set; }

    // Nullable only for rows created before the audit migration. Every new decision records all
    // four values so it can be explained without reconstructing ephemeral process state.
    public double? DetectorScore { get; set; }
    public double? DetectorPValue { get; set; }
    public int? DetectorHistoryCount { get; set; }
    public string? DetectorVersion { get; set; }
}
