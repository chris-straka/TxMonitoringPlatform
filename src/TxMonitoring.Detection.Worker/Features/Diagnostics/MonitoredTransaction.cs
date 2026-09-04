using Microsoft.EntityFrameworkCore;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// A monitored transaction stored in Postgres: the immutable observed event plus the
/// locally reproducible decision evidence. This is not a tamper-evident or regulatory
/// audit trail. Amounts are bigint; the risk score is a transparent ranking, not a
/// calibrated fraud probability.
/// </summary>
[Index(nameof(MessageId), IsUnique = true)]
[Index(nameof(AccountId), nameof(Currency), nameof(DetectedAt), nameof(Id))]
[Index(nameof(AccountId), nameof(Currency), nameof(OccurredAt))]
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

    // SHA-256 over the immutable origin content (see TransactionFingerprint). A replay
    // with the same ID but different content is a deterministic conflict, not a duplicate.
    public byte[] PayloadFingerprint { get; set; } = [];
    public int FingerprintVersion { get; set; }

    public int RiskScore { get; set; }
    public bool ReviewRecommended { get; set; }

    /// <summary>
    /// Every fired reason in fixed rule order, serialized with the shared JSON shape.
    /// Persisted even below the alert threshold.
    /// </summary>
    public string ReasonsJson { get; set; } = "[]";

    public string VelocityStatus { get; set; } = string.Empty;
    public long? Count1m { get; set; }
    public long? Amount5mMinor { get; set; }

    public bool DetectorIsSpike { get; set; }
    public double DetectorRawScore { get; set; }
    public double DetectorPValue { get; set; }
    public int DetectorHistoryCount { get; set; }
    public string DetectorVersion { get; set; } = string.Empty;

    public string PolicyVersion { get; set; } = string.Empty;
    public string PolicyConfigurationHash { get; set; } = string.Empty;
}
