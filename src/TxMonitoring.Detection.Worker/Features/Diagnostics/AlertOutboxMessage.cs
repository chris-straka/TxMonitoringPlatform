using Microsoft.EntityFrameworkCore;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// A risk alert waiting to be published to Kafka.
/// </summary>
/// <remarks>
/// This row commits in the same Postgres transaction as the monitored transaction. Publishing
/// is at-least-once: a crash after Kafka acknowledges but before PublishedAt commits sends a
/// duplicate, so consumers dedupe on MessageId.
/// </remarks>
[Index(nameof(MessageId), IsUnique = true)]
[Index(nameof(PublishedAt), nameof(CreatedAt))]
public sealed class AlertOutboxMessage
{
    public Guid Id { get; set; } = Guid.CreateVersion7();
    public Guid MessageId { get; set; }
    public string AccountId { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public string? TraceParent { get; set; }
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? PublishedAt { get; set; }
    public int AttemptCount { get; set; }
    public string? LastError { get; set; }
}
