using System.Text;

using Confluent.Kafka;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using TxMonitoring.Detection.Worker.Configuration;
using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// Publishes committed review alerts from the Postgres outbox to Kafka.
/// </summary>
/// <remarks>
/// The consumer committed the complete payload in the same Postgres transaction as the
/// decision, so this worker forwards it unchanged with no enrichment.
/// </remarks>
public sealed class AlertOutboxPublisherWorker(
    IDbContextFactory<AppDbContext> contextFactory,
    IProducer<string, string> producer,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<OutboxOptions> outboxOptions,
    WorkerMetrics metrics,
    ILogger<AlertOutboxPublisherWorker> logger
) : BackgroundService
{
    private readonly TimeSpan _idleDelay = TimeSpan.FromMilliseconds(
        outboxOptions.Value.IdleDelayMs
    );
    private readonly TimeSpan _baseBackoff = TimeSpan.FromSeconds(
        outboxOptions.Value.BaseBackoffSeconds
    );
    private readonly TimeSpan _maxBackoff = TimeSpan.FromSeconds(
        outboxOptions.Value.MaxBackoffSeconds
    );
    private readonly TimeSpan _publishedRetention = TimeSpan.FromHours(
        outboxOptions.Value.PublishedRetentionHours
    );
    private readonly TimeSpan _maintenanceInterval = TimeSpan.FromSeconds(
        outboxOptions.Value.MaintenanceIntervalSeconds
    );
    private DateTimeOffset _nextMaintenanceAt = DateTimeOffset.MinValue;
    private int _consecutiveFailures;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await MaintainOutboxAsync(stoppingToken);

                switch (await PublishOneAsync(stoppingToken))
                {
                    case PublishOutcome.Empty:
                        _consecutiveFailures = 0;
                        await SafeDelayAsync(_idleDelay, stoppingToken);
                        break;
                    case PublishOutcome.Published:
                        _consecutiveFailures = 0;
                        break;
                    case PublishOutcome.Failed:
                        await BackoffAsync(stoppingToken);
                        break;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Alert outbox publisher failed before completing a row.");
                await BackoffAsync(stoppingToken);
            }
        }
    }

    private async Task MaintainOutboxAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        if (now < _nextMaintenanceAt)
            return;

        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        var cutoff = now - _publishedRetention;

        // Only published rows expire. Keeping a bounded window of them preserves recent history
        // without letting this hot table grow without limit.
        var deleted = await db
            .AlertOutboxMessages.Where(row =>
                row.PublishedAt != null && row.PublishedAt < cutoff
            )
            .ExecuteDeleteAsync(cancellationToken);
        var pending = await db.AlertOutboxMessages.LongCountAsync(
            row => row.PublishedAt == null,
            cancellationToken
        );

        metrics.SetPendingAlertOutbox(pending);
        _nextMaintenanceAt = now + _maintenanceInterval;

        if (deleted > 0)
            logger.LogInformation("Pruned {Count} published alert outbox rows.", deleted);
    }

    private async Task<PublishOutcome> PublishOneAsync(CancellationToken cancellationToken)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

        // SKIP LOCKED lets replicas drain the shared outbox without publishing the same row at
        // once. The lock is held through Kafka's ACK, and a crash rolls it back for retry.
        var candidates = await db
            .AlertOutboxMessages.FromSqlRaw(
                """
                SELECT * FROM "AlertOutboxMessages"
                WHERE "PublishedAt" IS NULL
                ORDER BY "CreatedAt"
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            .AsTracking()
            .ToListAsync(cancellationToken);

        var pending = candidates.SingleOrDefault();
        if (pending is null)
        {
            await transaction.CommitAsync(cancellationToken);
            return PublishOutcome.Empty;
        }

        var alert = new Message<string, string>
        {
            Key = pending.AccountId,
            Value = pending.Payload,
        };

        if (!string.IsNullOrEmpty(pending.TraceParent))
        {
            alert.Headers =
            [
                new Header("traceparent", Encoding.UTF8.GetBytes(pending.TraceParent)),
            ];
        }

        try
        {
            var delivery = await producer.ProduceAsync(
                kafkaOptions.Value.AlertsTopic,
                alert,
                cancellationToken
            );

            if (delivery.Status != PersistenceStatus.Persisted)
            {
                throw new KafkaException(
                    new Error(
                        ErrorCode.Local_MsgTimedOut,
                        $"Kafka reported {delivery.Status} for outbox message {pending.Id}."
                    )
                );
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            pending.AttemptCount++;
            pending.LastError = Truncate(ex.Message, 2_000);
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            metrics.AlertFailures.Add(1);
            logger.LogError(
                ex,
                "Alert outbox message {OutboxId} for {AccountId} was not published; it remains pending.",
                pending.Id,
                pending.AccountId
            );
            return PublishOutcome.Failed;
        }

        pending.PublishedAt = DateTimeOffset.UtcNow;
        pending.AttemptCount++;
        pending.LastError = null;
        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        metrics.AlertsPublished.Add(1);
        return PublishOutcome.Published;
    }

    private async Task BackoffAsync(CancellationToken cancellationToken)
    {
        _consecutiveFailures++;
        var exponential = _baseBackoff * Math.Pow(2, Math.Min(_consecutiveFailures - 1, 10));
        var capped = exponential > _maxBackoff ? _maxBackoff : exponential;
        var jittered = TimeSpan.FromMilliseconds(
            Random.Shared.NextDouble() * capped.TotalMilliseconds
        );
        await SafeDelayAsync(jittered, cancellationToken);
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];

    private static async Task SafeDelayAsync(
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }

    private enum PublishOutcome
    {
        Empty,
        Published,
        Failed,
    }
}
