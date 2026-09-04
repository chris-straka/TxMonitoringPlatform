using System.Diagnostics;
using System.Text;
using System.Text.Json;

using Confluent.Kafka;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using Npgsql;

using TxMonitoring.Detection.Worker.Configuration;
using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

public class TransactionConsumerWorker(
    IDbContextFactory<AppDbContext> contextFactory,
    ILogger<TransactionConsumerWorker> logger,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<ConsumerOptions> consumerOptions,
    IDeadLetterPublisher deadLetterPublisher,
    DetectionConsumerReadiness readiness,
    ModelEngine modelEngine,
    WorkerMetrics metrics
) : BackgroundService
{
    private int _consecutiveFailures;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafkaConfig = kafkaOptions.Value;

        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaConfig.BootstrapServers,
            GroupId = kafkaConfig.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            MetadataMaxAgeMs = 5000,
            AllowAutoCreateTopics = false,
            // A record becomes durable in Kafka's consumer-group state only after its Postgres
            // transaction succeeds (or after Kafka persists a poison copy in the DLQ).
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
                readiness.PartitionsAssigned(partitions.Count)
            )
            .SetPartitionsRevokedHandler((_, _) => readiness.PartitionsRevoked())
            .SetPartitionsLostHandler((_, _) => readiness.PartitionsRevoked())
            .Build();
        consumer.Subscribe(kafkaConfig.EventsTopic);

        logger.LogInformation(
            "Subscribed to Kafka topic {Topic} on {Servers}",
            kafkaConfig.EventsTopic,
            kafkaConfig.BootstrapServers
        );

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                Activity? activity = null;
                ConsumeResult<string, string>? consumeResult = null;

                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                    if (consumeResult is null)
                        continue;

                    metrics.Consumed.Add(1);

                    if (consumeResult.Message?.Value is null)
                    {
                        const string reason = "Kafka record is a tombstone";
                        await deadLetterPublisher.PublishAsync(
                            consumeResult,
                            reason,
                            stoppingToken
                        );
                        logger.LogWarning(
                            "Quarantined Kafka tombstone at {TopicPartitionOffset}.",
                            consumeResult.TopicPartitionOffset
                        );
                        consumer.Commit(consumeResult);
                        _consecutiveFailures = 0;
                        continue;
                    }

                    TransactionEvent? data;

                    try
                    {
                        // One shared serializer configuration; the positional constructor
                        // keeps a renamed member a compile-time break, not a silent default.
                        data = JsonSerializer.Deserialize<TransactionEvent>(
                            consumeResult.Message.Value,
                            TransactionJson.Options
                        );
                    }
                    catch (JsonException)
                    {
                        data = null;
                    }

                    if (!TryValidate(data, out var messageId, out var validationError))
                    {
                        await deadLetterPublisher.PublishAsync(
                            consumeResult,
                            validationError,
                            stoppingToken
                        );
                        logger.LogWarning(
                            "Quarantined poison message at {TopicPartitionOffset}: {Reason}",
                            consumeResult.TopicPartitionOffset,
                            validationError
                        );
                        consumer.Commit(consumeResult);
                        _consecutiveFailures = 0;
                        continue;
                    }

                    // JSON permits an equivalent instant with any numeric UTC offset, but Npgsql
                    // accepts DateTimeOffset for timestamptz only when its offset is zero.
                    data = NormalizeTimestamps(data!);

                    activity = WorkerTracing.Source.StartActivity(
                        "transaction.process",
                        ActivityKind.Consumer,
                        ReadTraceContext(consumeResult.Message.Headers),
                        tags:
                        [
                            new("messaging.system", "kafka"),
                            new("messaging.destination.name", consumeResult.Topic),
                            new("messaging.kafka.offset", consumeResult.Offset.Value),
                            new("messaging.kafka.partition", consumeResult.Partition.Value),
                            new("account.id", data.AccountId),
                        ]
                    );

                    await HandleTransactionAsync(data, messageId, stoppingToken);
                    consumer.Commit(consumeResult);
                    _consecutiveFailures = 0;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                    metrics.ProcessingFailures.Add(1);

                    if (consumeResult is not null)
                    {
                        try
                        {
                            // Consume advances the local fetch position before processing. Rewind
                            // it explicitly so a later success can never commit past this failure.
                            consumer.Seek(consumeResult.TopicPartitionOffset);
                        }
                        catch (Exception seekException)
                        {
                            throw new InvalidOperationException(
                                $"Could not rewind failed Kafka record {consumeResult.TopicPartitionOffset}; stopping prevents an offset skip.",
                                new AggregateException(ex, seekException)
                            );
                        }
                    }

                    logger.LogError(
                        ex,
                        consumeResult is null
                            ? "Kafka consume loop failed before returning a record."
                            : "Error processing Kafka record; it was rewound for retry."
                    );
                    await BackoffAsync(stoppingToken);
                }
                finally
                {
                    activity?.Dispose();
                }
            }
        }
        finally
        {
            readiness.PartitionsRevoked();
            consumer.Close();
        }
    }

    // Jittered, because every replica fails on the same Postgres at the same instant and a
    // fixed delay makes them retry in lockstep.
    private async Task BackoffAsync(CancellationToken cancellationToken)
    {
        _consecutiveFailures++;

        var consumerConfig = consumerOptions.Value;
        var baseBackoff = TimeSpan.FromSeconds(consumerConfig.BaseBackoffSeconds);
        var maxBackoff = TimeSpan.FromSeconds(consumerConfig.MaxBackoffSeconds);

        var exponential = baseBackoff * Math.Pow(2, Math.Min(_consecutiveFailures - 1, 10));
        var capped = exponential > maxBackoff ? maxBackoff : exponential;
        var jittered = TimeSpan.FromMilliseconds(
            Random.Shared.NextDouble() * capped.TotalMilliseconds
        );

        await SafeDelayAsync(jittered, cancellationToken);
    }

    private static async Task SafeDelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }

    private static ActivityContext ReadTraceContext(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes("traceparent", out var raw) || raw is null)
            return default;

        return ActivityContext.TryParse(
            Encoding.UTF8.GetString(raw),
            null,
            isRemote: true,
            out var ctx
        )
            ? ctx
            : default;
    }

    private async Task HandleTransactionAsync(
        TransactionEvent data,
        Guid messageId,
        CancellationToken stoppingToken
    )
    {
        await using var db = await contextFactory.CreateDbContextAsync(stoppingToken);

        if (await db.Transactions.AnyAsync(r => r.MessageId == messageId, stoppingToken))
        {
            logger.LogDebug("Duplicate MessageId {MessageId} skipped.", messageId);
            metrics.RecordDuplicate("select");
            return;
        }

        var modelAdvanced = false;
        MachineHealthResult result;

        try
        {
            // INTERIM detector input: the raw minor-unit amount as a float. The temperature
            // artifact's scale is meaningless on money; the policy port regenerates an
            // amount-feature artifact with a shared log transform and (AccountId, Currency)
            // isolation instead of renaming the old binary.
            result = await modelEngine.InspectAsync(
                data.AccountId,
                (float)data.AmountMinor,
                async cancellationToken =>
                {
                    var history = await db
                        .Transactions.Where(r => r.AccountId == data.AccountId)
                        // Reconstruct the order in which this stateful detector consumed rows.
                        // Origin clocks may jump, so event time is not a safe processing order.
                        .OrderByDescending(r => r.DetectedAt)
                        .ThenByDescending(r => r.Id)
                        .Take(ModelEngine.HistoryLength)
                        .Select(r => (float)r.AmountMinor)
                        .ToListAsync(cancellationToken);

                    history.Reverse();
                    return history;
                },
                stoppingToken
            );
            modelAdvanced = true;

            logger.LogDebug(
                "ML - ID: {Id}, Seq: {Seq}, Amount: {Amount}, IsAnomaly: {IsAnomaly}, Score: {Score:F4}, P-Value: {PVal:F4}, Lag: {Lag:F1}s",
                data.AccountId,
                data.SequenceNumber,
                data.AmountMinor,
                result.IsAnomaly,
                result.Score,
                result.PValue,
                (data.ReceivedAt - data.OccurredAt).TotalSeconds
            );

            var transaction = new MonitoredTransaction
            {
                MessageId = messageId,
                AccountId = data.AccountId,
                SequenceNumber = data.SequenceNumber,
                OccurredAt = data.OccurredAt,
                ReceivedAt = data.ReceivedAt,
                AmountMinor = data.AmountMinor,
                Currency = data.Currency,
                IsAnomaly = result.IsAnomaly,
                DetectorScore = result.Score,
                DetectorPValue = result.PValue,
                DetectorHistoryCount = result.HistoryCount,
                DetectorVersion = result.DetectorVersion,
            };

            db.Transactions.Add(transaction);

            if (result.IsAnomaly)
            {
                // The outbox row carries the complete observed event. The publisher forwards
                // it unchanged; the full RiskAlert decision envelope replaces this interim
                // payload with the policy port.
                db.AlertOutboxMessages.Add(
                    new AlertOutboxMessage
                    {
                        MessageId = messageId,
                        AccountId = data.AccountId,
                        Payload = JsonSerializer.Serialize(data, TransactionJson.Options),
                        TraceParent = Activity.Current?.Id,
                        CreatedAt = transaction.DetectedAt,
                    }
                );
            }

            // EF wraps both inserts in one Postgres transaction. The Kafka offset is committed
            // only after this returns, and the outbox publisher owns the separate Kafka write.
            await db.SaveChangesAsync(stoppingToken);
        }
        catch (DbUpdateException ex)
            when (ex.InnerException is PostgresException { SqlState: "23505" })
        {
            if (modelAdvanced)
                modelEngine.Invalidate(data.AccountId);

            // Several unique indexes exist. Treat the exception as idempotent success only if
            // another transaction actually persisted this transaction MessageId.
            await using var verificationDb = await contextFactory.CreateDbContextAsync(
                stoppingToken
            );
            if (
                await verificationDb.Transactions.AnyAsync(
                    r => r.MessageId == messageId,
                    stoppingToken
                )
            )
            {
                logger.LogDebug("Duplicate MessageId {MessageId} rejected by index.", messageId);
                metrics.RecordDuplicate("index");
                return;
            }

            throw;
        }
        catch
        {
            if (modelAdvanced)
                modelEngine.Invalidate(data.AccountId);
            throw;
        }

        metrics.Persisted.Add(1);

        metrics.Lag.Record((DateTimeOffset.UtcNow - data.OccurredAt).TotalSeconds);

        if (result.IsAnomaly)
        {
            metrics.Anomalies.Add(1);
        }
    }

    // Only deterministic validation failures reach the DLQ-before-commit path. Kafka,
    // producer, and internal failures must never be classified here: that would delete
    // the gateway's durable copy by committing past a retryable record.
    private static bool TryValidate(
        TransactionEvent? data,
        out Guid messageId,
        out string reason
    )
    {
        messageId = default;

        if (data is null)
        {
            reason = "payload is not valid transaction JSON";
            return false;
        }

        if (data.SchemaVersion != 1)
        {
            reason = "SchemaVersion is not 1";
            return false;
        }

        if (!Guid.TryParseExact(data.MessageId, "D", out messageId))
        {
            reason = "MessageId is not a canonical GUID";
            return false;
        }

        if (
            string.IsNullOrWhiteSpace(data.AccountId)
            || data.AccountId.Length > TransactionLimits.MaxAccountIdLength
            || data.AccountId != data.AccountId.Trim()
            || !data.AccountId.All(IsAccountIdCharacter)
        )
        {
            reason = "AccountId is missing or too long";
            return false;
        }

        if (data.SequenceNumber <= 0 || data.SequenceNumber > TransactionLimits.MaxSequenceNumber)
        {
            reason = "SequenceNumber is out of range";
            return false;
        }

        if (data.OccurredAt == default || data.ReceivedAt == default)
        {
            reason = "an event or ingestion timestamp is missing";
            return false;
        }

        if (data.AmountMinor <= 0 || data.AmountMinor > TransactionLimits.MaxAmountMinor)
        {
            reason = "AmountMinor is out of range";
            return false;
        }

        if (!TransactionCurrencies.IsSupported(data.Currency))
        {
            reason = "Currency is not supported";
            return false;
        }

        reason = string.Empty;
        return true;
    }

    internal static TransactionEvent NormalizeTimestamps(TransactionEvent data) =>
        data with
        {
            OccurredAt = data.OccurredAt.ToUniversalTime(),
            ReceivedAt = data.ReceivedAt.ToUniversalTime(),
        };

    private static bool IsAccountIdCharacter(char value) =>
        char.IsAsciiLetterOrDigit(value) || value is '-' or '_' or '.' or ':';
}
