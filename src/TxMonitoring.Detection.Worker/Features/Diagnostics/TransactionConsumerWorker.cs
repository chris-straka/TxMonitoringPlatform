using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;

using Confluent.Kafka;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using Npgsql;

using TxMonitoring.Detection.Worker.Configuration;
using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

public class TransactionConsumerWorker(
    IDbContextFactory<AppDbContext> contextFactory,
    ILogger<TransactionConsumerWorker> logger,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<ConsumerOptions> consumerOptions,
    IOptions<RiskPolicyOptions> riskPolicyOptions,
    IDeadLetterPublisher deadLetterPublisher,
    DetectionConsumerReadiness readiness,
    AmountSpikeDetector spikeDetector,
    IVelocityStore velocityStore,
    ITransactionHistoryRepository historyRepository,
    TimeProvider timeProvider,
    WorkerMetrics metrics
) : BackgroundService
{
    private readonly string _policyConfigurationHash = RiskPolicyHash.Compute(
        riskPolicyOptions.Value
    );

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

                    var outcome = await HandleTransactionAsync(data, messageId, stoppingToken);
                    if (outcome == HandleOutcome.Conflict)
                    {
                        await deadLetterPublisher.PublishAsync(
                            consumeResult,
                            "MessageId reuses conflicting immutable content",
                            stoppingToken
                        );
                        logger.LogWarning(
                            "Quarantined conflicting MessageId {MessageId} at {TopicPartitionOffset}.",
                            messageId,
                            consumeResult.TopicPartitionOffset
                        );
                    }

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

    private enum HandleOutcome
    {
        Processed,
        Duplicate,
        Conflict,
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

    private async Task<HandleOutcome> HandleTransactionAsync(
        TransactionEvent data,
        Guid messageId,
        CancellationToken stoppingToken
    )
    {
        var fingerprint = TransactionFingerprint.ComputeV1(
            data.AccountId,
            data.SequenceNumber,
            data.OccurredAt,
            data.AmountMinor,
            data.Currency
        );

        await using var db = await contextFactory.CreateDbContextAsync(stoppingToken);

        var existing = await db.Transactions.FirstOrDefaultAsync(
            r => r.MessageId == messageId,
            stoppingToken
        );
        if (existing is not null)
        {
            // A Kafka replay is a no-op only when the immutable content matches. Differing
            // content under one MessageId is a deterministic conflict for the DLQ, never an
            // overwrite and never a silent duplicate.
            if (
                RedeliveryClassifier.Classify(existing.PayloadFingerprint, fingerprint)
                == RedeliveryOutcome.Duplicate
            )
            {
                logger.LogDebug("Duplicate MessageId {MessageId} skipped.", messageId);
                metrics.RecordDuplicate("select");
                return HandleOutcome.Duplicate;
            }

            logger.LogWarning(
                "Conflicting immutable content for MessageId {MessageId}; routing to DLQ.",
                messageId
            );
            return HandleOutcome.Conflict;
        }

        var detectorAdvanced = false;
        var reviewRecommended = false;
        AmountSpikeResult spike;
        VelocitySnapshot velocity;

        var detectorStarted = Stopwatch.GetTimestamp();
        try
        {
            spike = await spikeDetector.InspectAsync(
                data.AccountId,
                data.Currency,
                data.AmountMinor,
                async cancellationToken =>
                {
                    // The detector and velocity warm paths share this repository page, so a
                    // restart warms both from the same recent rows in detection order.
                    var recent = await historyRepository.LoadRecentAsync(
                        data.AccountId,
                        data.Currency,
                        AmountSpikeDetector.HistoryLength,
                        cancellationToken
                    );

                    return recent.Select(r => r.AmountMinor).Reverse().ToList();
                },
                stoppingToken
            );
            detectorAdvanced = true;
            var detectorWarm = spike.HistoryCount >= AmountSpikeDetector.HistoryLength;
            metrics.RecordDetectorLatency(
                Stopwatch.GetElapsedTime(detectorStarted).TotalMilliseconds,
                detectorWarm
            );
            if (detectorWarm)
            {
                metrics.DetectorWarms.Add(1);
            }

            // Velocity is derived cache state: any failure degrades to Unavailable instead of
            // failing the message, rewinding the offset, or touching the DLQ. Caller
            // cancellation still propagates.
            try
            {
                velocity = await velocityStore.ObserveAsync(
                    data.AccountId,
                    data.Currency,
                    messageId,
                    data.AmountMinor,
                    data.OccurredAt,
                    stoppingToken
                );
            }
            catch (Exception velocityException)
                when (velocityException is not OperationCanceledException)
            {
                logger.LogWarning(
                    velocityException,
                    "Velocity unavailable for {AccountId}; scoring from remaining signals.",
                    data.AccountId
                );
                velocity = VelocitySnapshot.Unavailable();
            }

            if (velocity.Status == VelocityStatus.Unavailable)
            {
                logger.LogDebug(
                    "Velocity {Status} for {AccountId}; persisting null evidence.",
                    velocity.Status,
                    data.AccountId
                );
            }

            var decision = RiskPolicyEvaluator.Evaluate(
                data,
                spike,
                velocity,
                riskPolicyOptions.Value,
                _policyConfigurationHash
            );
            reviewRecommended = decision.ReviewRecommended;
            metrics.Evaluated.Add(1);
            foreach (var reason in decision.Reasons)
            {
                metrics.RecordRuleFired(reason.Code);
            }
            if (reviewRecommended)
            {
                metrics.ReviewRecommended.Add(1);
            }

            logger.LogDebug(
                "Decision - ID: {Id}, Seq: {Seq}, Amount: {Amount}, Score: {Score}, Review: {Review}, Lag: {Lag:F1}s",
                data.AccountId,
                data.SequenceNumber,
                data.AmountMinor,
                decision.RiskScore,
                decision.ReviewRecommended,
                (data.ReceivedAt - data.OccurredAt).TotalSeconds
            );

            // One clock call: the stored transaction, the outbox CreatedAt, and the alert
            // payload all share this exact instant.
            var detectedAt = timeProvider.GetUtcNow();

            var transaction = new MonitoredTransaction
            {
                MessageId = messageId,
                AccountId = data.AccountId,
                SequenceNumber = data.SequenceNumber,
                OccurredAt = data.OccurredAt,
                ReceivedAt = data.ReceivedAt,
                DetectedAt = detectedAt,
                AmountMinor = data.AmountMinor,
                Currency = data.Currency,
                PayloadFingerprint = fingerprint,
                FingerprintVersion = TransactionFingerprint.Version,
                RiskScore = decision.RiskScore,
                ReviewRecommended = decision.ReviewRecommended,
                ReasonsJson = JsonSerializer.Serialize(decision.Reasons, TransactionJson.Options),
                VelocityStatus = velocity.Status.ToString(),
                Count1m = velocity.Count1m,
                Amount5mMinor = velocity.Amount5mMinor,
                DetectorIsSpike = spike.IsSpike,
                DetectorRawScore = spike.RawScore,
                DetectorPValue = spike.PValue,
                DetectorHistoryCount = spike.HistoryCount,
                DetectorVersion = spike.DetectorVersion,
                PolicyVersion = decision.PolicyVersion,
                PolicyConfigurationHash = decision.PolicyConfigurationHash,
            };

            db.Transactions.Add(transaction);

            if (decision.ReviewRecommended)
            {
                db.AlertOutboxMessages.Add(
                    new AlertOutboxMessage
                    {
                        MessageId = messageId,
                        AccountId = data.AccountId,
                        Payload = JsonSerializer.Serialize(
                            new RiskAlert(
                                data.MessageId,
                                data.AccountId,
                                data.SequenceNumber,
                                data.OccurredAt,
                                data.ReceivedAt,
                                detectedAt,
                                data.AmountMinor,
                                data.Currency,
                                decision.RiskScore,
                                decision.ReviewRecommended,
                                decision.Reasons,
                                velocity.Status,
                                velocity.Status == VelocityStatus.Available
                                    ? new VelocityEvidence(
                                        velocity.Count1m!.Value.ToString(
                                            CultureInfo.InvariantCulture
                                        ),
                                        velocity.Amount5mMinor!.Value.ToString(
                                            CultureInfo.InvariantCulture
                                        )
                                    )
                                    : null,
                                new DetectorEvidence(
                                    spike.IsSpike,
                                    spike.RawScore,
                                    spike.PValue,
                                    spike.HistoryCount,
                                    spike.DetectorVersion
                                ),
                                decision.PolicyVersion,
                                decision.PolicyConfigurationHash
                            ),
                            TransactionJson.Options
                        ),
                        TraceParent = Activity.Current?.Id,
                        CreatedAt = detectedAt,
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
            if (detectorAdvanced)
                spikeDetector.Invalidate(data.AccountId, data.Currency);

            // A unique race means another transaction won. It is idempotent success only when
            // the winner carries the same immutable content; otherwise it is a conflict.
            await using var verificationDb = await contextFactory.CreateDbContextAsync(
                stoppingToken
            );
            var winner = await verificationDb.Transactions.FirstOrDefaultAsync(
                r => r.MessageId == messageId,
                stoppingToken
            );
            if (winner is not null)
            {
                if (
                    RedeliveryClassifier.Classify(winner.PayloadFingerprint, fingerprint)
                    == RedeliveryOutcome.Duplicate
                )
                {
                    logger.LogDebug("Duplicate MessageId {MessageId} rejected by index.", messageId);
                    metrics.RecordDuplicate("index");
                    return HandleOutcome.Duplicate;
                }

                logger.LogWarning(
                    "Conflicting immutable content for MessageId {MessageId}; routing to DLQ.",
                    messageId
                );
                return HandleOutcome.Conflict;
            }

            throw;
        }
        catch
        {
            if (detectorAdvanced)
                spikeDetector.Invalidate(data.AccountId, data.Currency);
            throw;
        }

        metrics.Persisted.Add(1);

        metrics.Lag.Record((DateTimeOffset.UtcNow - data.OccurredAt).TotalSeconds);

        if (reviewRecommended)
        {
            metrics.ReviewsRecommended.Add(1);
        }

        return HandleOutcome.Processed;
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
