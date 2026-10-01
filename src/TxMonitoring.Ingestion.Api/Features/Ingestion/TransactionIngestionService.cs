using System.Text;
using System.Text.Json;

using Confluent.Kafka;

using FluentValidation;

using Grpc.Core;

using Microsoft.Extensions.Options;

using TxMonitoring.Ingestion.Api.Configuration;
using TxMonitoring.Ingestion.Api.Infrastructure;
using TxMonitoring.Ingestion.V1;
using TxMonitoring.Shared;

namespace TxMonitoring.Ingestion.Api.Features.Ingestion;

/// <summary>
/// Accepts a batch of transactions from a capture gateway and hands each one to Kafka.
/// </summary>
public class TransactionIngestionService(
    IProducer<string, string> kafkaProducer,
    IValidator<TransactionRecord> validator,
    IOptions<KafkaOptions> kafkaOptions,
    IngestionMetrics metrics,
    ILogger<TransactionIngestionService> logger
) : TransactionIngestion.TransactionIngestionBase
{
    private readonly string _topic = kafkaOptions.Value.EventsTopic;

    public override async Task<IngestResponse> UploadTransactions(
        UploadTransactionsRequest request,
        ServerCallContext context
    )
    {
        if (request.Transactions.Count > TransactionLimits.MaxBatchSize)
        {
            throw new RpcException(
                new Status(
                    StatusCode.InvalidArgument,
                    $"A batch cannot contain more than {TransactionLimits.MaxBatchSize} transactions."
                )
            );
        }

        var accepted = new List<string>(request.Transactions.Count);
        var rejected = new List<string>();
        var inflight = new List<(string MessageId, Task<DeliveryResult<string, string>> Delivery)>(
            request.Transactions.Count
        );
        var undelivered = 0;
        Exception? firstFailure = null;

        foreach (var record in request.Transactions)
        {
            var validation = validator.Validate(record);

            if (!validation.IsValid)
            {
                logger.LogWarning(
                    "Rejecting transaction {MessageId} from {AccountId}: {Errors}.",
                    record.MessageId,
                    record.AccountId,
                    string.Join("; ", validation.Errors.Select(e => e.ErrorMessage))
                );

                rejected.Add(record.MessageId);
                metrics.Rejected.Add(1);
                continue;
            }

            try
            {
                var payload = new TransactionEvent(
                    record.MessageId,
                    record.AccountId,
                    record.SequenceNumber,
                    record.OccurredAt.ToDateTimeOffset(),
                    // Stamped per publish attempt. A retry after a lost acknowledgement may stamp
                    // a later value, but the first Postgres row for the MessageId wins.
                    DateTimeOffset.UtcNow,
                    record.AmountMinor,
                    record.Currency
                );

                var kafkaMsg = new Message<string, string>
                {
                    Key = record.AccountId,
                    Value = JsonSerializer.Serialize(payload, TransactionJson.Options),
                };

                // Carries the generator's trace context through to the consumer.
                if (!string.IsNullOrEmpty(record.Traceparent))
                {
                    kafkaMsg.Headers =
                    [
                        new Header("traceparent", Encoding.UTF8.GetBytes(record.Traceparent)),
                    ];
                }

                // Produce all first and await later so librdkafka can batch them into one request.
                inflight.Add(
                    (
                        record.MessageId,
                        kafkaProducer.ProduceAsync(_topic, kafkaMsg, context.CancellationToken)
                    )
                );
            }
            catch (OperationCanceledException)
                when (context.CancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Only validation failures are named as rejected. A producer failure leaves the
                // ID unnamed so the gateway keeps its durable copy and retries.
                logger.LogError(
                    ex,
                    "Could not queue transaction {MessageId} from {AccountId}; gateway will retry it.",
                    record.MessageId,
                    record.AccountId
                );
                undelivered++;
                firstFailure ??= ex;
            }
        }

        foreach (var (messageId, delivery) in inflight)
        {
            try
            {
                var result = await delivery;
                if (result.Status == PersistenceStatus.Persisted)
                {
                    accepted.Add(messageId);
                    metrics.Produced.Add(1);
                }
                else
                {
                    // PossiblyPersisted is ambiguous. Naming it accepted would let the gateway
                    // delete its copy without a broker acknowledgement.
                    undelivered++;
                    firstFailure ??= new KafkaException(
                        new Error(
                            ErrorCode.Local_MsgTimedOut,
                            $"Kafka reported {result.Status} for {messageId}."
                        )
                    );
                }
            }
            catch (OperationCanceledException)
                when (context.CancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                undelivered++;
                firstFailure ??= ex;
            }
        }

        metrics.Failed.Add(undelivered);

        // One summary line, since an outage fails every record and would flood the log.
        if (undelivered > 0)
        {
            logger.LogError(
                firstFailure,
                "{Undelivered} of {Sent} transactions were not durably delivered. Gateway will re-send them.",
                undelivered,
                request.Transactions.Count
            );
        }

        logger.LogInformation(
            "Batch: accepted {Accepted}, rejected {Rejected} of {Sent} (faulted: {Faulted}).",
            accepted.Count,
            rejected.Count,
            request.Transactions.Count,
            undelivered > 0
        );

        // Complete means every ID was classified as accepted or rejected. Unnamed IDs stay with
        // the gateway for retry.
        return new IngestResponse
        {
            Complete = undelivered == 0,
            AcceptedMessageIds = { accepted },
            RejectedMessageIds = { rejected },
        };
    }
}
