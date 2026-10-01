using System.Diagnostics;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using TxMonitoring.Capture.Gateway.Configuration;
using TxMonitoring.Capture.Gateway.Features.Buffer;
using TxMonitoring.Capture.Gateway.Infrastructure;
using TxMonitoring.Ingestion.V1; // generated from Protos/transactions.proto

namespace TxMonitoring.Capture.Gateway.Features.Upload;

/// <summary>
/// Drains the local SQLite buffer to ingestion over gRPC, oldest row first.
/// </summary>
/// <remarks>
/// Runs independently of the receive endpoint. Delivery is at-least-once: an ambiguous failure
/// re-sends, and Postgres dedupes on MessageId. Only rows ingestion names in its response are
/// deleted. gRPC's built-in retry is not used because it would re-send the whole batch without
/// knowing which rows persisted.
/// </remarks>
public class UploaderWorker(
    IServiceScopeFactory scopeFactory,
    EdgeMetrics metrics,
    BufferDepth bufferDepth,
    IOptions<UploaderOptions> uploaderOptions,
    ILogger<UploaderWorker> logger
) : BackgroundService
{
    #region private_vars

    // Larger batches mean fewer round trips but more rows re-sent after a failure.
    private readonly int _batchSize = uploaderOptions.Value.BatchSize;

    // The buffer is usually empty, so this is roughly how long a new row waits to be sent.
    private readonly TimeSpan _idleDelay = TimeSpan.FromMilliseconds(
        uploaderOptions.Value.IdleDelayMs
    );

    // Doubled for each consecutive failure.
    private readonly TimeSpan _baseBackoff = TimeSpan.FromSeconds(
        uploaderOptions.Value.BaseBackoffSeconds
    );

    private readonly TimeSpan _maxBackoff = TimeSpan.FromSeconds(
        uploaderOptions.Value.MaxBackoffSeconds
    );

    // Caps the wait on a hung connection.
    private readonly TimeSpan _uploadTimeout = TimeSpan.FromSeconds(
        uploaderOptions.Value.UploadTimeoutSeconds
    );

    // Reset after any batch is settled.
    private int _consecutiveFailures;

    // Only the first unreachable attempt in an outage logs at Warning.
    private int _consecutiveUnreachable;

    #endregion

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Edge uploader started. Batch size {BatchSize}, idle poll {IdleDelay}.",
            _batchSize,
            _idleDelay
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // BackgroundService is a singleton, so scoped services need their own scope.
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<EdgeDbContext>();
                var settlementStore =
                    scope.ServiceProvider.GetRequiredService<BufferSettlementStore>();
                var grpcClient =
                    scope.ServiceProvider.GetRequiredService<TransactionIngestion.TransactionIngestionClient>();

                await RefreshGaugesAsync(db, stoppingToken);

                // SQLite assigns IDs in insert order, so the lowest ID is the oldest row.
                var pending = await db
                    .BufferedTransactions.OrderBy(r => r.Id)
                    .Take(_batchSize)
                    .ToListAsync(stoppingToken);

                if (pending.Count == 0)
                {
                    await SafeDelayAsync(_idleDelay, stoppingToken);
                    continue;
                }

                var batch = pending
                    .Select(item => new OutgoingTransaction(item, ToRecord(item)))
                    .ToList();

                var result = await UploadBatchAsync(grpcClient, batch, stoppingToken);

                metrics.SetIngestionReachable(result.Outcome != UploadOutcome.Unreachable);

                switch (result.Outcome)
                {
                    // Each of these has already logged, and none leaves a row safe to delete.
                    case UploadOutcome.Malformed:
                    case UploadOutcome.Unreachable:
                    case UploadOutcome.Refused:
                        await BackoffAsync(stoppingToken);
                        continue;

                    // Nothing settled, so there is nothing to delete and no reason to retry
                    // immediately.
                    case UploadOutcome.Answered when result.Settled.Count == 0:
                        await BackoffAsync(stoppingToken);
                        continue;
                }

                var settledRows = batch
                    .Where(p => result.Settled.Contains(p.Row.MessageId))
                    .Select(p => p.Row)
                    .ToList();

                await settlementStore.SettleAsync(
                    settledRows,
                    result.RejectedIds,
                    stoppingToken
                );

                metrics.Uploaded.Add(result.AcceptedIds.Count);
                metrics.Rejected.Add(result.RejectedIds.Count);

                _consecutiveFailures = 0;
                _consecutiveUnreachable = 0;

                if (result.RejectedIds.Count > 0)
                {
                    logger.LogError(
                        "Ingestion permanently rejected {Count} transactions. Quarantined {MessageIds} "
                            + "with a generic reason because the response has no per-reading cause.",
                        result.RejectedIds.Count,
                        string.Join(", ", result.RejectedIds)
                    );
                }

                if (settledRows.Count < batch.Count)
                {
                    logger.LogWarning(
                        "Ingestion settled {Settled}/{Sent} (faulted: {Faulted}). Retrying the remainder.",
                        settledRows.Count,
                        batch.Count,
                        !result.Complete
                    );
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A bug or local failure such as a full disk. UploadBatchAsync handles outages.
                logger.LogError(ex, "Unexpected failure in the uploader loop.");
                await BackoffAsync(stoppingToken);
            }
        }

        logger.LogInformation("Edge uploader stopped.");
    }

    private async Task<UploadResult> UploadBatchAsync(
        TransactionIngestion.TransactionIngestionClient grpcClient,
        List<OutgoingTransaction> batch,
        CancellationToken cancellationToken
    )
    {
        var startedAt = Stopwatch.GetTimestamp();
        var outcome = "local_failure";

        // The 202 ended each generator trace, so the batch span links back to them.
        var links = BuildTraceLinks(batch);

        // Null when nothing listens to the source.
        using var activity = EdgeTracing.Source.StartActivity(
            "capture.upload",
            ActivityKind.Client,
            parentContext: default,
            tags:
            [
                new("capture.batch.size", batch.Count),
                new("capture.batch.linked_traces", links.Count),
                new("capture.upload.attempt", _consecutiveFailures + 1),
            ],
            links: links
        );

        try
        {
            var request = new UploadTransactionsRequest
            {
                Transactions = { batch.Select(p => p.Record) },
            };

            activity?.SetTag("capture.batch.bytes", request.CalculateSize());

            using var call = grpcClient.UploadTransactionsAsync(
                request,
                deadline: DateTime.UtcNow.Add(_uploadTimeout),
                cancellationToken: cancellationToken
            );

            var response = await call.ResponseAsync;
            outcome = "answered";

            activity?.SetTag("capture.batch.accepted", response.AcceptedMessageIds.Count);
            activity?.SetTag("capture.batch.rejected", response.RejectedMessageIds.Count);

            var sentIds = batch
                .Select(item => item.Row.MessageId)
                .ToHashSet(StringComparer.Ordinal);
            var acceptedIds = response.AcceptedMessageIds.ToHashSet(StringComparer.Ordinal);
            var rejectedIds = response.RejectedMessageIds.ToHashSet(StringComparer.Ordinal);

            if (
                !acceptedIds.IsSubsetOf(sentIds)
                || !rejectedIds.IsSubsetOf(sentIds)
                || acceptedIds.Overlaps(rejectedIds)
            )
            {
                outcome = "malformed_response";
                activity?.SetStatus(
                    ActivityStatusCode.Error,
                    "Cloud response contained an unknown or contradictory MessageId."
                );
                metrics.RecordUploadFailure("malformed_response");
                logger.LogError(
                    "Cloud returned an invalid acknowledgement set. No local rows will be deleted."
                );
                return UploadResult.Failed(UploadOutcome.Malformed);
            }

            return new UploadResult(
                UploadOutcome.Answered,
                [.. acceptedIds],
                [.. rejectedIds],
                response.Complete
            );
        }
        catch (RpcException ex) when (IsShutdownCancellation(ex.StatusCode, cancellationToken))
        {
            outcome = "cancelled";
            // gRPC reports cancellation as RpcException. The loop expects
            // OperationCanceledException.
            throw new OperationCanceledException(cancellationToken);
        }
        catch (RpcException ex) when (IsBatchContentError(ex.StatusCode))
        {
            outcome = "malformed";
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            metrics.RecordUploadFailure("malformed");
            logger.LogError(
                ex,
                "Cloud refused the whole batch with {Status}. Buffering until this is fixed.",
                ex.StatusCode
            );
            return UploadResult.Failed(UploadOutcome.Malformed);
        }
        catch (RpcException ex) when (IsCallerRefused(ex.StatusCode))
        {
            outcome = "refused";
            // Only a configuration change can fix a refused caller.
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            metrics.RecordUploadFailure("refused");
            logger.LogError(
                ex,
                "Cloud rejected this gateway with {Status}. Buffering until this is fixed.",
                ex.StatusCode
            );
            return UploadResult.Failed(UploadOutcome.Refused);
        }
        catch (RpcException ex)
        {
            outcome = "unreachable";
            // Dropped connections and expired deadlines.
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            metrics.RecordUploadFailure("unreachable");

            _consecutiveUnreachable++;

            if (_consecutiveUnreachable == 1)
                logger.LogWarning(ex, "Cloud unreachable. Buffering locally.");
            else
                logger.LogDebug(
                    "Cloud still unreachable ({Attempts} attempts).",
                    _consecutiveUnreachable
                );

            return UploadResult.Failed(UploadOutcome.Unreachable);
        }
        finally
        {
            metrics.RecordUploadDuration(
                Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds,
                outcome
            );
        }
    }

    private static TransactionRecord ToRecord(BufferedTransaction item) =>
        new()
        {
            MessageId = item.MessageId,
            AccountId = item.AccountId,
            SequenceNumber = item.SequenceNumber,
            OccurredAt = Timestamp.FromDateTimeOffset(item.OccurredAt),
            AmountMinor = item.AmountMinor,
            Currency = item.Currency,
            Traceparent = item.TraceParent ?? "",
        };

    /// <summary>
    /// Builds span links to each buffered transaction's originating trace.
    /// </summary>
    /// <remarks>
    /// TryParse skips a bad traceparent. Parse would throw and fail the whole batch.
    /// </remarks>
    private static List<ActivityLink> BuildTraceLinks(List<OutgoingTransaction> batch) =>
        batch
            .Select(p => p.Row.TraceParent)
            .Where(tp => !string.IsNullOrEmpty(tp))
            .Select(tp =>
                ActivityContext.TryParse(tp, null, isRemote: true, out var ctx) ? ctx : default
            )
            .Where(ctx => ctx != default)
            .Select(ctx => new ActivityLink(ctx))
            .ToList();

    /// <summary>
    /// Updates gauges derived from the oldest queued row.
    /// </summary>
    /// <remarks>
    /// BufferDepth is initialized from COUNT(*) once at startup and then maintained by atomic
    /// reservations. Recounting here would race those reservations and overwrite a newer value.
    /// </remarks>
    private async Task RefreshGaugesAsync(EdgeDbContext db, CancellationToken cancellationToken)
    {
        if (bufferDepth.Current == 0)
        {
            metrics.NoTransactionsBuffered();
            return;
        }

        // Reads one row off the primary key, so it is cheap enough to run every pass.
        var oldest = await db
            .BufferedTransactions.OrderBy(r => r.Id)
            .Select(r => (DateTimeOffset?)r.OccurredAt) // empty yields null, not 0001-01-01
            .FirstOrDefaultAsync(cancellationToken);

        // The depth counter can briefly lead the table, e.g. while a receive is in flight.
        if (oldest is null)
        {
            metrics.NoTransactionsBuffered();
            return;
        }

        metrics.SetOldestTransactionAge((DateTimeOffset.UtcNow - oldest.Value).TotalSeconds);
    }

    /// <summary>
    /// Exponential backoff with full jitter.
    /// </summary>
    /// <remarks>
    /// Jitter keeps gateways from retrying in lockstep during an outage. This is hand-rolled
    /// rather than Polly because it spaces loop iterations, not a single call.
    /// </remarks>
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

    // Our own shutdown cancelled the call, as opposed to the server hanging up.
    private static bool IsShutdownCancellation(StatusCode status, CancellationToken token) =>
        status == StatusCode.Cancelled && token.IsCancellationRequested;

    // Ingestion is reachable but refused the call itself. Per-transaction rejections arrive in a
    // successful response instead.
    private static bool IsBatchContentError(StatusCode status) =>
        status is StatusCode.InvalidArgument or StatusCode.OutOfRange;

    // Ingestion is refusing this caller, so changing the batch cannot help.
    private static bool IsCallerRefused(StatusCode status) =>
        status
            is StatusCode.Unauthenticated
                or StatusCode.PermissionDenied
                or StatusCode.Unimplemented;

    // Each outcome needs a different decision in the loop, so failures are classified here
    // instead of rethrown.
    private enum UploadOutcome
    {
        Answered,
        Unreachable,
        Malformed,
        Refused,
    }

    /// <param name="Outcome">How the call ended.</param>
    /// <param name="AcceptedIds">Transactions ingestion has durably accepted.</param>
    /// <param name="RejectedIds">Transactions ingestion permanently rejected.</param>
    /// <param name="Complete">False when ingestion stopped partway through the batch.</param>
    private readonly record struct UploadResult(
        UploadOutcome Outcome,
        IReadOnlyList<string> AcceptedIds,
        IReadOnlyList<string> RejectedIds,
        bool Complete
    )
    {
        public HashSet<string> Settled { get; } = [.. AcceptedIds.Concat(RejectedIds)];

        public static UploadResult Failed(UploadOutcome outcome) =>
            new(outcome, [], [], Complete: false);
    }

    private readonly record struct OutgoingTransaction(
        BufferedTransaction Row,
        TransactionRecord Record
    );

    // Task.Delay throws on cancellation. One caller is the loop's catch block, where a throw
    // would escape ExecuteAsync instead of reaching the shutdown handler.
    private static async Task SafeDelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }
}
