using System.Diagnostics;

using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using TxMonitoring.Capture.Gateway.Configuration;
using TxMonitoring.Capture.Gateway.Infrastructure;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Receives one transaction from the feed generator and durably buffers it.
/// A 202 means the transaction is on local disk under WAL with synchronous=FULL.
/// </summary>
public static class ReceiveTelemetryEndpoint
{
    private const int MaxTraceParentLength = 128;

    public static void MapReceiverEndpoints(this IEndpointRouteBuilder app)
    {
        // A static class cannot be the T in ILogger<T>, so create the logger from the factory.
        var logger = app
            .ServiceProvider.GetRequiredService<ILoggerFactory>()
            .CreateLogger(typeof(ReceiveTelemetryEndpoint).FullName!);

        app.MapPost(
            "/api/local/transactions",
            async (
                TransactionSubmission req,
                EdgeDbContext db,
                EdgeMetrics metrics,
                BufferDepth bufferDepth,
                BufferMutationGate mutationGate,
                IOptions<BufferOptions> bufferOptions,
                HttpContext http
            ) =>
            {
                var validationError = TransactionAdmissionValidator.Validate(
                    req,
                    out var canonicalMessageId
                );
                if (validationError is not null)
                {
                    metrics.Malformed.Add(1);
                    return Results.BadRequest(new { error = validationError });
                }

                var fingerprint = TxMonitoring.Shared.TransactionFingerprint.ComputeV1(
                    req.AccountId,
                    req.SequenceNumber,
                    req.OccurredAt,
                    req.AmountMinor,
                    req.Currency
                );

                await mutationGate.EnterAsync(http.RequestAborted);
                try
                {
                    // A MessageId can be queued, recently settled, or quarantined. Check all three
                    // before reserving capacity so a delayed retry stays idempotent. A matching
                    // fingerprint is a duplicate. A different one is a 409, and the original row
                    // is kept.
                    var live = await db.BufferedTransactions.FirstOrDefaultAsync(
                        x => x.MessageId == canonicalMessageId,
                        http.RequestAborted
                    );
                    if (live is not null)
                    {
                        return live.PayloadFingerprint.SequenceEqual(fingerprint)
                            ? Duplicate(canonicalMessageId, req.AccountId)
                            : Conflict(canonicalMessageId, req.AccountId);
                    }

                    var settled = await db.SettledMessages.FirstOrDefaultAsync(
                        x => x.MessageId == canonicalMessageId,
                        http.RequestAborted
                    );
                    if (settled is not null)
                    {
                        return settled.PayloadFingerprint.SequenceEqual(fingerprint)
                            ? Duplicate(canonicalMessageId, req.AccountId)
                            : Conflict(canonicalMessageId, req.AccountId);
                    }

                    var quarantined = await db.QuarantinedTransactions.FirstOrDefaultAsync(
                        x => x.MessageId == canonicalMessageId,
                        http.RequestAborted
                    );
                    if (quarantined is not null)
                    {
                        if (quarantined.PayloadFingerprint.SequenceEqual(fingerprint))
                        {
                            // Ingestion already rejected this exact content permanently, so answer
                            // with a stable 422 instead of requeueing it.
                            metrics.Duplicates.Add(1);
                            logger.LogDebug(
                                "Quarantined MessageId {MessageId} retried; returning terminal rejection.",
                                canonicalMessageId
                            );
                            return Results.Json(
                                new
                                {
                                    error = "terminal_rejection",
                                    code = quarantined.RejectionCode,
                                },
                                statusCode: StatusCodes.Status422UnprocessableEntity
                            );
                        }

                        return Conflict(canonicalMessageId, req.AccountId);
                    }

                    IResult Duplicate(string messageId, string accountId)
                    {
                        metrics.Duplicates.Add(1);
                        logger.LogDebug(
                            "Duplicate MessageId {MessageId} from {AccountId} ignored.",
                            messageId,
                            accountId
                        );
                        return Results.Accepted();
                    }

                    IResult Conflict(string messageId, string accountId)
                    {
                        metrics.IdempotencyConflicts.Add(1);
                        logger.LogWarning(
                            "Conflicting immutable content for MessageId {MessageId} from {AccountId}; preserving the original.",
                            messageId,
                            accountId
                        );
                        return Results.Conflict(new { error = "idempotency_conflict" });
                    }

                    // Reserve before writing to SQLite. A failed insert releases the reservation.
                    var maxDepth = bufferOptions.Value.MaxDepth;
                    if (!bufferDepth.TryReserve(maxDepth))
                    {
                        metrics.Shed.Add(1);

                        // Jitter keeps a fleet from retrying in lockstep when space reappears.
                        var retryAfter = Random.Shared.Next(2, 15);

                        logger.LogWarning(
                            "Buffer full ({Depth}/{Max}). Shedding {AccountId}, retry in {RetryAfter}s.",
                            bufferDepth.Current,
                            maxDepth,
                            req.AccountId,
                            retryAfter
                        );

                        http.Response.Headers.RetryAfter = retryAfter.ToString();
                        return Results.Json(
                            new { error = "buffer_full", retryAfterSeconds = retryAfter },
                            statusCode: StatusCodes.Status429TooManyRequests
                        );
                    }

                    var traceParent = Activity.Current?.Id;
                    if (traceParent?.Length > MaxTraceParentLength)
                        traceParent = null;

                    var record = new BufferedTransaction
                    {
                        MessageId = canonicalMessageId,
                        PayloadFingerprint = fingerprint,
                        FingerprintVersion = TxMonitoring.Shared.TransactionFingerprint.Version,
                        AccountId = req.AccountId,
                        SequenceNumber = req.SequenceNumber,
                        // The same instant at a different offset is the same data, so store UTC.
                        OccurredAt = req.OccurredAt.ToUniversalTime(),
                        BufferedAt = DateTimeOffset.UtcNow, // gateway-local, not sent upstream
                        TraceParent = traceParent,
                        AmountMinor = req.AmountMinor,
                        Currency = req.Currency,
                    };

                    db.BufferedTransactions.Add(record);

                    try
                    {
                        // One SaveChanges is one SQLite transaction and one fsync. No cancellation
                        // token: once capacity is reserved, a dropped client connection should not
                        // abort the durable write.
                        await db.SaveChangesAsync();

                        metrics.Received.Add(1);

                        return Results.Accepted();
                    }
                    catch (DbUpdateException ex) when (IsDuplicateMessageId(ex))
                    {
                        bufferDepth.Release(1);
                        metrics.Duplicates.Add(1);
                        logger.LogDebug(
                            "Duplicate MessageId {MessageId} from {AccountId} ignored.",
                            canonicalMessageId,
                            req.AccountId
                        );
                        return Results.Accepted();
                    }
                    catch
                    {
                        bufferDepth.Release(1);
                        throw;
                    }
                }
                finally
                {
                    mutationGate.Exit();
                }
            }
        );

        // Counts rows on disk, not the in-memory BufferDepth, so it can be checked without
        // Prometheus.
        app.MapGet(
            "/buffer",
            async (EdgeDbContext db) =>
                Results.Ok(new { queueDepth = await db.BufferedTransactions.CountAsync() })
        );

        // Bounded view of permanent ingestion rejections. The cap keeps one request from loading
        // the whole quarantine into memory.
        app.MapGet(
            "/buffer/quarantine",
            async (int? limit, EdgeDbContext db, CancellationToken cancellationToken) =>
            {
                var take = Math.Clamp(limit ?? 100, 1, 200);
                var total = await db.QuarantinedTransactions.CountAsync(cancellationToken);
                // SQLite's EF provider cannot translate DateTimeOffset ordering, so sort and limit
                // in raw SQL with the limit parameterized.
                var rows = await db
                    .QuarantinedTransactions.FromSqlInterpolated(
                        $"""
                        SELECT *
                        FROM "QuarantinedTransactions"
                        ORDER BY "RejectedAt" DESC, "MessageId" DESC
                        LIMIT {take}
                        """
                    )
                    .AsNoTracking()
                    .ToListAsync(cancellationToken);
                var items = rows
                    .Select(row => new
                    {
                        row.MessageId,
                        row.OriginalQueueId,
                        row.AccountId,
                        row.SequenceNumber,
                        row.OccurredAt,
                        row.BufferedAt,
                        row.TraceParent,
                        row.AmountMinor,
                        row.Currency,
                        row.RejectedAt,
                        row.RejectionCode,
                        row.RejectionReason,
                    })
                    .ToList();

                return Results.Ok(new { total, limit = take, items });
            }
        );
    }

    // 2067 is SQLITE_CONSTRAINT_UNIQUE. MessageId has the only unique index on this table.
    private static bool IsDuplicateMessageId(DbUpdateException ex) =>
        ex.InnerException is SqliteException { SqliteExtendedErrorCode: 2067 };
}
