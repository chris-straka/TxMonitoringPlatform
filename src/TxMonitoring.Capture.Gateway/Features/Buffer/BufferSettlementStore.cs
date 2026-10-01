using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using TxMonitoring.Capture.Gateway.Configuration;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Atomically moves rows settled by ingestion out of the live queue.
/// </summary>
public sealed class BufferSettlementStore(
    EdgeDbContext db,
    BufferMutationGate mutationGate,
    BufferDepth bufferDepth,
    IOptions<BufferOptions> bufferOptions
)
{
    public const string IngestionRejectionCode = "ingestion_permanent_rejection";

    // The response carries rejected IDs but no reason or error code, so store a generic reason
    // rather than guess one.
    public const string IngestionRejectionReason =
        "Ingestion named this MessageId as permanently rejected; the current protocol does not provide a per-reading reason.";

    private readonly TimeSpan _settledIdRetention = TimeSpan.FromHours(
        bufferOptions.Value.SettledIdRetentionHours
    );
    private readonly TimeSpan _quarantineRetention = TimeSpan.FromHours(
        bufferOptions.Value.QuarantineRetentionHours
    );
    private readonly int _quarantineMaxRows = bufferOptions.Value.QuarantineMaxRows;

    public async Task SettleAsync(
        IReadOnlyCollection<BufferedTransaction> settledRows,
        IReadOnlyCollection<string> rejectedIds,
        CancellationToken cancellationToken
    )
    {
        if (settledRows.Count == 0)
            return;

        var settledIds = settledRows
            .Select(row => row.MessageId)
            .ToHashSet(StringComparer.Ordinal);
        var rejected = rejectedIds.ToHashSet(StringComparer.Ordinal);
        if (!rejected.IsSubsetOf(settledIds))
            throw new InvalidOperationException(
                "A rejected MessageId must refer to a row being settled."
            );

        await mutationGate.EnterAsync(cancellationToken);
        try
        {
            // The quarantine insert, settled marker, and live-row delete share one transaction.
            // The receiver holds the same gate while checking IDs, so it cannot see a MessageId
            // mid-transition.
            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);

            var settledAt = DateTimeOffset.UtcNow;
            var rejectedRows = settledRows.Where(row => rejected.Contains(row.MessageId)).ToList();
            HashSet<string> alreadyQuarantinedIds = rejectedRows.Count == 0
                ? []
                : (await db.QuarantinedTransactions
                    .Where(row => rejected.Contains(row.MessageId))
                    .Select(row => row.MessageId)
                    .ToListAsync(cancellationToken))
                    .ToHashSet(StringComparer.Ordinal);

            db.SettledMessages.AddRange(
                settledRows.Select(row => new SettledMessage
                {
                    MessageId = row.MessageId,
                    SettledAt = settledAt,
                    PayloadFingerprint = row.PayloadFingerprint,
                    FingerprintVersion = row.FingerprintVersion,
                })
            );
            db.QuarantinedTransactions.AddRange(
                rejectedRows
                    // An existing buffer can hold a queued row whose ID is already quarantined.
                    // Keep the first quarantine copy and let the queued row settle so it does not
                    // block the queue.
                    .Where(row => !alreadyQuarantinedIds.Contains(row.MessageId))
                    .Select(row => new QuarantinedTransaction
                    {
                        MessageId = row.MessageId,
                        PayloadFingerprint = row.PayloadFingerprint,
                        FingerprintVersion = row.FingerprintVersion,
                        OriginalQueueId = row.Id,
                        AccountId = row.AccountId,
                        SequenceNumber = row.SequenceNumber,
                        OccurredAt = row.OccurredAt,
                        BufferedAt = row.BufferedAt,
                        TraceParent = row.TraceParent,
                        AmountMinor = row.AmountMinor,
                        Currency = row.Currency,
                        RejectedAt = settledAt,
                        RejectionCode = IngestionRejectionCode,
                        RejectionReason = IngestionRejectionReason,
                    })
            );
            db.BufferedTransactions.RemoveRange(settledRows);
            await db.SaveChangesAsync(cancellationToken);

            var settledCutoff = settledAt - _settledIdRetention;
            // SQLite's EF provider cannot translate DateTimeOffset comparisons, though the stored
            // UTC text sorts correctly.
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                DELETE FROM "SettledMessages"
                WHERE "SettledAt" < {settledCutoff};
                """,
                cancellationToken
            );

            var quarantineCutoff = settledAt - _quarantineRetention;
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                DELETE FROM "QuarantinedTransactions"
                WHERE "RejectedAt" < {quarantineCutoff};
                """,
                cancellationToken
            );

            // The age bound keeps rows relevant. This count bound caps disk use during a sustained
            // run of rejections. SQLite reuses the freed pages.
            var quarantineCount = await db.QuarantinedTransactions.CountAsync(
                cancellationToken
            );
            var excessRows = quarantineCount - _quarantineMaxRows;
            if (excessRows > 0)
            {
                await db.Database.ExecuteSqlInterpolatedAsync(
                    $"""
                    DELETE FROM "QuarantinedTransactions"
                    WHERE "MessageId" IN (
                        SELECT "MessageId"
                        FROM "QuarantinedTransactions"
                        ORDER BY "RejectedAt", "MessageId"
                        LIMIT {excessRows}
                    );
                    """,
                    cancellationToken
                );
            }

            await transaction.CommitAsync(cancellationToken);

            // Release only after the commit. If the process dies in between, the startup recount
            // repairs the depth.
            bufferDepth.Release(settledRows.Count);
        }
        finally
        {
            mutationGate.Exit();
        }
    }
}
