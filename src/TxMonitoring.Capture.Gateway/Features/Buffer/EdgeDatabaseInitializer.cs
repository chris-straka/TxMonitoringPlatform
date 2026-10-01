using Microsoft.EntityFrameworkCore;

using TxMonitoring.Capture.Gateway.Configuration;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Adds backwards-compatible tables that EnsureCreated cannot add to an existing SQLite buffer.
/// </summary>
/// <remarks>
/// Only the current schema is created. Buffer files from the earlier telemetry schema are not
/// migrated, so deployments start with fresh volumes.
/// </remarks>
public static class EdgeDatabaseInitializer
{
    public static async Task EnsureCompatibleSchemaAsync(
        EdgeDbContext db,
        BufferOptions bufferOptions,
        CancellationToken cancellationToken = default
    )
    {
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS "BufferedTransactions" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_BufferedTransactions" PRIMARY KEY AUTOINCREMENT,
                "MessageId" TEXT NOT NULL,
                "PayloadFingerprint" BLOB NOT NULL,
                "FingerprintVersion" INTEGER NOT NULL,
                "AccountId" TEXT NOT NULL,
                "SequenceNumber" INTEGER NOT NULL,
                "OccurredAt" TEXT NOT NULL,
                "BufferedAt" TEXT NOT NULL,
                "TraceParent" TEXT NULL,
                "AmountMinor" INTEGER NOT NULL,
                "Currency" TEXT NOT NULL
            );
            """,
            cancellationToken
        );
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS "IX_BufferedTransactions_MessageId"
            ON "BufferedTransactions" ("MessageId");
            """,
            cancellationToken
        );
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS "SettledMessages" (
                "MessageId" TEXT NOT NULL CONSTRAINT "PK_SettledMessages" PRIMARY KEY,
                "SettledAt" TEXT NOT NULL,
                "PayloadFingerprint" BLOB NOT NULL,
                "FingerprintVersion" INTEGER NOT NULL
            );
            """,
            cancellationToken
        );
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE INDEX IF NOT EXISTS "IX_SettledMessages_SettledAt"
            ON "SettledMessages" ("SettledAt");
            """,
            cancellationToken
        );
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE TABLE IF NOT EXISTS "QuarantinedTransactions" (
                "MessageId" TEXT NOT NULL CONSTRAINT "PK_QuarantinedTransactions" PRIMARY KEY,
                "PayloadFingerprint" BLOB NOT NULL,
                "FingerprintVersion" INTEGER NOT NULL,
                "OriginalQueueId" INTEGER NOT NULL,
                "AccountId" TEXT NOT NULL,
                "SequenceNumber" INTEGER NOT NULL,
                "OccurredAt" TEXT NOT NULL,
                "BufferedAt" TEXT NOT NULL,
                "TraceParent" TEXT NULL,
                "AmountMinor" INTEGER NOT NULL,
                "Currency" TEXT NOT NULL,
                "RejectedAt" TEXT NOT NULL,
                "RejectionCode" TEXT NOT NULL,
                "RejectionReason" TEXT NOT NULL
            );
            """,
            cancellationToken
        );
        await db.Database.ExecuteSqlRawAsync(
            """
            CREATE INDEX IF NOT EXISTS "IX_QuarantinedTransactions_RejectedAt"
            ON "QuarantinedTransactions" ("RejectedAt");
            """,
            cancellationToken
        );

        // Apply both bounds at startup in case they changed. Normal pruning happens in the
        // uploader's settlement transaction.
        var quarantineCutoff = DateTimeOffset.UtcNow.AddHours(
            -bufferOptions.QuarantineRetentionHours
        );
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"""
            DELETE FROM "QuarantinedTransactions"
            WHERE "RejectedAt" < {quarantineCutoff};
            """,
            cancellationToken
        );
        await db.Database.ExecuteSqlInterpolatedAsync(
            $"""
            DELETE FROM "QuarantinedTransactions"
            WHERE "MessageId" IN (
                SELECT "MessageId"
                FROM "QuarantinedTransactions"
                ORDER BY "RejectedAt" DESC, "MessageId" DESC
                LIMIT -1 OFFSET {bufferOptions.QuarantineMaxRows}
            );
            """,
            cancellationToken
        );
    }
}
