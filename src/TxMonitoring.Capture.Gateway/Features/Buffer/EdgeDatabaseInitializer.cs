using Microsoft.EntityFrameworkCore;

using TxMonitoring.Capture.Gateway.Configuration;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Adds backwards-compatible tables that EnsureCreated cannot add to an existing SQLite buffer.
/// </summary>
/// <remarks>
/// This creates the target transaction schema only. An industrial-era buffer file is not
/// migrated: the port starts with fresh volumes, and old tables are never renamed in place.
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
                "SettledAt" TEXT NOT NULL
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

        // Enforce both bounds immediately after an upgrade/reconfiguration. Normal pruning happens
        // inside the uploader's settlement transaction.
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
