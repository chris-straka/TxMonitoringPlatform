using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

using TxMonitoring.Capture.Gateway.Configuration;
using TxMonitoring.Capture.Gateway.Features.Buffer;

namespace TxMonitoring.Capture.Tests;

public sealed class BufferSettlementStoreTests
{
    [Fact]
    public async Task Rejection_is_quarantined_with_original_payload_in_same_settlement()
    {
        await using var connection = await OpenConnectionAsync();
        var dbOptions = new DbContextOptionsBuilder<EdgeDbContext>()
            .UseSqlite(connection)
            .Options;
        await using var db = new EdgeDbContext(dbOptions);
        await db.Database.EnsureCreatedAsync();

        var accepted = Reading("ACCT-accepted", sequence: 10);
        var rejected = Reading("ACCT-rejected", sequence: 20);
        db.BufferedTransactions.AddRange(accepted, rejected);
        await db.SaveChangesAsync();

        var depth = new BufferDepth();
        depth.Initialize(2);
        using var gate = new BufferMutationGate();
        var store = CreateStore(db, gate, depth, quarantineMaxRows: 100);

        await store.SettleAsync(
            [accepted, rejected],
            [rejected.MessageId],
            CancellationToken.None
        );

        Assert.Equal(0, await db.BufferedTransactions.CountAsync());
        Assert.Equal(2, await db.SettledMessages.CountAsync());
        Assert.Equal(0, depth.Current);

        var quarantined = await db.QuarantinedTransactions.SingleAsync();
        Assert.Equal(rejected.MessageId, quarantined.MessageId);
        Assert.Equal(rejected.PayloadFingerprint, quarantined.PayloadFingerprint);
        Assert.Equal(rejected.Id, quarantined.OriginalQueueId);
        Assert.Equal(rejected.AccountId, quarantined.AccountId);
        Assert.Equal(rejected.SequenceNumber, quarantined.SequenceNumber);
        Assert.Equal(rejected.OccurredAt, quarantined.OccurredAt);
        Assert.Equal(rejected.BufferedAt, quarantined.BufferedAt);
        Assert.Equal(rejected.TraceParent, quarantined.TraceParent);
        Assert.Equal(rejected.AmountMinor, quarantined.AmountMinor);
        Assert.Equal(rejected.Currency, quarantined.Currency);
        Assert.Equal(BufferSettlementStore.IngestionRejectionCode, quarantined.RejectionCode);
        Assert.Contains("does not provide a per-reading reason", quarantined.RejectionReason);
    }

    [Fact]
    public async Task Quarantine_prunes_expired_rows_and_enforces_the_hard_row_limit()
    {
        await using var connection = await OpenConnectionAsync();
        var dbOptions = new DbContextOptionsBuilder<EdgeDbContext>()
            .UseSqlite(connection)
            .Options;
        await using var db = new EdgeDbContext(dbOptions);
        await db.Database.EnsureCreatedAsync();

        var expired = Quarantined(Reading("ACCT-old", sequence: 1), DateTimeOffset.UtcNow.AddDays(-2));
        var rejected = Enumerable.Range(2, 3).Select(i => Reading("ACCT-new", i)).ToList();
        db.QuarantinedTransactions.Add(expired);
        db.BufferedTransactions.AddRange(rejected);
        await db.SaveChangesAsync();

        var depth = new BufferDepth();
        depth.Initialize(rejected.Count);
        using var gate = new BufferMutationGate();
        var store = CreateStore(db, gate, depth, quarantineMaxRows: 2);

        await store.SettleAsync(
            rejected,
            rejected.Select(row => row.MessageId).ToList(),
            CancellationToken.None
        );

        Assert.False(
            await db.QuarantinedTransactions.AnyAsync(row => row.MessageId == expired.MessageId)
        );
        Assert.Equal(2, await db.QuarantinedTransactions.CountAsync());
        Assert.Equal(0, depth.Current);
    }

    [Fact]
    public async Task Existing_quarantine_row_does_not_wedge_rejection_settlement()
    {
        await using var connection = await OpenConnectionAsync();
        var dbOptions = new DbContextOptionsBuilder<EdgeDbContext>()
            .UseSqlite(connection)
            .Options;
        await using var db = new EdgeDbContext(dbOptions);
        await db.Database.EnsureCreatedAsync();

        var rejected = Reading("ACCT-readmitted", sequence: 3);
        var firstRejectedAt = DateTimeOffset.UtcNow.AddHours(-1);
        db.BufferedTransactions.Add(rejected);
        db.QuarantinedTransactions.Add(Quarantined(rejected, firstRejectedAt));
        await db.SaveChangesAsync();

        var depth = new BufferDepth();
        depth.Initialize(1);
        using var gate = new BufferMutationGate();
        var store = CreateStore(db, gate, depth, quarantineMaxRows: 100);

        await store.SettleAsync([rejected], [rejected.MessageId], CancellationToken.None);

        Assert.Empty(await db.BufferedTransactions.ToListAsync());
        Assert.Single(await db.SettledMessages.ToListAsync());
        var quarantined = await db.QuarantinedTransactions.SingleAsync();
        Assert.Equal(rejected.MessageId, quarantined.MessageId);
        Assert.Equal(firstRejectedAt, quarantined.RejectedAt);
        Assert.Equal(0, depth.Current);
    }

    [Fact]
    public async Task Settlement_failure_rolls_back_quarantine_and_live_row_delete()
    {
        await using var connection = await OpenConnectionAsync();
        var dbOptions = new DbContextOptionsBuilder<EdgeDbContext>()
            .UseSqlite(connection)
            .Options;
        await using var db = new EdgeDbContext(dbOptions);
        await db.Database.EnsureCreatedAsync();

        var rejected = Reading("ACCT-conflict", sequence: 1);
        db.BufferedTransactions.Add(rejected);
        db.SettledMessages.Add(
            new SettledMessage
            {
                MessageId = rejected.MessageId,
                SettledAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            }
        );
        await db.SaveChangesAsync();
        db.ChangeTracker.Clear();

        var trackedReading = await db.BufferedTransactions.SingleAsync();
        var depth = new BufferDepth();
        depth.Initialize(1);
        using var gate = new BufferMutationGate();
        var store = CreateStore(db, gate, depth, quarantineMaxRows: 100);

        await Assert.ThrowsAsync<DbUpdateException>(() =>
            store.SettleAsync(
                [trackedReading],
                [trackedReading.MessageId],
                CancellationToken.None
            )
        );

        await using var verificationDb = new EdgeDbContext(dbOptions);
        Assert.Equal(1, await verificationDb.BufferedTransactions.CountAsync());
        Assert.Equal(0, await verificationDb.QuarantinedTransactions.CountAsync());
        Assert.Equal(1, depth.Current);
    }

    private static BufferSettlementStore CreateStore(
        EdgeDbContext db,
        BufferMutationGate gate,
        BufferDepth depth,
        int quarantineMaxRows
    ) =>
        new(
            db,
            gate,
            depth,
            Options.Create(
                new BufferOptions
                {
                    Path = "unused.db",
                    MaxDepth = 100,
                    SettledIdRetentionHours = 24,
                    QuarantineRetentionHours = 24,
                    QuarantineMaxRows = quarantineMaxRows,
                }
            )
        );

    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    private static BufferedTransaction Reading(string accountId, long sequence)
    {
        var occurredAt = DateTimeOffset.UtcNow.AddSeconds(-2);
        return new()
        {
            MessageId = Guid.CreateVersion7().ToString("D"),
            PayloadFingerprint = TxMonitoring.Shared.TransactionFingerprint.ComputeV1(
                accountId,
                sequence,
                occurredAt,
                2_500,
                "CAD"
            ),
            FingerprintVersion = TxMonitoring.Shared.TransactionFingerprint.Version,
            AccountId = accountId,
            SequenceNumber = sequence,
            OccurredAt = occurredAt,
            BufferedAt = DateTimeOffset.UtcNow.AddSeconds(-1),
            TraceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
            AmountMinor = 2_500,
            Currency = "CAD",
        };
    }

    private static QuarantinedTransaction Quarantined(
        BufferedTransaction reading,
        DateTimeOffset rejectedAt
    ) =>
        new()
        {
            MessageId = reading.MessageId,
            OriginalQueueId = reading.Id,
            AccountId = reading.AccountId,
            SequenceNumber = reading.SequenceNumber,
            OccurredAt = reading.OccurredAt,
            BufferedAt = reading.BufferedAt,
            TraceParent = reading.TraceParent,
            AmountMinor = reading.AmountMinor,
            Currency = reading.Currency,
            RejectedAt = rejectedAt,
            RejectionCode = BufferSettlementStore.IngestionRejectionCode,
            RejectionReason = BufferSettlementStore.IngestionRejectionReason,
        };
}
