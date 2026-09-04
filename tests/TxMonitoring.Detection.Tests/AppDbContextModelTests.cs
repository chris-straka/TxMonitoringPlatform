using Microsoft.EntityFrameworkCore;

using TxMonitoring.Detection.Worker.Features.Diagnostics;
using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Tests;

public sealed class AppDbContextModelTests
{
    [Fact]
    public void Alert_outbox_has_the_idempotency_and_pending_scan_indexes()
    {
        using var db = CreateDb();

        var entity = db.Model.FindEntityType(typeof(AlertOutboxMessage));

        Assert.NotNull(entity);
        Assert.Contains(
            entity.GetIndexes(),
            index => index.IsUnique && index.Properties.Single().Name == nameof(AlertOutboxMessage.MessageId)
        );
        Assert.Contains(
            entity.GetIndexes(),
            index => index.Properties.Select(property => property.Name).SequenceEqual(
                [nameof(AlertOutboxMessage.PublishedAt), nameof(AlertOutboxMessage.CreatedAt)]
            )
        );
        Assert.Contains("20260904194005_DecisionEvidence", db.Database.GetMigrations());
    }

    [Fact]
    public void Transactions_store_integer_money_with_unique_message_ids()
    {
        using var db = CreateDb();

        var entity = db.Model.FindEntityType(typeof(MonitoredTransaction));

        Assert.NotNull(entity);
        Assert.Equal("Transactions", entity.GetTableName());
        Assert.Contains(
            entity.GetIndexes(),
            index => index.IsUnique && index.Properties.Single().Name == nameof(MonitoredTransaction.MessageId)
        );
        Assert.Equal(
            "bigint",
            entity.FindProperty(nameof(MonitoredTransaction.AmountMinor))!.GetColumnType()
        );
        Assert.Equal(
            64,
            entity.FindProperty(nameof(MonitoredTransaction.AccountId))!.GetMaxLength()
        );
        Assert.Equal(
            3,
            entity.FindProperty(nameof(MonitoredTransaction.Currency))!.GetMaxLength()
        );
    }

    private static AppDbContext CreateDb()
    {
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql("Host=localhost;Database=model_test;Username=test;Password=test")
            .Options;
        return new AppDbContext(options);
    }
}
