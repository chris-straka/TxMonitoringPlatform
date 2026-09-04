using Microsoft.EntityFrameworkCore;

using TxMonitoring.Detection.Worker.Features.Diagnostics;
using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Tests;

public sealed class AppDbContextModelTests
{
    [Fact]
    public void Alert_outbox_has_the_idempotency_and_pending_scan_indexes()
    {
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql("Host=localhost;Database=model_test;Username=test;Password=test")
            .Options;
        using var db = new AppDbContext(options);

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
        Assert.Contains("20260902000000_AddAlertOutbox", db.Database.GetMigrations());
    }
}
