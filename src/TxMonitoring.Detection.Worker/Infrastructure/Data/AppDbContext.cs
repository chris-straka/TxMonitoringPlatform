using Microsoft.EntityFrameworkCore;

using TxMonitoring.Detection.Worker.Features.Diagnostics;

namespace TxMonitoring.Detection.Worker.Infrastructure.Data;

// Creates the Transactions table in my DB
public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<MonitoredTransaction> Transactions => Set<MonitoredTransaction>();
    public DbSet<AlertOutboxMessage> AlertOutboxMessages => Set<AlertOutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var transactions = modelBuilder.Entity<MonitoredTransaction>();
        transactions.ToTable("Transactions");
        transactions.Property(x => x.DetectorVersion).HasMaxLength(64);
        transactions.Property(x => x.AccountId).HasMaxLength(64);
        transactions.Property(x => x.Currency).HasMaxLength(3);

        var outbox = modelBuilder.Entity<AlertOutboxMessage>();
        outbox.Property(x => x.AccountId).HasMaxLength(64);
        outbox.Property(x => x.TraceParent).HasMaxLength(128);
        outbox.Property(x => x.LastError).HasMaxLength(2_000);
    }
}
