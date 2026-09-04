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
        transactions.Property(x => x.VelocityStatus).HasMaxLength(16);
        transactions.Property(x => x.PolicyVersion).HasMaxLength(64);
        transactions.Property(x => x.PolicyConfigurationHash).HasMaxLength(64);

        // Money, score, and count bounds live with the data, not only in validators.
        transactions.HasCheckConstraint(
            "CK_Transactions_AmountMinor",
            "\"AmountMinor\" >= 1 AND \"AmountMinor\" <= 100000000000"
        );
        transactions.HasCheckConstraint(
            "CK_Transactions_Currency",
            "char_length(\"Currency\") = 3"
        );
        transactions.HasCheckConstraint(
            "CK_Transactions_RiskScore",
            "\"RiskScore\" >= 0 AND \"RiskScore\" <= 100"
        );
        transactions.HasCheckConstraint(
            "CK_Transactions_VelocityCounts",
            "\"Count1m\" IS NULL OR \"Count1m\" >= 0"
        );
        transactions.HasCheckConstraint(
            "CK_Transactions_VelocitySums",
            "\"Amount5mMinor\" IS NULL OR \"Amount5mMinor\" >= 0"
        );

        var outbox = modelBuilder.Entity<AlertOutboxMessage>();
        outbox.Property(x => x.AccountId).HasMaxLength(64);
        outbox.Property(x => x.TraceParent).HasMaxLength(128);
        outbox.Property(x => x.LastError).HasMaxLength(2_000);
    }
}
