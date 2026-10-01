using Microsoft.EntityFrameworkCore;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Durable store-and-forward buffer.
/// </summary>
/// <remarks>
/// SQLite is a single file with no server to run, and gives crash recovery and atomic batch
/// deletes. The context is scoped: a DbContext held by a singleton would grow its change tracker
/// without bound and serve stale data.
/// </remarks>
public class EdgeDbContext(DbContextOptions<EdgeDbContext> options) : DbContext(options)
{
    public DbSet<BufferedTransaction> BufferedTransactions => Set<BufferedTransaction>();
    public DbSet<SettledMessage> SettledMessages => Set<SettledMessage>();
    public DbSet<QuarantinedTransaction> QuarantinedTransactions =>
        Set<QuarantinedTransaction>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var buffered = modelBuilder.Entity<BufferedTransaction>();
        buffered.ToTable("BufferedTransactions");

        // Enforces live-queue idempotency in the database, not only in the endpoint's check.
        buffered.HasIndex(r => r.MessageId).IsUnique();
        buffered.Property(r => r.AccountId).HasMaxLength(64);
        buffered.Property(r => r.Currency).HasMaxLength(3);
        buffered.Property(r => r.TraceParent).HasMaxLength(128);

        var settled = modelBuilder.Entity<SettledMessage>();
        settled.HasKey(r => r.MessageId);
        settled.HasIndex(r => r.SettledAt);

        var quarantine = modelBuilder.Entity<QuarantinedTransaction>();
        quarantine.ToTable("QuarantinedTransactions");
        quarantine.HasKey(r => r.MessageId);
        quarantine.Property(r => r.AccountId).HasMaxLength(64);
        quarantine.Property(r => r.Currency).HasMaxLength(3);
        quarantine.Property(r => r.TraceParent).HasMaxLength(128);
        quarantine.Property(r => r.RejectionCode).HasMaxLength(64);
        quarantine.Property(r => r.RejectionReason).HasMaxLength(512);
        quarantine.HasIndex(r => r.RejectedAt);
    }
}
