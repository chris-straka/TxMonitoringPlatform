using Microsoft.EntityFrameworkCore;

using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// The single repository seam for cold velocity and detector state. Both warm paths read
/// through here so restarts and rebalances converge on the same recent Postgres rows.
/// </summary>
public interface ITransactionHistoryRepository
{
    /// <summary>
    /// Most recent rows for one identity, newest first by detection order (DetectedAt, Id),
    /// capped at <paramref name="limit"/>.
    /// </summary>
    Task<IReadOnlyList<(Guid MessageId, DateTimeOffset OccurredAt, long AmountMinor)>> LoadRecentAsync(
        string accountId,
        string currency,
        int limit,
        CancellationToken cancellationToken
    );
}

public sealed class TransactionHistoryRepository(IDbContextFactory<AppDbContext> contextFactory)
    : ITransactionHistoryRepository
{
    public async Task<IReadOnlyList<(Guid MessageId, DateTimeOffset OccurredAt, long AmountMinor)>> LoadRecentAsync(
        string accountId,
        string currency,
        int limit,
        CancellationToken cancellationToken
    )
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        var rows = await db
            .Transactions.Where(r => r.AccountId == accountId && r.Currency == currency)
            .OrderByDescending(r => r.DetectedAt)
            .ThenByDescending(r => r.Id)
            .Take(limit)
            .Select(r => new { r.MessageId, r.OccurredAt, r.AmountMinor })
            .ToListAsync(cancellationToken);
        return rows.Select(r => (r.MessageId, r.OccurredAt, r.AmountMinor)).ToList();
    }
}
