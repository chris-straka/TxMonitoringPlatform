using System.Data.Common;

using Microsoft.EntityFrameworkCore.Diagnostics;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Applies durability PRAGMAs to every pooled SQLite connection.
/// </summary>
/// <remarks>
/// journal_mode=WAL persists in the database file, but synchronous is per connection and EF
/// pools connections, so setting it once at startup would cover only the first connection.
/// FULL survives power loss, where NORMAL survives only a process kill. An edge gateway is the
/// machine most likely to lose power, so it pays an fsync per commit.
/// </remarks>
public class SqlitePragmaInterceptor : DbConnectionInterceptor
{
    public override void ConnectionOpened(DbConnection connection, ConnectionEndEventData eventData)
    {
        Apply(connection);
        base.ConnectionOpened(connection, eventData);
    }

    public override async Task ConnectionOpenedAsync(
        DbConnection connection,
        ConnectionEndEventData eventData,
        CancellationToken cancellationToken = default
    )
    {
        Apply(connection);
        await base.ConnectionOpenedAsync(connection, eventData, cancellationToken);
    }

    private static void Apply(DbConnection connection)
    {
        using var cmd = connection.CreateCommand();
        // busy_timeout makes a concurrent reader or writer wait instead of failing.
        cmd.CommandText = "PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;";
        cmd.ExecuteNonQuery();
    }
}
