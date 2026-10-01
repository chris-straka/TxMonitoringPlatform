namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Tracks the number of buffered transactions and reserves capacity atomically.
/// </summary>
/// <remarks>
/// SQLite keeps no row count, so COUNT(*) walks every row. Startup seeds this value from disk,
/// then each committed insert and delete moves it by a matching delta. Compare-exchange makes
/// MaxDepth an exact ceiling under concurrent requests. A crash cannot leave it stale because
/// startup recounts the durable rows.
/// </remarks>
public sealed class BufferDepth
{
    private long _current;

    public long Current => Interlocked.Read(ref _current);

    public bool TryReserve(long maxDepth)
    {
        while (true)
        {
            var current = Interlocked.Read(ref _current);
            if (current >= maxDepth)
                return false;

            if (Interlocked.CompareExchange(ref _current, current + 1, current) == current)
                return true;
        }
    }

    /// <summary>Releases capacity held by rows whose deletion has committed.</summary>
    /// <remarks>
    /// An underflow throws before the counter is written, so a bug cannot corrupt it.
    /// </remarks>
    public void Release(long count)
    {
        if (count <= 0)
            return;

        while (true)
        {
            var current = Interlocked.Read(ref _current);
            if (count > current)
            {
                throw new InvalidOperationException(
                    "The in-memory buffer depth would fall below zero."
                );
            }

            if (Interlocked.CompareExchange(ref _current, current - count, current) == current)
                return;
        }
    }

    /// <summary>Seeds the counter from the durable row count before the server starts.</summary>
    public void Initialize(long depth)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(depth);
        Interlocked.Exchange(ref _current, depth);
    }
}
