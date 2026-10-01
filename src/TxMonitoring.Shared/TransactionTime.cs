namespace TxMonitoring.Shared;

/// <summary>
/// Origin event-time helpers. Origins mint whole-millisecond instants because validators
/// reject finer precision rather than silently round it.
/// </summary>
public static class TransactionTime
{
    public static DateTimeOffset TruncateToMilliseconds(DateTimeOffset value) =>
        new(
            value.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond,
            value.Offset
        );
}
