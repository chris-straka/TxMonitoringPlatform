namespace TxMonitoring.Shared;

/// <summary>
/// Origin event-time helpers. The contract resolution is one millisecond: origins must
/// mint whole-millisecond instants, because validators reject sub-millisecond precision
/// as malformed rather than silently rounding money-movement evidence.
/// </summary>
public static class TransactionTime
{
    public static DateTimeOffset TruncateToMilliseconds(DateTimeOffset value) =>
        new(
            value.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond,
            value.Offset
        );
}
