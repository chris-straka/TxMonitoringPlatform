using System.Text.Json.Serialization;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// Availability of the velocity evidence behind a decision. Only <see cref="Available"/>
/// carries evidence; every other state persists null evidence, never zeros.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum VelocityStatus
{
    Available,
    Unavailable,
    OutOfOrder,
    Rebuilding,
}

/// <summary>
/// The velocity-window evidence a policy evaluation observed. The (T - window, T] interval
/// uses OccurredAt, so draining an outage does not manufacture a burst from old events.
/// </summary>
public sealed record VelocitySnapshot(VelocityStatus Status, long? Count1m, long? Amount5mMinor)
{
    public static VelocitySnapshot Available(long count1m, long amount5mMinor) =>
        new(VelocityStatus.Available, count1m, amount5mMinor);

    public static VelocitySnapshot Unavailable() => new(VelocityStatus.Unavailable, null, null);

    public static VelocitySnapshot OutOfOrder() => new(VelocityStatus.OutOfOrder, null, null);

    public static VelocitySnapshot Rebuilding() => new(VelocityStatus.Rebuilding, null, null);
}

/// <summary>
/// Atomic velocity observation. Implemented by Redis in production and by an explicit
/// unavailable stub where Redis is absent; the policy treats anything but
/// <see cref="VelocityStatus.Available"/> as missing evidence, never as zero counts.
/// </summary>
public interface IVelocityStore
{
    Task<VelocitySnapshot> ObserveAsync(
        string accountId,
        string currency,
        Guid messageId,
        long amountMinor,
        DateTimeOffset occurredAt,
        CancellationToken cancellationToken
    );
}

/// <summary>
/// Degraded-mode velocity: Redis is not wired (or is unreachable), so every observation
/// reports <see cref="VelocityStatus.Unavailable"/> and decisions score from the remaining
/// signals. Losing velocity loses accuracy for a window, never a transaction.
/// </summary>
public sealed class UnavailableVelocityStore : IVelocityStore
{
    public Task<VelocitySnapshot> ObserveAsync(
        string accountId,
        string currency,
        Guid messageId,
        long amountMinor,
        DateTimeOffset occurredAt,
        CancellationToken cancellationToken
    ) => Task.FromResult(VelocitySnapshot.Unavailable());
}
