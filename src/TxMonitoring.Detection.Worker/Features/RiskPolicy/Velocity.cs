using System.Text.Json.Serialization;

namespace TxMonitoring.Detection.Worker.Features.RiskPolicy;

/// <summary>
/// Availability of the velocity evidence behind a decision. Only <see cref="Available"/>
/// carries evidence. Every other state persists null, not zeros.
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
/// The velocity-window evidence a policy evaluation saw. The (T - window, T] interval uses
/// OccurredAt, so draining a backlog after an outage does not look like a burst.
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
/// Atomic velocity observation, backed by Redis. The policy treats anything but
/// <see cref="VelocityStatus.Available"/> as missing evidence, not zero counts.
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
