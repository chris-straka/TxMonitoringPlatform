using System.ComponentModel.DataAnnotations;
using System.Diagnostics.Metrics;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;

namespace TxMonitoring.Detection.Tests;

public sealed class VelocityOptionsTests
{
    [Fact]
    public void Default_options_validate()
    {
        var results = Validate(new VelocityOptions());

        Assert.Empty(results);
    }

    [Fact]
    public void Empty_configuration_is_rejected()
    {
        var results = Validate(new VelocityOptions { Configuration = "  " });

        Assert.Contains(results, r => r.MemberNames.Contains(nameof(VelocityOptions.Configuration)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(100_001)]
    public void Rebuild_limits_are_bounded(int limit)
    {
        var results = Validate(new VelocityOptions { LazyRebuildLimit = limit });

        Assert.Contains(results, r => r.MemberNames.Contains(nameof(VelocityOptions.LazyRebuildLimit)));
    }

    private static List<ValidationResult> Validate(VelocityOptions options)
    {
        var results = new List<ValidationResult>();
        Validator.TryValidateObject(
            options,
            new ValidationContext(options),
            results,
            validateAllProperties: true
        );
        return results;
    }
}

public sealed class VelocityDegradationTests
{
    [Fact]
    public async Task Unreachable_redis_degrades_to_unavailable_and_counts_it()
    {
        var measurements = new List<long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (
                instrument.Meter.Name == WorkerMetrics.MeterName
                && instrument.Name == "detection.velocity.unavailable"
            )
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => measurements.Add(value));
        listener.Start();

        var metrics = new WorkerMetrics();
        var store = CreateStore(
            new VelocityOptions
            {
                Configuration = "invalid.invalid:6379",
                ConnectTimeoutMs = 500,
                SyncTimeoutMs = 500,
            },
            metrics
        );

        var snapshot = await store.ObserveAsync(
            "ACCT-0001",
            "CAD",
            Guid.CreateVersion7(),
            2_500,
            DateTimeOffset.UtcNow,
            CancellationToken.None
        );

        Assert.Equal(VelocityStatus.Unavailable, snapshot.Status);
        Assert.Null(snapshot.Count1m);
        Assert.Null(snapshot.Amount5mMinor);
        Assert.Equal([1], measurements);
    }

    [Fact]
    public async Task Key_unsafe_identity_degrades_without_touching_redis()
    {
        var metrics = new WorkerMetrics();
        var store = CreateStore(new VelocityOptions(), metrics);

        var snapshot = await store.ObserveAsync(
            "ACCT 0001\nINJECTED",
            "CAD",
            Guid.CreateVersion7(),
            2_500,
            DateTimeOffset.UtcNow,
            CancellationToken.None
        );

        Assert.Equal(VelocityStatus.Unavailable, snapshot.Status);
    }

    private static RedisVelocityStore CreateStore(VelocityOptions options, WorkerMetrics metrics) =>
        new(
            Options.Create(options),
            new EmptyHistory(),
            new RedisConnection(
                Options.Create(options),
                NullLogger<RedisConnection>.Instance
            ),
            TimeProvider.System,
            metrics,
            NullLogger<RedisVelocityStore>.Instance
        );

    private sealed class EmptyHistory : ITransactionHistoryRepository
    {
        public Task<IReadOnlyList<(Guid MessageId, DateTimeOffset OccurredAt, long AmountMinor)>> LoadRecentAsync(
            string accountId,
            string currency,
            int limit,
            CancellationToken cancellationToken
        ) =>
            Task.FromResult<IReadOnlyList<(Guid, DateTimeOffset, long)>>([]);
    }
}
