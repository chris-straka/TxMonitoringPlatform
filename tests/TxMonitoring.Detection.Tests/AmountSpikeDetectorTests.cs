using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Tests;

public sealed class AmountSpikeDetectorTests
{
    [Fact]
    public async Task Isolation_and_invalidation()
    {
        using var detector = new AmountSpikeDetector(
            Path.Combine(AppContext.BaseDirectory, "model.zip")
        );
        var cadLoads = 0;
        var usdLoads = 0;

        Task<IReadOnlyList<long>> LoadCad(CancellationToken _)
        {
            cadLoads++;
            return Task.FromResult<IReadOnlyList<long>>([2_500, 2_600, 2_400]);
        }

        Task<IReadOnlyList<long>> LoadUsd(CancellationToken _)
        {
            usdLoads++;
            return Task.FromResult<IReadOnlyList<long>>([2_500, 2_600, 2_400]);
        }

        var firstCad = await detector.InspectAsync("ACCT-A", "CAD", 2_550, LoadCad, CancellationToken.None);
        var secondCad = await detector.InspectAsync("ACCT-A", "CAD", 2_560, LoadCad, CancellationToken.None);
        await detector.InspectAsync("ACCT-A", "USD", 2_550, LoadUsd, CancellationToken.None);

        Assert.Equal(1, cadLoads);
        Assert.Equal(1, usdLoads);
        Assert.Equal(3, firstCad.HistoryCount);
        Assert.Equal(4, secondCad.HistoryCount);
        Assert.Equal(64, firstCad.DetectorVersion.Length);
        Assert.Equal(detector.DetectorVersion, firstCad.DetectorVersion);

        detector.Invalidate("ACCT-A", "CAD");
        var rebuiltCad = await detector.InspectAsync("ACCT-A", "CAD", 2_570, LoadCad, CancellationToken.None);

        Assert.Equal(2, cadLoads);
        Assert.Equal(1, usdLoads);
        Assert.Equal(3, rebuiltCad.HistoryCount);
    }

    [Fact]
    public async Task Cold_history_starts_empty_without_failing()
    {
        using var detector = new AmountSpikeDetector(
            Path.Combine(AppContext.BaseDirectory, "model.zip")
        );

        var result = await detector.InspectAsync(
            "ACCT-COLD",
            "CAD",
            2_500,
            _ => Task.FromResult<IReadOnlyList<long>>([]),
            CancellationToken.None
        );

        Assert.Equal(0, result.HistoryCount);
        Assert.True(double.IsFinite(result.RawScore));
        Assert.True(double.IsFinite(result.PValue));
    }

    [Fact]
    public void Amount_transform_matches_the_shared_golden_values()
    {
        // Oracle: math.log1p-equivalent values from an independent implementation.
        Assert.Equal(0.0f, AmountFeatures.FromAmountMinor(0));
        Assert.Equal(7.8244459f, AmountFeatures.FromAmountMinor(2_500), precision: 5);
        Assert.Equal(12.899222f, AmountFeatures.FromAmountMinor(400_000), precision: 5);
    }

    [Fact]
    public void Amount_transform_stays_finite_and_monotonic_over_the_money_range()
    {
        var last = float.NegativeInfinity;
        foreach (var amount in new long[] { 1, 2_500, 250_000, 1_000_000, 100_000_000_000L })
        {
            var feature = AmountFeatures.FromAmountMinor(amount);
            Assert.True(float.IsFinite(feature));
            Assert.True(feature > last);
            last = feature;
        }
    }
}
