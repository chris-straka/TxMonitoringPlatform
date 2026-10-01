using TxMonitoring.Feed.Emulator;
using TxMonitoring.Feed.Emulator.Configuration;

namespace TxMonitoring.Feed.Tests;

public sealed class GeneratorScenarioTests
{
    [Theory]
    [InlineData(GeneratorScenario.Steady, 1_000, 4_999)]
    [InlineData(GeneratorScenario.HighAmount, 300_000, 499_999)]
    [InlineData(GeneratorScenario.Burst, 8_000, 11_999)]
    public void Scenario_amounts_stay_inside_documented_bounds(
        GeneratorScenario scenario,
        long min,
        long max
    )
    {
        for (var seq = 1; seq <= 50; seq++)
        {
            Assert.InRange(
                TransactionGeneratorWorker.AmountFor(scenario, seq, noise: 0.0),
                min,
                max
            );
            Assert.InRange(
                TransactionGeneratorWorker.AmountFor(scenario, seq, noise: 0.9999),
                min,
                max
            );
        }
    }

    [Fact]
    public void Steady_amounts_stay_far_below_the_absolute_rule_bound()
    {
        // The absolute-high threshold is 250_000 minor units and Steady tops out at 4_999.
        // RiskPolicyTests covers the full below-threshold case against the shipped config.
        for (var seq = 1; seq <= 200; seq++)
        {
            Assert.True(
                TransactionGeneratorWorker.AmountFor(GeneratorScenario.Steady, seq, 0.9999) < 250_000
            );
        }
    }

    [Fact]
    public void Account_spike_isolates_a_sub_absolute_detector_probe()
    {
        var probe = TransactionGeneratorWorker.AmountFor(GeneratorScenario.AccountSpike, 12, 0.9999);
        var baseline = TransactionGeneratorWorker.AmountFor(GeneratorScenario.AccountSpike, 11, 0.9999);

        Assert.True(probe < 250_000);
        Assert.True(probe > baseline * 4);
    }

    [Fact]
    public void Combined_scenario_fires_a_full_absolute_amount_every_tenth_event()
    {
        Assert.Equal(400_000, TransactionGeneratorWorker.AmountFor(GeneratorScenario.Combined, 10, 0.0));
        Assert.Equal(400_000, TransactionGeneratorWorker.AmountFor(GeneratorScenario.Combined, 100, 0.9999));
        Assert.True(
            TransactionGeneratorWorker.AmountFor(GeneratorScenario.Combined, 11, 0.9999) < 250_000
        );
    }

    [Theory]
    [InlineData(GeneratorScenario.Steady, 5, 5)]
    [InlineData(GeneratorScenario.HighAmount, 5, 5)]
    [InlineData(GeneratorScenario.AccountSpike, 5, 5)]
    [InlineData(GeneratorScenario.Burst, 5, 2)]
    [InlineData(GeneratorScenario.Combined, 5, 2)]
    [InlineData(GeneratorScenario.Burst, 1, 1)]
    public void Burst_scenarios_compress_slow_cadences_to_two_seconds(
        GeneratorScenario scenario,
        int configured,
        int expected
    )
    {
        Assert.Equal(
            expected,
            TransactionGeneratorWorker.EffectiveIntervalSeconds(configured, scenario)
        );
    }

    [Fact]
    public void Steady_cadence_stays_below_the_velocity_count_bound()
    {
        // 5s per account is 12 events/min, below the 20/min count rule.
        var perMinute = 60 / TransactionGeneratorWorker.EffectiveIntervalSeconds(
            5,
            GeneratorScenario.Steady
        );

        Assert.True(perMinute < 20);
    }

    [Fact]
    public void Unknown_scenario_is_a_programming_error()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            TransactionGeneratorWorker.AmountFor((GeneratorScenario)999, 1, 0.5)
        );
    }
}
