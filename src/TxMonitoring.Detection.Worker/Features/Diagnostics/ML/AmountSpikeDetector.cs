using System.Collections.Concurrent;
using System.Security.Cryptography;

using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.Transforms.TimeSeries;

using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Worker.Features.Diagnostics.ML;

public class AmountObservation
{
    [LoadColumn(0)]
    public float AmountFeature { get; set; }
}

public class SpikePrediction
{
    // IID spike detection always returns [alert, raw score, p-value].
    [VectorType(3)]
    public double[] Prediction { get; set; } = default!;
}

/// <summary>
/// The online spike-detector outcome for one observation. The raw score is a detector
/// statistic, never a probability; <see cref="IsSpike"/> is one input to the risk policy,
/// which additionally requires a warm history before the spike rule may fire.
/// </summary>
public record AmountSpikeResult(
    bool IsSpike,
    double RawScore,
    double PValue,
    int HistoryCount,
    string DetectorVersion
);

/// <summary>
/// Owns one online IID detector state per (account, currency) identity.
/// </summary>
/// <remarks>
/// model.zip stores the detector configuration and schema; IID spike detection learns its
/// reference window online rather than fitting parameters from a training set. Mixing
/// accounts or currencies in one prediction engine makes one account's amounts become
/// another's history, so each key gets an isolated engine. A newly created state is warmed
/// from persisted transactions, which makes restarts and Kafka rebalances converge on the
/// same recent history. This is a pedagogical per-account baseline, not a fraud model:
/// real amounts are heavy-tailed, seasonal, and conditional, and IID ignores all of that.
/// </remarks>
public sealed class AmountSpikeDetector : IDisposable
{
    public const int HistoryLength = 20;
    private const int MaxAccountStates = 10_000;

    private readonly MLContext _mlContext = new();
    private readonly ITransformer _model;
    private readonly ConcurrentDictionary<(string AccountId, string Currency), DetectorState> _states =
        new();

    public AmountSpikeDetector(string modelPath)
    {
        DetectorVersion = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(modelPath)))
            .ToLowerInvariant();
        _model = _mlContext.Model.Load(modelPath, out _);
    }

    /// <summary>SHA-256 of the exact detector artifact loaded by this process.</summary>
    public string DetectorVersion { get; }

    public async Task<AmountSpikeResult> InspectAsync(
        string accountId,
        string currency,
        long amountMinor,
        Func<CancellationToken, Task<IReadOnlyList<long>>> loadHistory,
        CancellationToken cancellationToken
    )
    {
        var key = (accountId, currency);
        if (!_states.TryGetValue(key, out var state))
        {
            var history = await loadHistory(cancellationToken);
            var candidate = CreateState(history);
            state = _states.GetOrAdd(key, candidate);

            if (ReferenceEquals(state, candidate))
                TrimCacheIfNeeded(key);
        }

        lock (state.Gate)
        {
            state.LastUsed = Environment.TickCount64;
            var historyCount = state.ObservationCount;
            var prediction = state.Engine.Predict(
                new AmountObservation { AmountFeature = AmountFeatures.FromAmountMinor(amountMinor) }
            );
            if (state.ObservationCount < HistoryLength)
                state.ObservationCount++;

            return new AmountSpikeResult(
                prediction.Prediction[0] == 1,
                prediction.Prediction[1],
                prediction.Prediction[2],
                historyCount,
                DetectorVersion
            );
        }
    }

    // A failed database transaction must not leave the in-memory window one observation ahead
    // of durable state. The retry recreates this account's engine from Postgres before scoring.
    public void Invalidate(string accountId, string currency) =>
        _states.TryRemove((accountId, currency), out _);

    private DetectorState CreateState(IReadOnlyList<long> history)
    {
        var engine = _model.CreateTimeSeriesEngine<AmountObservation, SpikePrediction>(_mlContext);

        foreach (var amountMinor in history)
            engine.Predict(
                new AmountObservation { AmountFeature = AmountFeatures.FromAmountMinor(amountMinor) }
            );

        return new DetectorState(engine, Math.Min(history.Count, HistoryLength));
    }

    private void TrimCacheIfNeeded((string AccountId, string Currency) currentKey)
    {
        if (_states.Count <= MaxAccountStates)
            return;

        var oldest = _states
            .Where(pair => pair.Key != currentKey)
            .MinBy(pair => Volatile.Read(ref pair.Value.LastUsed));

        if (!string.IsNullOrEmpty(oldest.Key.AccountId))
            _states.TryRemove(oldest.Key, out _);
    }

    public void Dispose() => _states.Clear();

    private sealed class DetectorState(
        TimeSeriesPredictionEngine<AmountObservation, SpikePrediction> engine,
        int observationCount
    )
    {
        public object Gate { get; } = new();
        public TimeSeriesPredictionEngine<AmountObservation, SpikePrediction> Engine { get; } =
            engine;
        public int ObservationCount = observationCount;
        public long LastUsed = Environment.TickCount64;
    }
}
