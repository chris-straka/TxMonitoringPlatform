using System.Diagnostics.Metrics;

using TxMonitoring.Capture.Gateway.Features.Buffer;

namespace TxMonitoring.Capture.Gateway.Infrastructure;

/// <summary>
/// The gateway's OpenTelemetry instruments.
/// </summary>
/// <remarks>
/// Buffer depth and oldest-transaction age make an ingestion outage visible: both climb during
/// one and drain after. Depth itself lives in <see cref="BufferDepth"/> because load shedding
/// depends on it, and admission should not depend on telemetry code.
/// </remarks>
public sealed class EdgeMetrics : IDisposable
{
    public const string MeterName = "TxMonitoring.Capture.Gateway";

    private readonly Meter _meter;
    private readonly Counter<long> _uploadFailures;
    private readonly Histogram<double> _uploadDuration;

    // NaN means nothing is buffered. A separate flag would need a second volatile read that
    // could disagree with this one.
    private double _oldestAgeSeconds = double.NaN;
    private int _ingestionReachable = 1;

    public EdgeMetrics(BufferDepth bufferDepth)
    {
        _meter = new Meter(MeterName);

        Received = _meter.CreateCounter<long>(
            "capture.transaction.received",
            unit: "{transaction}",
            description: "Transactions durably written to the local buffer."
        );

        Duplicates = _meter.CreateCounter<long>(
            "capture.transaction.duplicate",
            unit: "{transaction}",
            description: "Transactions acknowledged as duplicates because the MessageId was buffered, recently settled, or quarantined."
        );

        IdempotencyConflicts = _meter.CreateCounter<long>(
            "capture.transaction.idempotency_conflict",
            unit: "{transaction}",
            description: "Retries reusing a MessageId with different immutable content; the original is preserved."
        );

        Uploaded = _meter.CreateCounter<long>(
            "capture.transaction.uploaded",
            unit: "{transaction}",
            description: "Transactions ingestion durably acknowledged."
        );

        Shed = _meter.CreateCounter<long>(
            "capture.transaction.shed",
            unit: "{transaction}",
            description: "Transactions rejected with 429 because the local buffer hit its ceiling."
        );

        // A 400 never reaches capture.transaction.received, so without this counter a rejected
        // transaction is indistinguishable from one that was never sent.
        Malformed = _meter.CreateCounter<long>(
            "capture.transaction.malformed",
            unit: "{transaction}",
            description: "Transactions rejected with 400 because an identity, sequence, timestamp, amount, or currency was invalid."
        );

        Rejected = _meter.CreateCounter<long>(
            "capture.transaction.rejected",
            unit: "{transaction}",
            description: "Transactions dropped because ingestion named them as permanently refused."
        );

        // Tagged by outcome because an unreachable ingestion and a refused caller have different
        // fixes.
        _uploadFailures = _meter.CreateCounter<long>(
            "capture.upload.failures",
            unit: "{attempt}",
            description: "Upload attempts that ended without a full acknowledgement."
        );

        _uploadDuration = _meter.CreateHistogram<double>(
            "capture.upload.duration",
            unit: "ms",
            description: "Duration of one logical gRPC batch attempt, tagged by classified outcome."
        );

        _meter.CreateObservableGauge(
            "capture.transaction.buffer_depth",
            () => bufferDepth.Current,
            unit: "{transaction}",
            description: "Transactions sitting in the local buffer awaiting ingestion acknowledgement."
        );

        _meter.CreateObservableGauge(
            "capture.transaction.oldest_age",
            ObserveOldestTransactionAge,
            unit: "s",
            description: "Age of the oldest unacknowledged transaction. This is the real SLO."
        );

        // A gauge so it can be graphed beside buffer depth.
        _meter.CreateObservableGauge(
            "capture.ingestion.reachable",
            () => Volatile.Read(ref _ingestionReachable),
            description: "1 when the last upload attempt reached ingestion, 0 otherwise."
        );
    }

    public Counter<long> Received { get; }
    public Counter<long> Duplicates { get; }
    public Counter<long> IdempotencyConflicts { get; }
    public Counter<long> Uploaded { get; }
    public Counter<long> Rejected { get; }
    public Counter<long> Shed { get; }
    public Counter<long> Malformed { get; }

    // A method rather than a public counter so every sample carries the outcome tag.
    public void RecordUploadFailure(string outcome) =>
        _uploadFailures.Add(1, new KeyValuePair<string, object?>("outcome", outcome));

    public void RecordUploadDuration(double milliseconds, string outcome) =>
        _uploadDuration.Record(
            milliseconds,
            new KeyValuePair<string, object?>("outcome", outcome)
        );

    // Gauge callbacks read these on OTel's collection thread, so writes are volatile. Plain
    // stores are enough; nothing here needs an atomic read-modify-write.
    public void SetOldestTransactionAge(double seconds) =>
        Volatile.Write(ref _oldestAgeSeconds, seconds);

    // An empty buffer has no oldest transaction, which differs from one that is zero seconds old.
    public void NoTransactionsBuffered() => Volatile.Write(ref _oldestAgeSeconds, double.NaN);

    public void SetIngestionReachable(bool reachable) =>
        Volatile.Write(ref _ingestionReachable, reachable ? 1 : 0);

    // Yields nothing when empty, leaving a gap in the series. A zero would look like a
    // transaction that just arrived.
    private IEnumerable<Measurement<double>> ObserveOldestTransactionAge()
    {
        var seconds = Volatile.Read(ref _oldestAgeSeconds);

        if (!double.IsNaN(seconds))
            yield return new Measurement<double>(seconds);
    }

    public void Dispose() => _meter.Dispose();
}
