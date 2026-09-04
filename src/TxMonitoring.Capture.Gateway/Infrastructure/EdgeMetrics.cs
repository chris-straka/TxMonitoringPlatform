using System.Diagnostics.Metrics;

using TxMonitoring.Capture.Gateway.Features.Buffer;

namespace TxMonitoring.Capture.Gateway.Infrastructure;

/// <summary>
/// The gateway's OpenTelemetry instruments.
/// </summary>
/// <remarks>
/// A Counter only climbs and is read as a rate; a Gauge is a level that moves both ways
///
/// Depth and oldest-transaction-age are the gauges that make an outage visible
/// They climb during an outage and drain once there's no more outage
///
/// Queue depth is the one value this class does not own, because it also steers load shedding
/// I don't want application logic to depend on OTel implementation details
/// It lives in <see cref="BufferDepth"/>, so losing this file costs dashboards, never the ceiling
/// </remarks>
public sealed class EdgeMetrics : IDisposable
{
    public const string MeterName = "TxMonitoring.Capture.Gateway";

    private readonly Meter _meter;
    private readonly Counter<long> _uploadFailures;
    private readonly Histogram<double> _uploadDuration;

    // NaN means nothing is buffered, so there is no age to report
    // A companion bool would need two volatile reads that can disagree, and this needs one
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

        // A 400 never reaches capture.transaction.received, so without this a rejected
        // transaction looks like one the generator never sent
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

        // Tagged by outcome, because unreachable ingestion and ingestion refusing this caller
        // are one climbing line otherwise, and they need different people to fix them
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

        // Observable means OTel calls this at collection time instead of us pushing values
        // The depth stays owned by BufferDepth, so losing this file costs dashboards, never the ceiling
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

        // A gauge rather than a log line so it can be graphed beside the buffer depth
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

    // A method rather than a public counter, so every sample carries the tag
    // An untagged Add from somewhere else would land in the same metric with no outcome at all
    public void RecordUploadFailure(string outcome) =>
        _uploadFailures.Add(1, new KeyValuePair<string, object?>("outcome", outcome));

    public void RecordUploadDuration(double milliseconds, string outcome) =>
        _uploadDuration.Record(
            milliseconds,
            new KeyValuePair<string, object?>("outcome", outcome)
        );

    // Volatile because the gauge callbacks read these on OTel's collection thread
    // A plain write can sit in a register the reader never sees, so the dashboard freezes on a stale value
    // Not Interlocked, which buys indivisible read-modify-write that neither of these does (docs/Concurrency.md)
    public void SetOldestTransactionAge(double seconds) =>
        Volatile.Write(ref _oldestAgeSeconds, seconds);

    // An empty buffer has no oldest transaction, which is not the same as one that is zero seconds old
    public void NoTransactionsBuffered() => Volatile.Write(ref _oldestAgeSeconds, double.NaN);

    public void SetIngestionReachable(bool reachable) =>
        Volatile.Write(ref _ingestionReachable, reachable ? 1 : 0);

    // Yielding nothing leaves a gap in the series
    // Reporting a zero would draw the same flat line as a transaction that arrived this instant
    private IEnumerable<Measurement<double>> ObserveOldestTransactionAge()
    {
        var seconds = Volatile.Read(ref _oldestAgeSeconds);

        if (!double.IsNaN(seconds))
            yield return new Measurement<double>(seconds);
    }

    public void Dispose() => _meter.Dispose();
}
