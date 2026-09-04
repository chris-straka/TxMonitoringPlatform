using System.Diagnostics.Metrics;

namespace TxMonitoring.Detection.Worker.Infrastructure;

/// <summary>
/// The worker's Otel's instruments.
/// </summary>
public sealed class WorkerMetrics : IDisposable
{
    public const string MeterName = "TxMonitoring.Detection.Worker";

    private readonly Meter _meter;
    private readonly Counter<long> _duplicates;
    private readonly Counter<long> _ruleFired;
    private readonly Histogram<double> _detectorLatency;
    private long _pendingAlertOutbox;

    public WorkerMetrics()
    {
        _meter = new Meter(MeterName);

        Persisted = _meter.CreateCounter<long>(
            "detection.transaction.persisted",
            unit: "{transaction}",
            description: "Transactions durably written to Postgres."
        );

        Consumed = _meter.CreateCounter<long>(
            "detection.transaction.consumed",
            unit: "{message}",
            description: "Kafka records returned to the detection consumer."
        );

        ProcessingFailures = _meter.CreateCounter<long>(
            "detection.transaction.processing_failures",
            unit: "{attempt}",
            description: "Processing attempts that failed and were rewound for retry."
        );

        DeadLettersPublished = _meter.CreateCounter<long>(
            "detection.transaction.dead_letter_published",
            unit: "{message}",
            description: "Poison Kafka records durably acknowledged by the dead-letter topic."
        );

        DeadLetterFailures = _meter.CreateCounter<long>(
            "detection.transaction.dead_letter_failures",
            unit: "{attempt}",
            description: "Dead-letter writes that failed or had an ambiguous Kafka result."
        );

        ReviewsRecommended = _meter.CreateCounter<long>(
            "detection.reviews.recommended",
            unit: "{transaction}",
            description: "Transactions the policy scored at or above the review threshold."
        );

        Evaluated = _meter.CreateCounter<long>(
            "detection.policy.evaluated",
            unit: "{decision}",
            description: "Policy evaluations completed, whether or not they recommended review."
        );

        _ruleFired = _meter.CreateCounter<long>(
            "detection.policy.rule_fired",
            unit: "{rule}",
            description: "Policy rules fired, tagged by rule code."
        );

        ReviewRecommended = _meter.CreateCounter<long>(
            "detection.policy.review_recommended",
            unit: "{decision}",
            description: "Policy decisions at or above the review threshold."
        );

        DetectorWarms = _meter.CreateCounter<long>(
            "detection.detector.warm",
            unit: "{observation}",
            description: "Detector inspections with a fully warmed per-account history."
        );

        _detectorLatency = _meter.CreateHistogram<double>(
            "detection.detector.latency",
            unit: "ms",
            description: "Per-observation detector inspection time, including history warm."
        );

        VelocityUnavailable = _meter.CreateCounter<long>(
            "detection.velocity.unavailable",
            unit: "{observation}",
            description: "Velocity observations that degraded to Unavailable instead of failing the message."
        );

        VelocityObserved = _meter.CreateCounter<long>(
            "detection.velocity.observed",
            unit: "{observation}",
            description: "Velocity observations served from Redis windows."
        );

        VelocityOutOfOrder = _meter.CreateCounter<long>(
            "detection.velocity.out_of_order",
            unit: "{observation}",
            description: "Events older than the window watermark, never added to the index."
        );

        VelocityRebuilding = _meter.CreateCounter<long>(
            "detection.velocity.rebuilding",
            unit: "{observation}",
            description: "Events observed while a cold window rebuilds from Postgres."
        );

        RebuildRows = _meter.CreateCounter<long>(
            "detection.velocity.rebuild_rows",
            unit: "{row}",
            description: "Postgres rows materialized into cold velocity windows."
        );

        RebuildDuration = _meter.CreateHistogram<double>(
            "detection.velocity.rebuild_ms",
            unit: "ms",
            description: "Time spent rebuilding one cold velocity window."
        );

        AlertFailures = _meter.CreateCounter<long>(
            "worker.alert.failures",
            unit: "{attempt}",
            description: "Outbox publication attempts that failed; the alert remains pending."
        );

        AlertsPublished = _meter.CreateCounter<long>(
            "worker.alert.published",
            unit: "{alert}",
            description: "Outbox alerts acknowledged by Kafka and marked published in Postgres."
        );

        _meter.CreateObservableGauge(
            "worker.alert.outbox.pending",
            () => Interlocked.Read(ref _pendingAlertOutbox),
            unit: "{alert}",
            description: "Current unpublished alert rows in the shared Postgres outbox."
        );

        _duplicates = _meter.CreateCounter<long>(
            "detection.transaction.duplicate",
            unit: "{transaction}",
            description: "Transactions skipped because the MessageId was already persisted."
        );

        // Distribution of per-transaction generator to DB times (bucketed)
        Lag = _meter.CreateHistogram<double>(
            "detection.transaction.lag_seconds",
            unit: "s",
            description: "Seconds between the generator stamping a transaction and this worker persisting it."
        );
    }

    public Counter<long> Persisted { get; }
    public Counter<long> Consumed { get; }
    public Counter<long> ProcessingFailures { get; }
    public Counter<long> DeadLettersPublished { get; }
    public Counter<long> DeadLetterFailures { get; }
    public Counter<long> ReviewsRecommended { get; }
    public Counter<long> Evaluated { get; }
    public Counter<long> ReviewRecommended { get; }
    public Counter<long> DetectorWarms { get; }
    public Counter<long> AlertFailures { get; }
    public Counter<long> VelocityUnavailable { get; }
    public Counter<long> VelocityObserved { get; }
    public Counter<long> VelocityOutOfOrder { get; }
    public Counter<long> VelocityRebuilding { get; }
    public Counter<long> RebuildRows { get; }
    public Histogram<double> RebuildDuration { get; }
    public Counter<long> AlertsPublished { get; }
    public Histogram<double> Lag { get; }

    // A method rather than a public counter, so every sample carries the tag
    public void RecordDuplicate(string detectedBy) =>
        _duplicates.Add(1, new KeyValuePair<string, object?>("detected_by", detectedBy));

    public void RecordRuleFired(string rule) =>
        _ruleFired.Add(1, new KeyValuePair<string, object?>("rule", rule));

    public void RecordDetectorLatency(double milliseconds, bool warm) =>
        _detectorLatency.Record(
            milliseconds,
            new KeyValuePair<string, object?>("warm", warm)
        );

    public void SetPendingAlertOutbox(long count) =>
        Interlocked.Exchange(ref _pendingAlertOutbox, count);

    public void Dispose() => _meter.Dispose();
}
