using System.Diagnostics.Metrics;

namespace TxMonitoring.Ingestion.Api.Infrastructure;

/// <summary>
/// The ingestion API's OpenTelemetry instruments.
/// </summary>
public sealed class IngestionMetrics : IDisposable
{
    public const string MeterName = "TxMonitoring.Ingestion.Api";

    private readonly Meter _meter;

    public IngestionMetrics()
    {
        _meter = new Meter(MeterName);

        Produced = _meter.CreateCounter<long>(
            "ingestion.transaction.produced",
            unit: "{transaction}",
            description: "Transactions the broker durably acknowledged."
        );

        Rejected = _meter.CreateCounter<long>(
            "ingestion.transaction.rejected",
            unit: "{transaction}",
            description: "Transactions refused by deterministic validation; Kafka never saw them."
        );

        Failed = _meter.CreateCounter<long>(
            "ingestion.transaction.failed",
            unit: "{transaction}",
            description: "Transactions left unnamed after transient, internal, or ambiguous Kafka failures; the gateway retains them."
        );
    }

    public Counter<long> Produced { get; }
    public Counter<long> Rejected { get; }
    public Counter<long> Failed { get; }

    public void Dispose() => _meter.Dispose();
}
