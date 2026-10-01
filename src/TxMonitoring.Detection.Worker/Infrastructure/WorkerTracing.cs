using System.Diagnostics;

namespace TxMonitoring.Detection.Worker.Infrastructure;

/// <summary>
/// The worker's ActivitySource, the .NET counterpart of an OTel tracer.
/// </summary>
/// <remarks>
/// Kafka auto-instrumentation is still pre-release, so the consumer starts its own spans and
/// links them to the traceparent carried in the message headers.
/// </remarks>
public static class WorkerTracing
{
    public const string SourceName = "TxMonitoring.Detection.Worker";
    public static readonly ActivitySource Source = new(SourceName);
}
