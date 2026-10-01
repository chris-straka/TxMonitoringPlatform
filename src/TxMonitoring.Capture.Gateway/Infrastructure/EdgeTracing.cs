using System.Diagnostics;

namespace TxMonitoring.Capture.Gateway.Infrastructure;

/// <summary>
/// The gateway's ActivitySource, the .NET counterpart of an OTel tracer.
/// </summary>
/// <remarks>
/// HTTP client instrumentation sees only the gRPC call, not the read-and-upload pass, so the
/// uploader creates its batch span here. StartActivity returns null when nothing listens.
/// </remarks>
public static class EdgeTracing
{
    public const string SourceName = "TxMonitoring.Capture.Gateway";

    public static readonly ActivitySource Source = new(SourceName);
}
