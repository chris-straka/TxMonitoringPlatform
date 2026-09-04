using System.Diagnostics;

namespace TxMonitoring.Web.Api.Infrastructure;

public static class WebTracing
{
    public const string SourceName = "TxMonitoring.Web.Api";

    public static readonly ActivitySource Source = new(SourceName);
}
