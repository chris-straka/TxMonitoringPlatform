using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;

using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using TxMonitoring.Capture.Gateway.Configuration;
using TxMonitoring.Capture.Gateway.Features.Buffer;
using TxMonitoring.Capture.Gateway.Features.Upload;
using TxMonitoring.Capture.Gateway.Infrastructure;
using TxMonitoring.Ingestion.V1; // generated from Protos/transactions.proto
using TxMonitoring.Shared;

// --------------------------------------------------------------------------------
// Sits between the feed generator and ingestion. After replying 202 it durably owns each
// valid transaction, even if ingestion, the network, or this process fails. The generator
// is best-effort up to that 202.
//
//   generator --HTTP--> [ receiver -> SQLite (durable) -> uploader ] --gRPC--> ingestion
//
// The receiver and uploader are independent loops coupled only by disk. The 202 is sent once
// the transaction is on local disk, so the feed keeps producing through an ingestion outage.
// --------------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);

// A valid transaction is a few hundred bytes. Bounding the body before JSON binding keeps padding
// or ignored properties from turning this unauthenticated endpoint into memory and disk pressure.
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 16 * 1024);

#region config
builder
    .Services.AddOptions<OTelOptions>()
    .Bind(builder.Configuration.GetSection(OTelOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<IngestionOptions>()
    .Bind(builder.Configuration.GetSection(IngestionOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<BufferOptions>()
    .Bind(builder.Configuration.GetSection(BufferOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<UploaderOptions>()
    .Bind(builder.Configuration.GetSection(UploaderOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<TransportSecurityOptions>()
    .Bind(builder.Configuration.GetSection(TransportSecurityOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();

// Read directly because registration runs before the service provider exists.
var otel = builder.Configuration.GetSection(OTelOptions.Section).Get<OTelOptions>()!;
var ingestion =
    builder.Configuration.GetSection(IngestionOptions.Section).Get<IngestionOptions>()!;
var buffer = builder.Configuration.GetSection(BufferOptions.Section).Get<BufferOptions>()!;
var transportSecurity =
    builder.Configuration.GetSection(TransportSecurityOptions.Section)
        .Get<TransportSecurityOptions>() ?? new TransportSecurityOptions();
#endregion

if (
    transportSecurity.Enabled
    && !ingestion.ApiUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
)
{
    throw new InvalidOperationException(
        "Ingestion:ApiUrl must use https:// when transport security is enabled."
    );
}

builder.Services.AddSingleton<SqlitePragmaInterceptor>();

builder.Services.AddDbContext<EdgeDbContext>(
    (sp, opt) =>
        opt.UseSqlite($"Data Source={buffer.Path}")
            .AddInterceptors(sp.GetRequiredService<SqlitePragmaInterceptor>())
);

builder.Services.AddSingleton<BufferDepth>();
builder.Services.AddSingleton<BufferMutationGate>();
builder.Services.AddSingleton<EdgeMetrics>();
builder.Services.AddHealthChecks().AddCheck<EdgeReadinessCheck>("sqlite_write");
builder.Services.AddScoped<BufferSettlementStore>();

builder
    .Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(otel.ServiceName))
    .WithLogging(log => log.AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint)))
    .WithMetrics(m =>
        m.AddMeter(EdgeMetrics.MeterName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation() // includes the gRPC client
            .AddRuntimeInstrumentation()
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    )
    .WithTracing(t =>
        t.AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            // The upload loop is background work with no automatic span, so it has its own source.
            .AddSource(EdgeTracing.SourceName)
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    );

var ingestionClient = builder.Services.AddGrpcClient<TransactionIngestion.TransactionIngestionClient>(o =>
{
    o.Address = new Uri(ingestion.ApiUrl);
});
if (transportSecurity.Enabled)
{
    ingestionClient.ConfigurePrimaryHttpMessageHandler(() =>
        MutualTlsHttpHandlerFactory.Create(transportSecurity)
    );
}

builder.Services.AddHostedService<UploaderWorker>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<EdgeDbContext>();
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(buffer.Path))!);

    // EnsureCreated rather than migrations because this is a transient queue. A breaking schema
    // change means draining the buffer first. Only Detection.Worker owns EF migrations.
    await db.Database.EnsureCreatedAsync();

    // EnsureCreated does not add new tables to an existing database. Adding them here keeps
    // already-buffered transactions across an upgrade.
    await EdgeDatabaseInitializer.EnsureCompatibleSchemaAsync(db, buffer);

    // WAL lets the uploader read while the receiver writes.
    await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;");

    var buffered = await db.BufferedTransactions.CountAsync();
    app.Services.GetRequiredService<BufferDepth>().Initialize(buffered);
    if (buffered > 0)
    {
        // Rows written by a previous run that were not yet settled.
        app.Logger.LogInformation(
            "Recovered {Count} unsent transactions from the local buffer at {Path}.",
            buffered,
            buffer.Path
        );
    }
}

app.MapReceiverEndpoints();
app.MapGet("/health", () => Results.Ok());
app.MapHealthChecks("/health/ready", new HealthCheckOptions());

app.Run();
