// ImplicitUsings is hiding some packages
using System.Diagnostics;
using System.Threading.Channels;

using Microsoft.Extensions.Options;

using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using TxMonitoring.Feed.Emulator;
using TxMonitoring.Feed.Emulator.Configuration;
using TxMonitoring.Feed.Emulator.Infrastructure;
using TxMonitoring.Shared;

// Synthetic transaction load generator. It is deliberately best-effort and is not a
// system of record: acquisition never waits for the network, a full channel drops the
// new item, and a dequeued item without a gateway 202 is discarded. Durable custody
// begins only after the capture gateway commits the transaction and replies 202.

// Microsoft.Extensions.Hosting turns the console app into a Generic Host
// This brings in DI (builder.Services), Configuration (appsettings.json), Logging
var builder = Host.CreateApplicationBuilder(args);

// Options will grab a chunk from IConfiguration and create types + validate it.
// Linux env vars can't contain ':' but builder.Configuration will foo__bar -> foo:bar
// .Bind() uses reflection to get class members at runtime
// Binding "produces a T from a key-value config"
builder
    .Services.AddOptions<OTelOptions>()
    .Bind(builder.Configuration.GetSection(OTelOptions.Section))
    .ValidateDataAnnotations() // validate on use
    .ValidateOnStart(); // validate at start of runtime
builder
    .Services.AddOptions<GatewayOptions>()
    .Bind(builder.Configuration.GetSection(GatewayOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<TransactionGeneratorOptions>()
    .Bind(builder.Configuration.GetSection(TransactionGeneratorOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();

// DI is only available after builder.Build() and my Options above only run/validate after that
// To use them earlier (like to register services like Otel) I need this
var otel = builder.Configuration.GetSection(OTelOptions.Section).Get<OTelOptions>()!;
var generator =
    builder.Configuration.GetSection(TransactionGeneratorOptions.Section)
        .Get<TransactionGeneratorOptions>()!;

// Otel
// builder.Services is the IServiceCollection not the IServiceProvider (DI container)
builder
    .Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(otel.ServiceName))
    .WithLogging(log => log.AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint)))
    .WithMetrics(m =>
        m.AddMeter(FeedMetrics.MeterName)
            // Tracks outgoing HTTP metrics (the POST -> capture gateway)
            // This will tell us if the capture gateway is slow
            .AddHttpClientInstrumentation()
            // If the transmission loop can't get scheduled, it stops draining and drops transactions
            // This will emit thread-pool queue length, thread count, GC pause stats
            // This will tell us if the runtime is slow
            .AddRuntimeInstrumentation()
            // This is where we send our otel data every 60s
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    )
    .WithTracing(trace =>
        trace
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    );

// The DI container registers things by type and not by name (DI Keys are types)
// AddHttpClient<T> registers T as transient but AddHttpClient(NAME) registers nothing.
// Injecting a transient into a singleton would leave it trapped in the singleton's lifetime (app lifetime)

// HttpClient is a thin wrapper over an HttpMessageHandler chain (Polly on top, Sockets at the bottom).
// A captured client holds onto one chain forever, and DNS is only resolved when a new connection opens.
// So, the undisposed client keeps talking to the gateway's old IP indefinitely even if it changed.
builder
    .Services.AddHttpClient(
        GatewayClient.Name,
        (serviceProvider, client) =>
            client.BaseAddress = new Uri(
                // this grabs it from the DI container (no earlier .Get<T> necessary)
                serviceProvider.GetRequiredService<IOptions<GatewayOptions>>().Value.Url
            )
    )
    .AddStandardResilienceHandler(); // Adds Polly

// Polly adds retries with exponential backoff + jitter and a circuit breaker.
// Retries reduce loss during short gateway interruptions, but this generator is deliberately
// best-effort: after the bounded policy finishes, it discards the dequeued transaction. The durable
// contract begins only after the gateway replies 202. A retry happens when an
// outcome is ambiguous (the write may have worked even though the response was lost), so the
// gateway still needs MessageId idempotency.

// This is a decoupling buffer, not a durability buffer
// It prevents the generator from waiting on the network (if it's slow or down)
// The generator saves to RAM, while the gateway persists in storage
//
// A Channel is a thread-safe, async, in-memory queue
// FullMode decides what happens when it's full (Wait, DropOldest, DropNewest)
// FullMode.Wait means the channel will not evict an older transaction. Acquisition deliberately uses
// TryWrite rather than WriteAsync, so a full buffer drops the new item instead of slowing the
// simulated source loop.
builder.Services.AddSingleton(
    // Bounded to limit RAM usage
    Channel.CreateBounded<TransactionSubmission>(
        new BoundedChannelOptions(generator.BufferCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false, // one writer per simulated account
        }
    )
);

// Singleton because a meter and its instruments are process-wide state.
builder.Services.AddSingleton<FeedMetrics>();

// These two are registered as singletons (careful what you inject into them)
// Generates transactions, never waits for transmission
// Transmission reads, never waits on generation
builder.Services.AddHostedService<TransactionGeneratorWorker>();
builder.Services.AddHostedService<TransmissionWorker>();

var host = builder.Build();
host.Run(); // Blocks and listens for SIGTERM (Docker) or Ctrl+C

public static class GatewayClient
{
    public const string Name = "gateway";
    public const string Route = "/api/local/transactions";
}
