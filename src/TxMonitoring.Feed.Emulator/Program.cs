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

// Synthetic transaction load generator. It is best-effort and not a system of record.
// Acquisition does not wait for the network, a full channel drops the new item, and a dequeued
// item that never gets a 202 is discarded. Durable custody begins at the gateway's 202.

var builder = Host.CreateApplicationBuilder(args);

builder
    .Services.AddOptions<OTelOptions>()
    .Bind(builder.Configuration.GetSection(OTelOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
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

// Registration runs before the container is built, so read these values directly.
var otel = builder.Configuration.GetSection(OTelOptions.Section).Get<OTelOptions>()!;
var generator =
    builder.Configuration.GetSection(TransactionGeneratorOptions.Section)
        .Get<TransactionGeneratorOptions>()!;

builder
    .Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(otel.ServiceName))
    .WithLogging(log => log.AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint)))
    .WithMetrics(m =>
        m.AddMeter(FeedMetrics.MeterName)
            .AddHttpClientInstrumentation()
            // A starved thread pool stops the transmission loop from draining, which shows up
            // as dropped transactions. Runtime metrics make that cause visible.
            .AddRuntimeInstrumentation()
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    )
    .WithTracing(trace =>
        trace
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    );

// A named client, created per use from the factory. A typed client is transient and would be
// captured by the singleton worker, pinning one handler chain whose connections keep using the
// gateway's old IP after DNS changes.
builder
    .Services.AddHttpClient(
        GatewayClient.Name,
        (serviceProvider, client) =>
            client.BaseAddress = new Uri(
                serviceProvider.GetRequiredService<IOptions<GatewayOptions>>().Value.Url
            )
    )
    .AddStandardResilienceHandler();

// The standard resilience handler retries with jittered backoff behind a circuit breaker.
// Retries reduce loss during short gateway interruptions. Once the policy gives up, the item is
// discarded. A retry can follow a write that succeeded but lost its response, so the gateway
// dedupes on MessageId.

// An in-memory decoupling buffer, not a durable one. It keeps generation from waiting on a slow
// or unavailable gateway. FullMode.Wait never evicts an older item, and the generator uses
// TryWrite, so a full channel drops the new item instead of slowing generation.
builder.Services.AddSingleton(
    Channel.CreateBounded<TransactionSubmission>(
        new BoundedChannelOptions(generator.BufferCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false, // one writer per simulated account
        }
    )
);

builder.Services.AddSingleton<FeedMetrics>();

// Generation and transmission run independently and only meet at the channel.
builder.Services.AddHostedService<TransactionGeneratorWorker>();
builder.Services.AddHostedService<TransmissionWorker>();

var host = builder.Build();
host.Run();

public static class GatewayClient
{
    public const string Name = "gateway";
    public const string Route = "/api/local/transactions";
}
