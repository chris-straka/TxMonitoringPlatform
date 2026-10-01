using Confluent.Kafka;

using FluentValidation;

using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using TxMonitoring.Ingestion.Api.Configuration;
using TxMonitoring.Ingestion.Api.Features.Ingestion;
using TxMonitoring.Ingestion.Api.Infrastructure;
using TxMonitoring.Shared;

// --------------------------------------------------------------------------
// The ingestion end of the store-and-forward path.
//
//   ┌─────────────────┐  bounded batch, one call ┌───────────────┐
//   │ capture-gateway │ ────── gRPC:8081 ──────► │ ingestion-api │
//   │                 │ ◄──── MessageIds ─────── │               │
//   └─────────────────┘                          └───────┬───────┘
//                         one message per transaction    │
//                                key = AccountId         ▼
//                                        ┌──────────────────────────┐
//                                        │ Kafka transaction-events │
//                                        └──────────────────────────┘
//
// Keying by AccountId keeps an account's transactions on one Kafka partition, in order.
// The reply sorts every ID the gateway sent into:
//
//   accepted   the broker acknowledged the write
//   rejected   the validator refused it, so it was not produced
//   neither    the gateway still owns it and will resend it
//
// Test scenarios also enter through the feed and gateway, so this is the only ingestion path.
// --------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);

// Validated at startup so a bad deployment fails fast.
builder
    .Services.AddOptions<OTelOptions>()
    .Bind(builder.Configuration.GetSection(OTelOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<TransportSecurityOptions>()
    .Bind(builder.Configuration.GetSection(TransportSecurityOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<GrpcOptions>()
    .Bind(builder.Configuration.GetSection(GrpcOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();

var otel = builder.Configuration.GetSection(OTelOptions.Section).Get<OTelOptions>()!;
var kafka = builder.Configuration.GetSection(KafkaOptions.Section).Get<KafkaOptions>()!;
var grpc = GrpcOptions.Read(builder.Configuration);
var transportSecurity =
    builder.Configuration.GetSection(TransportSecurityOptions.Section)
        .Get<TransportSecurityOptions>() ?? new TransportSecurityOptions();

if (transportSecurity.Enabled)
{
    var grpcEndpoint = builder.Configuration["Kestrel:Endpoints:Grpc:Url"];
    if (grpcEndpoint is null || !grpcEndpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException(
            "The gRPC Kestrel endpoint must use https:// when transport security is enabled."
        );
    }

    var clientCertificatePolicy = ClientCertificatePolicy.Load(
        transportSecurity.TrustedClientCaPath,
        transportSecurity.AllowedClientFingerprintsPath
    );
    builder.Services.AddSingleton(clientCertificatePolicy);
    builder.WebHost.ConfigureKestrel(options =>
        options.ConfigureHttpsDefaults(https =>
        {
            https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
            https.ClientCertificateValidation = (certificate, _, _) =>
                clientCertificatePolicy.IsAllowed(certificate);
        })
    );
}

builder
    .Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(otel.ServiceName))
    .WithLogging(logging => logging.AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint)))
    .WithMetrics(metrics =>
        metrics
            .AddMeter(IngestionMetrics.MeterName)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    )
    .WithTracing(tracing =>
        tracing
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    );

builder.Services.AddSingleton<IngestionMetrics>();

builder.Services.AddValidatorsFromAssemblyContaining<TransactionRecordValidator>();

// A gateway batch becomes one Kafka message per accepted transaction, keyed by AccountId with a
// JSON value. The producer then batches messages per partition on its own.
builder.Services.AddSingleton(sp =>
{
    var logger = sp.GetRequiredService<ILogger<IProducer<string, string>>>();

    var config = new ProducerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        LingerMs = 20,
        MessageTimeoutMs = 20_000,
        CompressionType = CompressionType.Zstd,
        // A topic typo should fail delivery and leave the gateway row retryable, rather than
        // create a topic no consumer reads.
        AllowAutoCreateTopics = false,
        // MetadataMaxAgeMs = 5000,
    };

    return new ProducerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => logger.LogError("Kafka Producer Error: {Reason}", e.Reason))
        .Build();
});

builder.Services.AddSingleton<IAdminClient>(sp =>
{
    var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new AdminClientBuilder(
        new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
            AllowAutoCreateTopics = false,
        }
    ).Build();
});
builder
    .Services.AddHealthChecks()
    .AddCheck<KafkaTopicHealthCheck>(
        "kafka-events-topic",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready"],
        timeout: TimeSpan.FromSeconds(KafkaOptions.MaximumReadinessTimeoutSeconds + 1)
    );

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = grpc.MaxReceiveMessageSizeBytes;
});
builder.Services.AddOpenApi();
var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapGrpcService<TransactionIngestionService>();
app.MapGet("/health", () => Results.Ok());
app.MapHealthChecks(
    "/health/ready",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("ready"),
    }
);

app.Run();
