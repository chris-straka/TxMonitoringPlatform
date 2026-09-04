using Confluent.Kafka;

using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;

using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

using TxMonitoring.Detection.Worker.Configuration;
using TxMonitoring.Detection.Worker.Features.Diagnostics;
using TxMonitoring.Detection.Worker.Features.Diagnostics.ML;
using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Detection.Worker.Infrastructure;
using TxMonitoring.Detection.Worker.Infrastructure.Data;
using TxMonitoring.Shared;

var builder = WebApplication.CreateBuilder(args);

#region config
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
    .Services.AddOptions<ConsumerOptions>()
    .Bind(builder.Configuration.GetSection(ConsumerOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<OutboxOptions>()
    .Bind(builder.Configuration.GetSection(OutboxOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<RiskPolicyOptions>()
    .Bind(builder.Configuration.GetSection(RiskPolicyOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
builder
    .Services.AddOptions<VelocityOptions>()
    .Bind(builder.Configuration.GetSection(VelocityOptions.Section))
    .ValidateDataAnnotations()
    .ValidateOnStart();
// Needed during registration, before the sp container exists.
var otel = builder.Configuration.GetSection(OTelOptions.Section).Get<OTelOptions>()!;
var kafka = builder.Configuration.GetSection(KafkaOptions.Section).Get<KafkaOptions>()!;
var pgConnectionString = builder.Configuration.GetConnectionString("TxMonitoringDb");
ArgumentException.ThrowIfNullOrWhiteSpace(pgConnectionString);
#endregion

// Setup DB
builder.Services.AddPooledDbContextFactory<AppDbContext>(options =>
    options.UseNpgsql(pgConnectionString)
);

builder
    .Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(otel.ServiceName))
    .WithLogging(logging =>
        logging
            .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(otel.ServiceName))
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    )
    .WithMetrics(metrics =>
        metrics
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddMeter(WorkerMetrics.MeterName)
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    )
    .WithTracing(tracing =>
        tracing
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddEntityFrameworkCoreInstrumentation()
            .AddSource(WorkerTracing.SourceName)
            .AddOtlpExporter(opt => opt.Endpoint = new Uri(otel.Endpoint))
    );

builder.Services.AddSingleton<WorkerMetrics>();
builder.Services.AddSingleton<DetectionConsumerReadiness>();

// Kafka
builder.Services.AddSingleton(sp =>
{
    var logger = sp.GetRequiredService<ILogger<IProducer<string, string>>>();
    var config = new ProducerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageTimeoutMs = 20_000,
        AllowAutoCreateTopics = false,
        MetadataMaxAgeMs = 5000,
    };
    return new ProducerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => logger.LogError("Kafka Producer Error: {Reason}", e.Reason))
        .Build();
});
builder.Services.AddSingleton<IDeadLetterTransport, KafkaDeadLetterTransport>();
builder.Services.AddSingleton<IDeadLetterPublisher, DeadLetterPublisher>();
builder.Services.AddSingleton<IAdminClient>(
    new AdminClientBuilder(
        new AdminClientConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            AllowAutoCreateTopics = false,
        }
    ).Build()
);
builder
    .Services.AddHealthChecks()
    .AddCheck<DetectionConsumerReadiness>(
        "kafka_assignment",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready"]
    )
    .AddCheck<DetectionKafkaReadinessCheck>(
        "kafka_topics",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready"],
        timeout: TimeSpan.FromSeconds(KafkaOptions.MaximumReadinessTimeoutSeconds + 1)
    )
    .AddCheck<DetectionDatabaseReadinessCheck>(
        "postgres_migrations",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["ready"]
    );

// The online per-account spike detector artifact, built for the amount feature.
builder.Services.AddSingleton(
    new AmountSpikeDetector(Path.Combine(AppContext.BaseDirectory, "model.zip"))
);

// One clock for the process; unit tests substitute a manual TimeProvider.
builder.Services.AddSingleton(TimeProvider.System);

// Sliding-window velocity over Redis. The store degrades internally when Redis is
// unreachable, so ingestion and Postgres never depend on it being up.
builder.Services.AddSingleton<RedisConnection>();
builder.Services.AddSingleton<ITransactionHistoryRepository, TransactionHistoryRepository>();
builder.Services.AddSingleton<IVelocityStore, RedisVelocityStore>();

// The warmup runs before the consumer so windows are pre-materialized; it exits after
// one pass and sets no rebuilt flags, leaving per-key completion to the lazy path.
builder.Services.AddHostedService<VelocityWarmupWorker>();

// A hosted service, so the consume loop follows the app lifecycle without blocking it.
builder.Services.AddHostedService<TransactionConsumerWorker>();
builder.Services.AddHostedService<AlertOutboxPublisherWorker>();

var app = builder.Build();

// Migrate on startup for dev
if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    var factory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<AppDbContext>>();
    const int maxRetries = 5;
    var retryCount = 0;

    while (retryCount < maxRetries)
    {
        try
        {
            using var db = await factory.CreateDbContextAsync();
            await db.Database.MigrateAsync();
            app.Logger.LogInformation("DB Migrations Applied");
            break;
        }
        catch (Exception ex)
        {
            retryCount++;
            app.Logger.LogWarning(
                "⏳ DB Migration Retry {Count}/5: {Error}",
                retryCount,
                ex.Message
            );
            if (retryCount == 5)
                throw;
            await Task.Delay(2000);
        }
    }
}

app.MapGet("/health", () => Results.Ok());
app.MapHealthChecks(
    "/health/ready",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("ready"),
    }
);

app.Run();
