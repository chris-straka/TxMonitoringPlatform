using Microsoft.Extensions.Options;

using StackExchange.Redis;

using TxMonitoring.Detection.Worker.Features.RiskPolicy;

namespace TxMonitoring.Detection.Worker.Infrastructure;

/// <summary>
/// One lazy Redis connection shared by the velocity store and the startup warm worker.
/// A failed connect degrades to null (and retries on the next observation) instead of
/// failing startup: Redis is derived cache state, never the durable path.
/// </summary>
public sealed class RedisConnection(
    IOptions<VelocityOptions> options,
    ILogger<RedisConnection> logger
)
{
    private readonly object _gate = new();
    private Task<ConnectionMultiplexer?>? _connectionTask;

    public Task<ConnectionMultiplexer?> GetAsync()
    {
        lock (_gate)
        {
            _connectionTask ??= ConnectAsync();
            return _connectionTask;
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            if (_connectionTask is { IsCompletedSuccessfully: true })
                return;
            _connectionTask = null;
        }
    }

    private async Task<ConnectionMultiplexer?> ConnectAsync()
    {
        var velocity = options.Value;
        try
        {
            var configuration = ConfigurationOptions.Parse(velocity.Configuration);
            configuration.ConnectTimeout = velocity.ConnectTimeoutMs;
            configuration.SyncTimeout = velocity.SyncTimeoutMs;
            configuration.AbortOnConnectFail = false;
            var multiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
            logger.LogInformation(
                "Redis velocity connected to {Configuration}.",
                velocity.Configuration
            );
            return multiplexer;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Redis velocity unavailable; degrading to Unavailable.");
            Reset();
            return null;
        }
    }
}
