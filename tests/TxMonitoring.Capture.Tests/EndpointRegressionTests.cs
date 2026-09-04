using System.Net;
using System.Net.Http.Json;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using TxMonitoring.Capture.Gateway.Configuration;
using TxMonitoring.Capture.Gateway.Features.Buffer;
using TxMonitoring.Capture.Gateway.Infrastructure;

namespace TxMonitoring.Capture.Tests;

public sealed class EndpointRegressionTests
{
    [Fact]
    public async Task Liveness_route_resolves_once_and_returns_ok()
    {
        await using var connection = await OpenConnectionAsync();
        await using var app = await CreateAppAsync(connection);
        using var client = CreateClient(app);

        var response = await client.GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Delayed_retry_of_quarantined_id_is_acknowledged_without_requeueing()
    {
        await using var connection = await OpenConnectionAsync();
        await using var app = await CreateAppAsync(connection);
        var messageId = Guid.CreateVersion7().ToString("D");

        await using (var scope = app.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<EdgeDbContext>();
            db.QuarantinedTransactions.Add(
                new QuarantinedTransaction
                {
                    MessageId = messageId,
                    OriginalQueueId = 7,
                    AccountId = "ACCT-quarantined",
                    SequenceNumber = 11,
                    OccurredAt = DateTimeOffset.UtcNow.AddMinutes(-2),
                    BufferedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                    AmountMinor = 2_500,
                    Currency = "CAD",
                    RejectedAt = DateTimeOffset.UtcNow,
                    RejectionCode = BufferSettlementStore.IngestionRejectionCode,
                    RejectionReason = BufferSettlementStore.IngestionRejectionReason,
                }
            );
            await db.SaveChangesAsync();
        }

        using var client = CreateClient(app);
        var response = await client.PostAsJsonAsync(
            "/api/local/transactions",
            new TransactionSubmission(
                messageId,
                "ACCT-quarantined",
                11,
                TxMonitoring.Shared.TransactionTime.TruncateToMilliseconds(
                    DateTimeOffset.UtcNow
                ),
                2_500,
                "CAD"
            )
        );

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        await using var verificationScope = app.Services.CreateAsyncScope();
        var verificationDb = verificationScope.ServiceProvider.GetRequiredService<EdgeDbContext>();
        Assert.Empty(await verificationDb.BufferedTransactions.ToListAsync());
        Assert.Equal(0, app.Services.GetRequiredService<BufferDepth>().Current);
    }

    private static async Task<WebApplication> CreateAppAsync(SqliteConnection connection)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddDbContext<EdgeDbContext>(options => options.UseSqlite(connection));
        builder.Services.AddSingleton<BufferDepth>();
        builder.Services.AddSingleton<BufferMutationGate>();
        builder.Services.AddSingleton<EdgeMetrics>();
        builder.Services.Configure<BufferOptions>(options =>
        {
            options.Path = "unused.db";
            options.MaxDepth = 100;
            options.SettledIdRetentionHours = 24;
            options.QuarantineRetentionHours = 168;
            options.QuarantineMaxRows = 100;
        });

        var app = builder.Build();
        app.MapReceiverEndpoints();
        app.MapGet("/health", () => Results.Ok());

        await using (var scope = app.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<EdgeDbContext>();
            await db.Database.EnsureCreatedAsync();
        }

        await app.StartAsync();
        return app;
    }

    private static HttpClient CreateClient(WebApplication app)
    {
        var server = app.Services.GetRequiredService<IServer>();
        var addresses = server.Features.Get<IServerAddressesFeature>();
        return new HttpClient { BaseAddress = new Uri(addresses!.Addresses.Single()) };
    }

    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }
}
