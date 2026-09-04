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
    public async Task Identical_live_retry_returns_accepted_without_duplicating()
    {
        await using var connection = await OpenConnectionAsync();
        await using var app = await CreateAppAsync(connection);
        using var client = CreateClient(app);
        var submission = new TransactionSubmission(
            Guid.CreateVersion7().ToString("D"),
            "ACCT-live",
            3,
            TxMonitoring.Shared.TransactionTime.TruncateToMilliseconds(DateTimeOffset.UtcNow),
            2_500,
            "CAD"
        );

        var first = await client.PostAsJsonAsync("/api/local/transactions", submission);
        var retry = await client.PostAsJsonAsync("/api/local/transactions", submission);

        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, retry.StatusCode);
        await using var verificationScope = app.Services.CreateAsyncScope();
        var verificationDb = verificationScope.ServiceProvider.GetRequiredService<EdgeDbContext>();
        Assert.Equal(1, await verificationDb.BufferedTransactions.CountAsync());
        Assert.Equal(1, app.Services.GetRequiredService<BufferDepth>().Current);
    }

    [Fact]
    public async Task Conflicting_id_reuse_returns_conflict_and_preserves_the_original()
    {
        await using var connection = await OpenConnectionAsync();
        await using var app = await CreateAppAsync(connection);
        using var client = CreateClient(app);
        var messageId = Guid.CreateVersion7().ToString("D");
        var occurredAt = TxMonitoring.Shared.TransactionTime.TruncateToMilliseconds(
            DateTimeOffset.UtcNow
        );

        var first = await client.PostAsJsonAsync(
            "/api/local/transactions",
            new TransactionSubmission(messageId, "ACCT-orig", 3, occurredAt, 2_500, "CAD")
        );
        var conflict = await client.PostAsJsonAsync(
            "/api/local/transactions",
            new TransactionSubmission(messageId, "ACCT-orig", 3, occurredAt, 9_999, "CAD")
        );

        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        await using var verificationScope = app.Services.CreateAsyncScope();
        var verificationDb = verificationScope.ServiceProvider.GetRequiredService<EdgeDbContext>();
        var stored = await verificationDb.BufferedTransactions.SingleAsync();
        Assert.Equal(2_500, stored.AmountMinor);
    }

    [Fact]
    public async Task Quarantined_retry_returns_a_stable_terminal_rejection()
    {
        await using var connection = await OpenConnectionAsync();
        await using var app = await CreateAppAsync(connection);
        var messageId = Guid.CreateVersion7().ToString("D");
        var occurredAt = TxMonitoring.Shared.TransactionTime.TruncateToMilliseconds(
            DateTimeOffset.UtcNow.AddMinutes(-2)
        );

        await using (var scope = app.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<EdgeDbContext>();
            db.QuarantinedTransactions.Add(
                new QuarantinedTransaction
                {
                    MessageId = messageId,
                    PayloadFingerprint =
                        TxMonitoring.Shared.TransactionFingerprint.ComputeV1(
                            "ACCT-quarantined",
                            11,
                            occurredAt,
                            2_500,
                            "CAD"
                        ),
                    FingerprintVersion = TxMonitoring.Shared.TransactionFingerprint.Version,
                    OriginalQueueId = 7,
                    AccountId = "ACCT-quarantined",
                    SequenceNumber = 11,
                    OccurredAt = occurredAt,
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
                occurredAt,
                2_500,
                "CAD"
            )
        );

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
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
