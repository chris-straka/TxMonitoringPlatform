using System.Diagnostics;
using System.Net.Http.Json;
using System.Threading.Channels;

using TxMonitoring.Feed.Emulator.Configuration;
using TxMonitoring.Feed.Emulator.Infrastructure;

namespace TxMonitoring.Feed.Emulator;

/// <summary>
/// Drains the channel and posts each transaction to the gateway.
/// </summary>
public class TransmissionWorker(
    Channel<TransactionSubmission> channel,
    IHttpClientFactory httpClientFactory,
    FeedMetrics metrics,
    ILogger<TransmissionWorker> logger
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Transmission started.");

        await foreach (var data in channel.Reader.ReadAllAsync(stoppingToken))
        {
            var startedAt = Stopwatch.GetTimestamp();
            var outcome = "cancelled";

            try
            {
                // CreateClient reuses a pooled handler chain. The factory rotates chains after
                // HandlerLifetime (2 minutes by default), so DNS changes are picked up.
                var client = httpClientFactory.CreateClient(GatewayClient.Name);

                // Cancelling on shutdown only stops waiting for the response. The gateway may
                // still have stored the transaction; MessageId idempotency covers the resend.
                using var response = await client.PostAsJsonAsync(
                    GatewayClient.Route,
                    data,
                    stoppingToken
                );

                // Only the gateway's 202 means it durably owns the transaction. Any other 2xx,
                // e.g. from a proxy or a misrouted endpoint, counts as a dropped delivery.
                if (response.StatusCode == System.Net.HttpStatusCode.Accepted)
                {
                    outcome = "accepted";
                    metrics.Sent.Add(1);
                    logger.LogInformation(
                        "Sent {AccountId} #{Seq}: {Amount} {Currency}",
                        data.AccountId,
                        data.SequenceNumber,
                        data.AmountMinor,
                        data.Currency
                    );
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    outcome = "backpressure";
                    // The resilience handler already honoured Retry-After on its retries.
                    metrics.Rejected.Add(1);
                    metrics.DeliveryDropped.Add(1);
                    logger.LogWarning(
                        "[BACKPRESSURE] Gateway buffer stayed full after bounded retries; dropping transaction. Retry-After was {RetryAfter}",
                        response.Headers.RetryAfter?.ToString() ?? "unspecified"
                    );
                }
                else
                {
                    outcome = response.IsSuccessStatusCode ? "unexpected_success" : "rejected";
                    metrics.Rejected.Add(1);
                    metrics.DeliveryDropped.Add(1);
                    var error = await response.Content.ReadAsStringAsync(stoppingToken);
                    logger.LogWarning(
                        "[REJECTED] Gateway refused {AccountId}; dropping transaction: {Error}",
                        data.AccountId,
                        error
                    );
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                outcome = "failed";
                // Either the retries were exhausted or the circuit was already open and no call
                // was made. An open circuit later lets one half-open probe through.
                metrics.Failed.Add(1);
                metrics.DeliveryDropped.Add(1);
                logger.LogError(
                    "[DELIVERY DROPPED] Gateway did not accept the transaction after bounded retries: {Message}",
                    ex.Message
                );
            }
            finally
            {
                metrics.RecordDeliveryDuration(
                    Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds,
                    outcome
                );
            }
        }
    }
}
