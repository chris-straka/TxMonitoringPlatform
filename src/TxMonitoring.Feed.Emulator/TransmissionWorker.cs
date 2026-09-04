using System.Diagnostics;
using System.Net.Http.Json;
using System.Threading.Channels;

using TxMonitoring.Feed.Emulator.Configuration;
using TxMonitoring.Feed.Emulator.Infrastructure;

namespace TxMonitoring.Feed.Emulator;

/// <summary>
/// LOOP 2: Drains the buffer (channel) to send to the gateway.
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
                // CreateClient() returns a new HttpClient but not a new handler chain.
                // The factory pools chains for GatewayClient.Name that the client uses.
                // It then expires each chain after HandlerLifetime (2 mins by default).
                // This lets it detect DNS changes every 2 mins (DNS is only stale for 2 mins)
                var client = httpClientFactory.CreateClient(GatewayClient.Name);

                // The stoppingToken will abort the HTTP call on shutdown
                // But that only stops the client from waiting for a response
                // This means the result on the server is ambiguous (EffO protects)
                using var response = await client.PostAsJsonAsync(
                    GatewayClient.Route,
                    data,
                    stoppingToken
                );

                // Only the gateway's 202 carries the durable-ownership promise. Treat an
                // unexpected 2xx from a proxy or misrouted endpoint as a dropped delivery rather
                // than quietly widening the contract to every successful-looking response.
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
                    // BACKPRESSURE -> consumer telling producer to slow down
                    // Polly reads/respects the consumer's Retry-After response header
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
                // Two different paths land here (circuit breaker related)
                // 1. Polly exhausted its retries (timeout, 5xx, conn reset)
                // 2. The circuit is open from earlier failures (didn't even try)
                metrics.Failed.Add(1);
                metrics.DeliveryDropped.Add(1);
                logger.LogError(
                    "[DELIVERY DROPPED] Gateway did not accept the transaction after bounded retries: {Message}",
                    ex.Message
                );
                // If 2. it will eventually send a half-open PROBE, not a retry to close the circuit
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
