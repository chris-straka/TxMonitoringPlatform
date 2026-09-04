using System.Text.Json;

using TxMonitoring.Detection.Worker.Features.Diagnostics;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Tests;

public sealed class TransactionConsumerWorkerTests
{
    [Fact]
    public void Kafka_timestamp_offsets_are_normalized_before_Postgres_persistence()
    {
        var occurredAt = new DateTimeOffset(2026, 9, 3, 10, 0, 0, TimeSpan.FromHours(-6));
        var receivedAt = new DateTimeOffset(2026, 9, 3, 18, 0, 0, TimeSpan.FromHours(2));
        var @event = new TransactionEvent(
            Guid.CreateVersion7().ToString("D"),
            "ACCT-offset",
            1,
            occurredAt,
            receivedAt,
            2_500,
            "CAD"
        );

        var normalized = TransactionConsumerWorker.NormalizeTimestamps(@event);

        Assert.Equal(TimeSpan.Zero, normalized.OccurredAt.Offset);
        Assert.Equal(TimeSpan.Zero, normalized.ReceivedAt.Offset);
        Assert.Equal(occurredAt.UtcDateTime, normalized.OccurredAt.UtcDateTime);
        Assert.Equal(receivedAt.UtcDateTime, normalized.ReceivedAt.UtcDateTime);
    }

    [Fact]
    public void Golden_event_fixture_pins_version_names_casing_and_required_members()
    {
        const string golden = """
            {
              "schemaVersion": 1,
              "messageId": "0193f123-9a91-7e55-9c9c-0e3f2b1a4d5c",
              "accountId": "ACCT-0001",
              "sequenceNumber": 42,
              "occurredAt": "2026-09-03T10:00:00.123Z",
              "receivedAt": "2026-09-03T10:00:01.456Z",
              "amountMinor": 2500,
              "currency": "CAD"
            }
            """;

        var @event = JsonSerializer.Deserialize<TransactionEvent>(
            golden,
            TransactionJson.Options
        );

        Assert.NotNull(@event);
        Assert.Equal(1, @event.SchemaVersion);
        Assert.Equal("0193f123-9a91-7e55-9c9c-0e3f2b1a4d5c", @event.MessageId);
        Assert.Equal("ACCT-0001", @event.AccountId);
        Assert.Equal(42, @event.SequenceNumber);
        Assert.Equal(
            new DateTimeOffset(2026, 9, 3, 10, 0, 0, 123, TimeSpan.Zero),
            @event.OccurredAt
        );
        Assert.Equal(
            new DateTimeOffset(2026, 9, 3, 10, 0, 1, 456, TimeSpan.Zero),
            @event.ReceivedAt
        );
        Assert.Equal(2_500, @event.AmountMinor);
        Assert.Equal("CAD", @event.Currency);

        // The wire keeps camel-case names; a renamed member must break this, not drift.
        var reserialized = JsonSerializer.Serialize(@event, TransactionJson.Options);
        Assert.Contains("\"messageId\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"accountId\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"sequenceNumber\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"occurredAt\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"receivedAt\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"amountMinor\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"currency\"", reserialized, StringComparison.Ordinal);
        Assert.Contains("\"schemaVersion\"", reserialized, StringComparison.Ordinal);
    }
}
