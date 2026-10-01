using System.Text.Json;

using TxMonitoring.Detection.Worker.Features.RiskPolicy;
using TxMonitoring.Shared;

namespace TxMonitoring.Detection.Tests;

/// <summary>
/// Pins the v1 risk-alerts shape: version, casing, required members, and the string-encoded
/// velocity aggregates.
/// </summary>
public sealed class RiskAlertGoldenTests
{
    [Fact]
    public void Golden_alert_fixture_round_trips_with_exact_names()
    {
        const string golden = """
            {
              "schemaVersion": 1,
              "messageId": "0193f123-9a91-7e55-9c9c-0e3f2b1a4d5c",
              "accountId": "ACCT-0001",
              "sequenceNumber": 42,
              "occurredAt": "2026-09-03T10:00:00.123Z",
              "receivedAt": "2026-09-03T10:00:01.456Z",
              "detectedAt": "2026-09-03T10:00:02.789Z",
              "amountMinor": 1500000,
              "currency": "CAD",
              "riskScore": 70,
              "reviewRecommended": true,
              "reasons": [
                {
                  "code": "velocity.count_1m",
                  "metric": "velocity_count_1m",
                  "observedValue": "30",
                  "threshold": "20",
                  "points": 40
                },
                {
                  "code": "velocity.amount_5m",
                  "metric": "velocity_amount_5m_minor",
                  "observedValue": "1500000",
                  "threshold": "1000000",
                  "points": 40
                }
              ],
              "velocityStatus": "Available",
              "velocity": { "count1m": "30", "amount5mMinor": "1500000" },
              "detector": {
                "isSpike": false,
                "rawScore": 0.5,
                "pValue": 0.4,
                "historyCount": 25,
                "detectorVersion": "abc123"
              },
              "policyVersion": "risk-policy-v1",
              "policyConfigurationHash": "def456"
            }
            """;

        var alert = JsonSerializer.Deserialize<RiskAlert>(golden, TransactionJson.Options);

        Assert.NotNull(alert);
        Assert.Equal(1, alert.SchemaVersion);
        Assert.Equal("0193f123-9a91-7e55-9c9c-0e3f2b1a4d5c", alert.MessageId);
        Assert.Equal("ACCT-0001", alert.AccountId);
        Assert.Equal(42, alert.SequenceNumber);
        Assert.Equal(1_500_000, alert.AmountMinor);
        Assert.Equal("CAD", alert.Currency);
        Assert.Equal(70, alert.RiskScore);
        Assert.True(alert.ReviewRecommended);
        Assert.Equal(
            ["velocity.count_1m", "velocity.amount_5m"],
            alert.Reasons.Select(r => r.Code)
        );
        Assert.Equal(VelocityStatus.Available, alert.VelocityStatus);
        Assert.Equal("30", alert.Velocity!.Count1m);
        Assert.Equal("1500000", alert.Velocity.Amount5mMinor);
        Assert.False(alert.Detector.IsSpike);
        Assert.Equal(25, alert.Detector.HistoryCount);
        Assert.Equal("risk-policy-v1", alert.PolicyVersion);

        var reserialized = JsonSerializer.Serialize(alert, TransactionJson.Options);
        foreach (
            var name in new[]
            {
                "messageId",
                "accountId",
                "sequenceNumber",
                "occurredAt",
                "receivedAt",
                "detectedAt",
                "amountMinor",
                "currency",
                "riskScore",
                "reviewRecommended",
                "reasons",
                "velocityStatus",
                "velocity",
                "detector",
                "policyVersion",
                "policyConfigurationHash",
                "schemaVersion",
                "observedValue",
                "count1m",
                "amount5mMinor",
                "historyCount",
                "detectorVersion",
            }
        )
        {
            Assert.Contains($"\"{name}\"", reserialized, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Unavailable_velocity_serializes_null_evidence_not_zeros()
    {
        var alert = new RiskAlert(
            Guid.CreateVersion7().ToString("D"),
            "ACCT-0001",
            1,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow,
            2_500,
            "CAD",
            0,
            false,
            [],
            VelocityStatus.Unavailable,
            null,
            new DetectorEvidence(false, 0.5, 0.4, 25, "v"),
            "risk-policy-v1",
            "hash"
        );

        var json = JsonSerializer.Serialize(alert, TransactionJson.Options);

        Assert.Contains("\"velocity\":null", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"count1m\":\"0\"", json, StringComparison.Ordinal);
    }
}
