using System.Text.Json;
using System.Text.Json.Serialization;

namespace TxMonitoring.Shared;

/// <summary>
/// One named bound for money, enforced at gateway admission, ingestion, the
/// Kafka-consumer validator, and by a Postgres check constraint.
/// </summary>
public static class TransactionLimits
{
    /// <summary>
    /// Maximum attempted debit in minor units (1,000,000,000.00 CAD). Amounts are
    /// C# long / protobuf int64 / SQLite INTEGER / Postgres bigint end to end.
    /// Sums use checked integer arithmetic only.
    /// </summary>
    public const long MaxAmountMinor = 100_000_000_000L;

    /// <summary>
    /// Maximum sequence number. 2^53 - 1 keeps the JSON representation exact in
    /// JavaScript, where the dashboard validates Number.isSafeInteger.
    /// </summary>
    public const long MaxSequenceNumber = 9_007_199_254_740_991L;

    /// <summary>Maximum records in one gateway-to-ingestion gRPC batch.</summary>
    public const int MaxBatchSize = 1_000;

    public const int MaxMessageIdLength = 36;
    public const int MaxAccountIdLength = 64;
    public const int MaxTraceParentLength = 128;
}

/// <summary>
/// The accepted currency set lives here, once. Gateway, ingestion, detection,
/// the generator, tests, and policy must not carry independent lists; adding a
/// currency is a coordinated contract/config/policy change.
/// </summary>
public static class TransactionCurrencies
{
    public static readonly IReadOnlySet<string> Supported = new HashSet<string>(
        StringComparer.Ordinal
    ) { "CAD" };

    public static bool IsSupported(string? currency) =>
        currency is not null && Supported.Contains(currency);
}

/// <summary>One shared JSON shape for every Kafka payload.</summary>
public static class TransactionJson
{
    /// <summary>
    /// Web defaults: camel-case wire names, case-insensitive reads. A shared
    /// record type per payload means a renamed field breaks the build instead of
    /// silently deserializing a missing value as zero.
    /// </summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}

/// <summary>
/// The JSON value carried by the transaction-events Kafka topic (v1).
/// </summary>
/// <remarks>
/// Closed and versioned: changing a name, type, or meaning requires a schema
/// version decision and updated golden fixtures. Deserialization binds the
/// positional constructor, so a renamed C# member is a compiler error, not a
/// silent default. ReceivedAt is delivery metadata stamped once per Kafka
/// publication attempt; it is not part of payload identity.
/// </remarks>
public sealed record TransactionEvent(
    [property: JsonPropertyName("messageId")] string MessageId,
    [property: JsonPropertyName("accountId")] string AccountId,
    [property: JsonPropertyName("sequenceNumber")] long SequenceNumber,
    [property: JsonPropertyName("occurredAt")] DateTimeOffset OccurredAt,
    [property: JsonPropertyName("receivedAt")] DateTimeOffset ReceivedAt,
    [property: JsonPropertyName("amountMinor")] long AmountMinor,
    [property: JsonPropertyName("currency")] string Currency
)
{
    [JsonPropertyName("schemaVersion")]
    public int SchemaVersion { get; init; } = 1;
}
