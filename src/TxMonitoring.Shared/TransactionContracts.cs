using System.Text.Json;
using System.Text.Json.Serialization;

namespace TxMonitoring.Shared;

/// <summary>
/// Shared field bounds, enforced at gateway admission, ingestion, the Kafka consumer, and a
/// Postgres check constraint.
/// </summary>
public static class TransactionLimits
{
    /// <summary>
    /// Maximum attempted debit in minor units (1,000,000,000.00 CAD). Amounts are 64-bit
    /// integers end to end, and sums use checked arithmetic.
    /// </summary>
    public const long MaxAmountMinor = 100_000_000_000L;

    /// <summary>
    /// Maximum sequence number. 2^53 - 1 keeps the value exact in JavaScript, where the
    /// dashboard checks Number.isSafeInteger.
    /// </summary>
    public const long MaxSequenceNumber = 9_007_199_254_740_991L;

    /// <summary>Maximum records in one gateway-to-ingestion gRPC batch.</summary>
    public const int MaxBatchSize = 1_000;

    public const int MaxMessageIdLength = 36;
    public const int MaxAccountIdLength = 64;
    public const int MaxTraceParentLength = 128;
}

/// <summary>
/// The single accepted currency set. Other components read it rather than keeping their own
/// lists, so adding a currency is a coordinated contract, config, and policy change.
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
    /// Web defaults: camel-case names and case-insensitive reads. Sharing one record type per
    /// payload makes a renamed field a build error instead of a silent zero.
    /// </summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}

/// <summary>
/// The JSON value carried by the transaction-events Kafka topic (v1).
/// </summary>
/// <remarks>
/// Changing a field's name, type, or meaning needs a schema version decision and updated golden
/// fixtures. Deserialization binds the positional constructor, so a renamed member fails to
/// compile. ReceivedAt is stamped per publish attempt and is not part of payload identity.
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
