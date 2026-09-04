using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace TxMonitoring.Shared;

/// <summary>
/// Identity for idempotent retries: MessageId selects the record, the fingerprint proves
/// the immutable origin content is identical. ReceivedAt, trace context, broker
/// coordinates, and all other delivery metadata are excluded, so a retry after a lost
/// acknowledgement hashes identically. Equivalent timestamp offsets are not different data.
/// </summary>
/// <remarks>
/// The hash detects accidental reuse; it is not authentication. Gateway absorption lasts
/// only as long as bounded settled/quarantine retention, not as a permanent global registry.
/// </remarks>
public static class TransactionFingerprint
{
    public const int Version = 1;

    /// <summary>
    /// v1 encoding, in fixed order: one version byte; AccountId as a four-byte big-endian
    /// length plus exact UTF-8 bytes; SequenceNumber as signed 64-bit big-endian;
    /// OccurredAt as signed 64-bit epoch milliseconds; AmountMinor as signed 64-bit
    /// big-endian; canonical uppercase Currency as length-prefixed UTF-8. SHA-256 over
    /// the concatenation. Never interpolation, delimiters, culture-sensitive formatting,
    /// JSON, or database-round-tripped timestamps.
    /// </summary>
    public static byte[] ComputeV1(
        string accountId,
        long sequenceNumber,
        DateTimeOffset occurredAt,
        long amountMinor,
        string currency
    )
    {
        var accountBytes = Encoding.UTF8.GetBytes(accountId);
        var currencyBytes = Encoding.UTF8.GetBytes(currency.ToUpperInvariant());
        // The instant, not the offset: equivalent source offsets hash identically.
        var epochMillis =
            (occurredAt.ToUniversalTime().Ticks - DateTime.UnixEpoch.Ticks)
            / TimeSpan.TicksPerMillisecond;

        var input = new byte[1 + 4 + accountBytes.Length + 8 + 8 + 8 + 4 + currencyBytes.Length];
        var span = input.AsSpan();
        span[0] = Version;
        BinaryPrimitives.WriteInt32BigEndian(span[1..], accountBytes.Length);
        accountBytes.CopyTo(span[5..]);
        var offset = 5 + accountBytes.Length;
        BinaryPrimitives.WriteInt64BigEndian(span[offset..], sequenceNumber);
        BinaryPrimitives.WriteInt64BigEndian(span[(offset + 8)..], epochMillis);
        BinaryPrimitives.WriteInt64BigEndian(span[(offset + 16)..], amountMinor);
        BinaryPrimitives.WriteInt32BigEndian(span[(offset + 24)..], currencyBytes.Length);
        currencyBytes.CopyTo(span[(offset + 28)..]);

        return SHA256.HashData(input);
    }

    public static string ToHex(byte[] fingerprint) =>
        Convert.ToHexString(fingerprint).ToLowerInvariant();
}
