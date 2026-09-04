using TxMonitoring.Shared;

namespace TxMonitoring.Capture.Tests;

/// <summary>
/// Pins the v1 fingerprint encoding. The expected hash was computed with an independent
/// implementation (Python hashlib over the spec'd byte layout), not with this code.
/// </summary>
public sealed class FingerprintVectorTests
{
    [Fact]
    public void Golden_vector_pins_the_v1_encoding()
    {
        var fingerprint = TransactionFingerprint.ComputeV1(
            "A",
            1,
            DateTimeOffset.UnixEpoch,
            1,
            "CAD"
        );

        Assert.Equal(
            "cc6711d6b584d8fd6c3218e4921b0beb21a5bc82caefbe04b4765f33318a015f",
            TransactionFingerprint.ToHex(fingerprint)
        );
    }

    [Fact]
    public void Equivalent_timestamp_offsets_hash_identically()
    {
        var instant = new DateTimeOffset(2026, 9, 3, 10, 0, 0, 123, TimeSpan.Zero);

        var utc = TransactionFingerprint.ComputeV1("ACCT-1", 7, instant, 2_500, "CAD");
        var offset = TransactionFingerprint.ComputeV1(
            "ACCT-1",
            7,
            instant.ToOffset(TimeSpan.FromHours(5)),
            2_500,
            "CAD"
        );

        Assert.Equal(utc, offset);
    }

    [Fact]
    public void Currency_is_canonicalized_before_hashing()
    {
        var upper = TransactionFingerprint.ComputeV1(
            "ACCT-1",
            7,
            DateTimeOffset.UnixEpoch,
            2_500,
            "CAD"
        );
        var lower = TransactionFingerprint.ComputeV1(
            "ACCT-1",
            7,
            DateTimeOffset.UnixEpoch,
            2_500,
            "cad"
        );

        Assert.Equal(upper, lower);
    }

    [Theory]
    [InlineData("ACCT-2", 7, 2_500, "CAD")] // account
    [InlineData("ACCT-1", 8, 2_500, "CAD")] // sequence
    [InlineData("ACCT-1", 7, 2_501, "CAD")] // amount
    [InlineData("ACCT-1", 7, 2_500, "USD")] // currency
    public void Any_immutable_field_change_conflicts(
        string accountId,
        long sequence,
        long amount,
        string currency
    )
    {
        var baseline = TransactionFingerprint.ComputeV1(
            "ACCT-1",
            7,
            DateTimeOffset.UnixEpoch,
            2_500,
            "CAD"
        );
        var changed = TransactionFingerprint.ComputeV1(
            accountId,
            sequence,
            DateTimeOffset.UnixEpoch,
            amount,
            currency
        );

        Assert.NotEqual(baseline, changed);
    }

    [Fact]
    public void Version_is_advertised_for_stored_rows()
    {
        Assert.Equal(1, TransactionFingerprint.Version);
    }
}
