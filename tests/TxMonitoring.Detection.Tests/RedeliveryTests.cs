using TxMonitoring.Detection.Worker.Features.Diagnostics;

namespace TxMonitoring.Detection.Tests;

public sealed class RedeliveryTests
{
    [Fact]
    public void Identical_fingerprints_are_duplicates()
    {
        var stored = new byte[] { 1, 2, 3, 4 };
        var incoming = new byte[] { 1, 2, 3, 4 };

        Assert.Equal(
            RedeliveryOutcome.Duplicate,
            RedeliveryClassifier.Classify(stored, incoming)
        );
    }

    [Fact]
    public void Differing_content_under_one_id_is_a_conflict()
    {
        var stored = new byte[] { 1, 2, 3, 4 };
        var incoming = new byte[] { 1, 2, 3, 5 };

        Assert.Equal(
            RedeliveryOutcome.Conflict,
            RedeliveryClassifier.Classify(stored, incoming)
        );
    }

    [Fact]
    public void Empty_against_non_empty_is_a_conflict()
    {
        Assert.Equal(
            RedeliveryOutcome.Conflict,
            RedeliveryClassifier.Classify([], [7])
        );
        Assert.Equal(
            RedeliveryOutcome.Conflict,
            RedeliveryClassifier.Classify([7], [])
        );
    }

    [Fact]
    public void Different_lengths_are_conflicts()
    {
        Assert.Equal(
            RedeliveryOutcome.Conflict,
            RedeliveryClassifier.Classify([1, 2, 3], [1, 2, 3, 4])
        );
    }

    [Fact]
    public void Full_sha256_fingerprints_round_trip()
    {
        using var sha = System.Security.Cryptography.SHA256.Create();
        var stored = sha.ComputeHash("immutable-content"u8.ToArray());
        var incoming = sha.ComputeHash("immutable-content"u8.ToArray());
        var tampered = sha.ComputeHash("immutable-contenT"u8.ToArray());

        Assert.Equal(
            RedeliveryOutcome.Duplicate,
            RedeliveryClassifier.Classify(stored, incoming)
        );
        Assert.Equal(
            RedeliveryOutcome.Conflict,
            RedeliveryClassifier.Classify(stored, tampered)
        );
    }
}
