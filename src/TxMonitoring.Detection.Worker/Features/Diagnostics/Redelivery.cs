namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// The two outcomes when a MessageId is already stored. A duplicate replay is a no-op.
/// Different content under the same MessageId goes to the dead-letter queue and is not merged.
/// </summary>
public enum RedeliveryOutcome
{
    Duplicate,
    Conflict,
}

/// <summary>
/// The redelivery decision, shared by the pre-insert check and the unique-violation handler.
/// Equal fingerprints mean the replay carries identical content.
/// </summary>
public static class RedeliveryClassifier
{
    public static RedeliveryOutcome Classify(
        byte[] storedFingerprint,
        byte[] incomingFingerprint
    ) =>
        storedFingerprint.SequenceEqual(incomingFingerprint)
            ? RedeliveryOutcome.Duplicate
            : RedeliveryOutcome.Conflict;
}
