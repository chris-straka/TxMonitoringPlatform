namespace TxMonitoring.Detection.Worker.Features.Diagnostics;

/// <summary>
/// The two terminal answers to "this MessageId is already stored".
/// Duplicate replays are no-ops; conflicting immutable content under one
/// MessageId is routed to the dead-letter queue, never overwritten and
/// never silently merged.
/// </summary>
public enum RedeliveryOutcome
{
    Duplicate,
    Conflict,
}

/// <summary>
/// The single implementation of the redelivery decision table, shared by
/// the pre-insert check and the unique-violation race handler: equal
/// fingerprints mean the replay carries identical immutable content.
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
