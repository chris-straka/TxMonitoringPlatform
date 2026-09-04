namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// A recently completed MessageId retained after its live queue row is removed.
/// </summary>
/// <remarks>
/// The live queue's unique index catches retries only while a reading is buffered. This marker
/// also catches a delayed retry after the cloud has acknowledged the reading and the uploader has
/// deleted it. Markers expire because this is an edge buffer, not an unlimited event ledger.
/// </remarks>
public sealed class SettledMessage
{
    public string MessageId { get; set; } = string.Empty;
    public DateTimeOffset SettledAt { get; set; }

    // Fingerprint of the settled content: a delayed retry with the same ID but different
    // content is a conflict even after the live row is gone.
    public byte[] PayloadFingerprint { get; set; } = [];
    public int FingerprintVersion { get; set; }
}
