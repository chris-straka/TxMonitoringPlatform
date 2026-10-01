namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// A recently completed MessageId retained after its live queue row is removed.
/// </summary>
/// <remarks>
/// The live queue's unique index catches retries only while a transaction is buffered. This
/// marker also catches a delayed retry after the row was settled and deleted. Markers expire
/// because the buffer is not a permanent ledger.
/// </remarks>
public sealed class SettledMessage
{
    public string MessageId { get; set; } = string.Empty;
    public DateTimeOffset SettledAt { get; set; }

    // A delayed retry with the same ID but different content is still a conflict after the live
    // row is gone.
    public byte[] PayloadFingerprint { get; set; } = [];
    public int FingerprintVersion { get; set; }
}
