using System.ComponentModel.DataAnnotations;

namespace TxMonitoring.Capture.Gateway.Configuration;

public class IngestionOptions
{
    public const string Section = "Ingestion";

    [Required(AllowEmptyStrings = false)]
    [Url]
    public string ApiUrl { get; set; } = string.Empty;
}

public sealed class TransportSecurityOptions : IValidatableObject
{
    public const string Section = "TransportSecurity";

    public bool Enabled { get; set; }

    public string ClientCertificatePath { get; set; } = string.Empty;

    // Inject a secret in real deployments. The Compose development certificate has an empty
    // password because its private key lives in an isolated volume.
    public string? ClientCertificatePassword { get; set; }

    public string TrustedServerCaPath { get; set; } = string.Empty;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (!Enabled)
            yield break;

        if (string.IsNullOrWhiteSpace(ClientCertificatePath))
        {
            yield return new ValidationResult(
                "TransportSecurity:ClientCertificatePath is required when mTLS is enabled.",
                [nameof(ClientCertificatePath)]
            );
        }

        if (string.IsNullOrWhiteSpace(TrustedServerCaPath))
        {
            yield return new ValidationResult(
                "TransportSecurity:TrustedServerCaPath is required when mTLS is enabled.",
                [nameof(TrustedServerCaPath)]
            );
        }
    }
}

public class BufferOptions
{
    public const string Section = "Buffer";

    [Required(AllowEmptyStrings = false)]
    public string Path { get; set; } = string.Empty;

    [Range(1, 100_000_000)]
    public int MaxDepth { get; set; }

    // A retry after this window is treated as a new submission. Generator retries span seconds,
    // so hours of retention covers them while keeping the database bounded.
    [Range(1, 8_760)]
    public int SettledIdRetentionHours { get; set; }

    // Rejected payloads are kept for inspection, bounded by age and row count.
    [Range(1, 8_760)]
    public int QuarantineRetentionHours { get; set; }

    [Range(1, 1_000_000)]
    public int QuarantineMaxRows { get; set; }
}

public class UploaderOptions
{
    public const string Section = "Uploader";

    // With the field bounds, 1,000 transactions stay well under gRPC's default 4 MiB message limit.
    [Range(1, 1_000)]
    public int BatchSize { get; set; }

    [Range(1, 60_000)]
    public int IdleDelayMs { get; set; }

    [Range(1, 3_600)]
    public int BaseBackoffSeconds { get; set; }

    [Range(1, 3_600)]
    public int MaxBackoffSeconds { get; set; }

    // Deadline for one upload call, so a connection that dies without an RST cannot stall the
    // uploader. TCP keepalive is OS-wide and cannot detect a server that took the bytes and then
    // stopped answering.
    [Range(1, 600)]
    public int UploadTimeoutSeconds { get; set; }
}
