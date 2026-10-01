using System.ComponentModel.DataAnnotations;

namespace TxMonitoring.Shared;

/// <summary>
/// Collector settings. Every service exports to the same collector, so the section is shared.
/// </summary>
public class OTelOptions
{
    public const string Section = "OTel";

    // [Required] already rejects null, empty, and whitespace-only strings. The C# `required`
    // keyword would not help: the configuration binder creates objects by reflection, which
    // skips it.
    [Required]
    public string ServiceName { get; set; } = string.Empty;

    [Required]
    [Url]
    public string Endpoint { get; set; } = string.Empty;
}
