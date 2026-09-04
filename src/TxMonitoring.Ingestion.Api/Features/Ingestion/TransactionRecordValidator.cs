using System.Diagnostics;

using FluentValidation;

using Google.Protobuf.WellKnownTypes;

using TxMonitoring.Ingestion.V1;
using TxMonitoring.Shared;

namespace TxMonitoring.Ingestion.Api.Features.Ingestion;

/// <summary>
/// What ingestion requires of a transaction before it produces to Kafka.
/// </summary>
/// <remarks>
/// Every rule must be something no retry can fix (failures here delete the gateway copy).
/// A timestamp that is merely "in the future relative to now" can become valid on retry,
/// so it must never be a permanent rejection here; the gateway applies the now-relative
/// skew check at admission instead.
/// </remarks>
public class TransactionRecordValidator : AbstractValidator<TransactionRecord>
{
    private static readonly long MinSeconds = Timestamp
        .FromDateTimeOffset(DateTimeOffset.MinValue)
        .Seconds;
    private static readonly long MaxSeconds = Timestamp
        .FromDateTimeOffset(DateTimeOffset.MaxValue)
        .Seconds;

    private const int NanosPerSecond = 1_000_000_000;
    private const int NanosPerMillisecond = 1_000_000;

    // proto3 defaults are either 0 or null
    public TransactionRecordValidator()
    {
        // Detection dedupes on this too
        RuleFor(x => x.MessageId)
            .NotEmpty()
            .MaximumLength(TransactionLimits.MaxMessageIdLength)
            .Must(id => Guid.TryParseExact(id, "D", out _))
            .WithMessage("MessageId must use the canonical GUID format.");

        // The Kafka message key (used for partitions)
        RuleFor(x => x.AccountId)
            .NotEmpty()
            .MaximumLength(TransactionLimits.MaxAccountIdLength)
            .Matches("^[A-Za-z0-9._:-]+$")
            .Must(id => id is null || id == id.Trim())
            .WithMessage("AccountId cannot have leading or trailing whitespace.");

        // 0 should not be possible (generators count from 1); the ceiling keeps the
        // JSON representation exact in JavaScript.
        RuleFor(x => x.SequenceNumber)
            .GreaterThan(0)
            .LessThanOrEqualTo(TransactionLimits.MaxSequenceNumber);

        // Positive integer minor units within the single named ceiling.
        RuleFor(x => x.AmountMinor)
            .GreaterThan(0)
            .LessThanOrEqualTo(TransactionLimits.MaxAmountMinor);

        // The accepted set lives in shared contract code; v1 accepts CAD only.
        RuleFor(x => x.Currency)
            .Must(TransactionCurrencies.IsSupported)
            .WithMessage("Currency is not a supported three-letter code.");

        RuleFor(x => x.OccurredAt)
            .NotNull()
            .Must(IsRepresentable)
            .WithMessage("OccurredAt is outside the range google.protobuf.Timestamp can hold.")
            .Must(IsWholeMilliseconds)
            .WithMessage("OccurredAt must be exactly representable as epoch milliseconds.");

        // This becomes a Kafka header. Bounding and parsing it prevents an otherwise valid
        // transaction from becoming an oversized or malformed downstream message.
        RuleFor(x => x.Traceparent)
            .MaximumLength(TransactionLimits.MaxTraceParentLength)
            .Must(IsTraceParent)
            .WithMessage("Traceparent is not a valid W3C trace context.");
    }

    // Filters out values from a broken clock
    private static bool IsRepresentable(Timestamp? occurredAt) =>
        occurredAt is null
        || (
            occurredAt.Seconds >= MinSeconds
            && occurredAt.Seconds <= MaxSeconds
            && occurredAt.Nanos is >= 0 and < NanosPerSecond
        );

    // The contract resolution is one millisecond: sub-millisecond precision is malformed.
    private static bool IsWholeMilliseconds(Timestamp? occurredAt) =>
        occurredAt is null || occurredAt.Nanos % NanosPerMillisecond == 0;

    private static bool IsTraceParent(string traceParent) =>
        string.IsNullOrEmpty(traceParent)
        || ActivityContext.TryParse(traceParent, null, isRemote: true, out _);
}
