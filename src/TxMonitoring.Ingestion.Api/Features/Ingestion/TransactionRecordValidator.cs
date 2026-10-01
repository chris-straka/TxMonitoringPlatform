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
/// A failure here deletes the gateway's copy, so every rule must be one no retry can fix.
/// A future timestamp can become valid on retry, so the clock-skew check lives at gateway
/// admission instead.
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

    // Unset proto3 fields arrive as 0 or empty, so the rules reject those defaults.
    public TransactionRecordValidator()
    {
        RuleFor(x => x.MessageId)
            .NotEmpty()
            .MaximumLength(TransactionLimits.MaxMessageIdLength)
            .Must(id => Guid.TryParseExact(id, "D", out _))
            .WithMessage("MessageId must use the canonical GUID format.");

        // Also the Kafka partition key.
        RuleFor(x => x.AccountId)
            .NotEmpty()
            .MaximumLength(TransactionLimits.MaxAccountIdLength)
            .Matches("^[A-Za-z0-9._:-]+$")
            .Must(id => id is null || id == id.Trim())
            .WithMessage("AccountId cannot have leading or trailing whitespace.");

        // Generators count from 1. The ceiling keeps the value exact as a JavaScript number.
        RuleFor(x => x.SequenceNumber)
            .GreaterThan(0)
            .LessThanOrEqualTo(TransactionLimits.MaxSequenceNumber);

        RuleFor(x => x.AmountMinor)
            .GreaterThan(0)
            .LessThanOrEqualTo(TransactionLimits.MaxAmountMinor);

        // The accepted set lives in the shared contracts and is currently CAD only.
        RuleFor(x => x.Currency)
            .Must(TransactionCurrencies.IsSupported)
            .WithMessage("Currency is not a supported three-letter code.");

        RuleFor(x => x.OccurredAt)
            .NotNull()
            .Must(IsRepresentable)
            .WithMessage("OccurredAt is outside the range google.protobuf.Timestamp can hold.")
            .Must(IsWholeMilliseconds)
            .WithMessage("OccurredAt must be exactly representable as epoch milliseconds.");

        // Becomes a Kafka header, so it is bounded and parsed to keep the message well formed.
        RuleFor(x => x.Traceparent)
            .MaximumLength(TransactionLimits.MaxTraceParentLength)
            .Must(IsTraceParent)
            .WithMessage("Traceparent is not a valid W3C trace context.");
    }

    // Rejects values a broken clock could produce that Timestamp cannot represent.
    private static bool IsRepresentable(Timestamp? occurredAt) =>
        occurredAt is null
        || (
            occurredAt.Seconds >= MinSeconds
            && occurredAt.Seconds <= MaxSeconds
            && occurredAt.Nanos is >= 0 and < NanosPerSecond
        );

    // The contract resolution is one millisecond.
    private static bool IsWholeMilliseconds(Timestamp? occurredAt) =>
        occurredAt is null || occurredAt.Nanos % NanosPerMillisecond == 0;

    private static bool IsTraceParent(string traceParent) =>
        string.IsNullOrEmpty(traceParent)
        || ActivityContext.TryParse(traceParent, null, isRemote: true, out _);
}
