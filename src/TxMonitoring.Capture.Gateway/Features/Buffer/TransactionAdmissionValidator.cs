using TxMonitoring.Shared;

namespace TxMonitoring.Capture.Gateway.Features.Buffer;

/// <summary>
/// Applies permanent, retry-independent checks before a transaction can consume durable capacity.
/// </summary>
public static class TransactionAdmissionValidator
{
    /// <summary>
    /// Origin event time may not run ahead of the gateway clock by more than this. A broken
    /// generator clock is malformed at admission; ingestion must never turn a merely early
    /// timestamp into a permanent rejection because the same record can be valid on retry.
    /// </summary>
    public static readonly TimeSpan MaxFutureSkew = TimeSpan.FromMinutes(5);

    public static string? Validate(TransactionSubmission submission, out string canonicalMessageId)
    {
        canonicalMessageId = string.Empty;

        if (!Guid.TryParseExact(submission.MessageId, "D", out var parsedMessageId))
            return "message_id_must_be_a_guid";

        canonicalMessageId = parsedMessageId.ToString("D");

        if (
            string.IsNullOrWhiteSpace(submission.AccountId)
            || submission.AccountId.Length > TransactionLimits.MaxAccountIdLength
            || submission.AccountId != submission.AccountId.Trim()
            || !submission.AccountId.All(IsAccountIdCharacter)
        )
            return "account_id_is_invalid";

        if (
            submission.SequenceNumber <= 0
            || submission.SequenceNumber > TransactionLimits.MaxSequenceNumber
        )
            return "sequence_number_out_of_range";

        if (submission.OccurredAt == default)
            return "occurred_at_is_required";

        // The contract resolution is one millisecond. Equivalent source offsets are not
        // different data; the instant is compared and normalised to UTC before custody.
        if (submission.OccurredAt.UtcTicks % TimeSpan.TicksPerMillisecond != 0)
            return "occurred_at_must_be_whole_milliseconds";

        if (submission.OccurredAt.ToUniversalTime() > DateTimeOffset.UtcNow + MaxFutureSkew)
            return "occurred_at_too_far_in_future";

        if (
            submission.AmountMinor <= 0
            || submission.AmountMinor > TransactionLimits.MaxAmountMinor
        )
            return "amount_minor_out_of_range";

        if (!TransactionCurrencies.IsSupported(submission.Currency))
            return "currency_is_not_supported";

        return null;
    }

    private static bool IsAccountIdCharacter(char value) =>
        char.IsAsciiLetterOrDigit(value) || value is '-' or '_' or '.' or ':';
}
