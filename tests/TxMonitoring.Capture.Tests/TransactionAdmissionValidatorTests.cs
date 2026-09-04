using TxMonitoring.Capture.Gateway.Features.Buffer;
using TxMonitoring.Shared;

namespace TxMonitoring.Capture.Tests;

public sealed class TransactionAdmissionValidatorTests
{
    [Fact]
    public void Valid_submission_is_accepted_and_its_guid_is_canonicalized()
    {
        var id = Guid.CreateVersion7();
        var submission = ValidSubmission() with
        {
            MessageId = id.ToString("D").ToUpperInvariant(),
        };

        var error = TransactionAdmissionValidator.Validate(submission, out var canonicalId);

        Assert.Null(error);
        Assert.Equal(id.ToString("D"), canonicalId);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-guid")]
    [InlineData("00000000000000000000000000000000")]
    public void Noncanonical_message_ids_are_rejected(string messageId)
    {
        var error = TransactionAdmissionValidator.Validate(
            ValidSubmission() with { MessageId = messageId },
            out _
        );

        Assert.Equal("message_id_must_be_a_guid", error);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ACCT-1")]
    [InlineData("ACCT-1 ")]
    public void Bad_account_ids_are_rejected(string accountId)
    {
        var oversized = TransactionAdmissionValidator.Validate(
            ValidSubmission() with { AccountId = new string('x', TransactionLimits.MaxAccountIdLength + 1) },
            out _
        );
        var bad = TransactionAdmissionValidator.Validate(
            ValidSubmission() with { AccountId = accountId },
            out _
        );

        Assert.Equal("account_id_is_invalid", oversized);
        Assert.Equal("account_id_is_invalid", bad);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(TransactionLimits.MaxSequenceNumber + 1)]
    public void Out_of_range_sequence_numbers_are_rejected(long sequenceNumber)
    {
        var error = TransactionAdmissionValidator.Validate(
            ValidSubmission() with { SequenceNumber = sequenceNumber },
            out _
        );

        Assert.Equal("sequence_number_out_of_range", error);
    }

    [Fact]
    public void Sub_millisecond_event_time_is_rejected()
    {
        var subMs = ValidSubmission() with
        {
            OccurredAt = new DateTimeOffset(637_000_000_000_000_001, TimeSpan.Zero),
        };

        var error = TransactionAdmissionValidator.Validate(subMs, out _);

        Assert.Equal("occurred_at_must_be_whole_milliseconds", error);
    }

    [Fact]
    public void Event_time_far_in_the_future_is_rejected_at_admission()
    {
        var future = ValidSubmission() with
        {
            OccurredAt = TransactionTime.TruncateToMilliseconds(
                DateTimeOffset.UtcNow + TransactionAdmissionValidator.MaxFutureSkew * 2
            ),
        };

        var error = TransactionAdmissionValidator.Validate(future, out _);

        Assert.Equal("occurred_at_too_far_in_future", error);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-100)]
    [InlineData(TransactionLimits.MaxAmountMinor + 1)]
    public void Out_of_range_amounts_are_rejected(long amountMinor)
    {
        var error = TransactionAdmissionValidator.Validate(
            ValidSubmission() with { AmountMinor = amountMinor },
            out _
        );

        Assert.Equal("amount_minor_out_of_range", error);
    }

    [Theory]
    [InlineData("USD")]
    [InlineData("cad")]
    [InlineData("")]
    public void Unsupported_currencies_are_rejected(string currency)
    {
        var error = TransactionAdmissionValidator.Validate(
            ValidSubmission() with { Currency = currency },
            out _
        );

        Assert.Equal("currency_is_not_supported", error);
    }

    private static TransactionSubmission ValidSubmission() =>
        new(
            Guid.CreateVersion7().ToString("D"),
            "ACCT-0001",
            1,
            TransactionTime.TruncateToMilliseconds(DateTimeOffset.UtcNow),
            2_500,
            "CAD"
        );
}
