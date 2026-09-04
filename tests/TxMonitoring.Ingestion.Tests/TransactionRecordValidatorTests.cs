using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;

using TxMonitoring.Ingestion.Api.Configuration;
using TxMonitoring.Ingestion.Api.Features.Ingestion;
using TxMonitoring.Ingestion.V1;
using TxMonitoring.Shared;

namespace TxMonitoring.Ingestion.Tests;

public sealed class TransactionRecordValidatorTests
{
    private readonly TransactionRecordValidator _validator = new();

    [Fact]
    public void Generator_shaped_record_is_valid()
    {
        var result = _validator.Validate(ValidRecord());

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Oversized_account_is_permanently_rejected()
    {
        var oversized = ValidRecord();
        oversized.AccountId = new string('x', TransactionLimits.MaxAccountIdLength + 1);

        Assert.False(_validator.Validate(oversized).IsValid);
    }

    [Fact]
    public void Zero_negative_and_over_ceiling_amounts_are_permanently_rejected()
    {
        var zero = ValidRecord();
        zero.AmountMinor = 0;
        var negative = ValidRecord();
        negative.AmountMinor = -100;
        var overCeiling = ValidRecord();
        overCeiling.AmountMinor = TransactionLimits.MaxAmountMinor + 1;

        Assert.False(_validator.Validate(zero).IsValid);
        Assert.False(_validator.Validate(negative).IsValid);
        Assert.False(_validator.Validate(overCeiling).IsValid);
    }

    [Fact]
    public void Unsupported_currency_is_permanently_rejected()
    {
        var lowercase = ValidRecord();
        lowercase.Currency = "cad";
        var other = ValidRecord();
        other.Currency = "USD";

        Assert.False(_validator.Validate(lowercase).IsValid);
        Assert.False(_validator.Validate(other).IsValid);
    }

    [Fact]
    public void Sub_millisecond_timestamp_is_permanently_rejected()
    {
        var subMs = ValidRecord();
        subMs.OccurredAt = new Timestamp { Seconds = subMs.OccurredAt.Seconds, Nanos = 500 };

        Assert.False(_validator.Validate(subMs).IsValid);
    }

    [Fact]
    public void Future_relative_timestamp_is_not_a_permanent_rejection()
    {
        // A clock that runs ahead can be valid on retry, so the ingestion validator
        // must not fail it: only the gateway applies a now-relative admission skew bound.
        var future = ValidRecord();
        future.OccurredAt = Timestamp.FromDateTimeOffset(
            TransactionTime.TruncateToMilliseconds(DateTimeOffset.UtcNow.AddHours(1))
        );

        Assert.True(_validator.Validate(future).IsValid);
    }

    [Fact]
    public void Malformed_trace_context_is_rejected_before_it_becomes_a_kafka_header()
    {
        var record = ValidRecord();
        record.Traceparent = "not-a-traceparent";

        Assert.False(_validator.Validate(record).IsValid);
    }

    [Fact]
    public void Protobuf_contract_pins_service_rpc_fields_numbers_and_types()
    {
        var file = TransactionsReflection.Descriptor;
        Assert.Equal("txmonitoring.ingestion.v1", file.Package);

        var service = Assert.Single(file.Services);
        Assert.Equal("TransactionIngestion", service.Name);

        var rpc = Assert.Single(service.Methods);
        Assert.Equal("UploadTransactions", rpc.Name);
        Assert.Equal("UploadTransactionsRequest", rpc.InputType.Name);
        Assert.Equal("IngestResponse", rpc.OutputType.Name);

        var record = TransactionRecord.Descriptor;
        AssertFields(
            record,
            ("message_id", 1, FieldType.String, false),
            ("account_id", 2, FieldType.String, false),
            ("sequence_number", 3, FieldType.Int64, false),
            ("occurred_at", 4, FieldType.Message, false),
            ("amount_minor", 5, FieldType.Int64, false),
            ("currency", 6, FieldType.String, false),
            ("traceparent", 7, FieldType.String, false)
        );
        Assert.Equal(
            "google.protobuf.Timestamp",
            record.FindFieldByNumber(4).MessageType.FullName
        );

        var request = UploadTransactionsRequest.Descriptor;
        AssertFields(request, ("transactions", 1, FieldType.Message, true));

        // accepted_message_ids = 2 and rejected_message_ids = 3 are frozen wire tags.
        var response = IngestResponse.Descriptor;
        AssertFields(
            response,
            ("complete", 1, FieldType.Bool, false),
            ("accepted_message_ids", 2, FieldType.String, true),
            ("rejected_message_ids", 3, FieldType.String, true)
        );
    }

    [Fact]
    public void Worst_valid_batch_fits_the_configured_receive_limit()
    {
        var worst = new UploadTransactionsRequest();
        for (var i = 0; i < TransactionLimits.MaxBatchSize; i++)
        {
            worst.Transactions.Add(
                new TransactionRecord
                {
                    MessageId = new string('0', TransactionLimits.MaxMessageIdLength),
                    AccountId = new string('a', TransactionLimits.MaxAccountIdLength),
                    SequenceNumber = TransactionLimits.MaxSequenceNumber,
                    OccurredAt = new Timestamp
                    {
                        Seconds = 9_999_999_999L,
                        Nanos = 999_000_000,
                    },
                    AmountMinor = TransactionLimits.MaxAmountMinor,
                    Currency = "CAD",
                    Traceparent = new string('t', TransactionLimits.MaxTraceParentLength),
                }
            );
        }

        Assert.True(
            worst.CalculateSize() < GrpcOptions.DefaultMaxReceiveMessageSizeBytes,
            $"Worst valid batch is {worst.CalculateSize()} bytes; the uploader bound must stay below the receive limit."
        );
    }

    private static void AssertFields(
        MessageDescriptor message,
        params (string Name, int Number, FieldType Type, bool Repeated)[] expected
    )
    {
        Assert.Equal(expected.Length, message.Fields.InDeclarationOrder().Count());
        foreach (var (name, number, type, repeated) in expected)
        {
            var field = message.FindFieldByNumber(number);
            Assert.NotNull(field);
            Assert.Equal(name, field.Name);
            Assert.Equal(type, field.FieldType);
            Assert.Equal(repeated, field.IsRepeated);
        }
    }

    private static TransactionRecord ValidRecord() =>
        new()
        {
            MessageId = Guid.CreateVersion7().ToString("D"),
            AccountId = "ACCT-0001",
            SequenceNumber = 1,
            OccurredAt = Timestamp.FromDateTimeOffset(
                TransactionTime.TruncateToMilliseconds(DateTimeOffset.UtcNow)
            ),
            AmountMinor = 2_500,
            Currency = "CAD",
            Traceparent = string.Empty,
        };
}
