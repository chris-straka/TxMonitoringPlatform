using System;

using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TxMonitoring.Detection.Worker.Migrations;

/// <inheritdoc />
public partial class InitialCreate : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "AlertOutboxMessages",
            columns: table => new
            {
                Id = table.Column<Guid>(type: "uuid", nullable: false),
                MessageId = table.Column<Guid>(type: "uuid", nullable: false),
                AccountId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                Payload = table.Column<string>(type: "text", nullable: false),
                TraceParent = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: true),
                CreatedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                PublishedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                AttemptCount = table.Column<int>(type: "integer", nullable: false),
                LastError = table.Column<string>(type: "character varying(2000)", maxLength: 2000, nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_AlertOutboxMessages", x => x.Id);
            });

        migrationBuilder.CreateTable(
            name: "Transactions",
            columns: table => new
            {
                Id = table.Column<Guid>(type: "uuid", nullable: false),
                MessageId = table.Column<Guid>(type: "uuid", nullable: false),
                AccountId = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                SequenceNumber = table.Column<long>(type: "bigint", nullable: false),
                OccurredAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                ReceivedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                DetectedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                AmountMinor = table.Column<long>(type: "bigint", nullable: false),
                Currency = table.Column<string>(type: "character varying(3)", maxLength: 3, nullable: false),
                IsAnomaly = table.Column<bool>(type: "boolean", nullable: false),
                DetectorScore = table.Column<double>(type: "double precision", nullable: true),
                DetectorPValue = table.Column<double>(type: "double precision", nullable: true),
                DetectorHistoryCount = table.Column<int>(type: "integer", nullable: true),
                DetectorVersion = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_Transactions", x => x.Id);
            });

        migrationBuilder.CreateIndex(
            name: "IX_AlertOutboxMessages_MessageId",
            table: "AlertOutboxMessages",
            column: "MessageId",
            unique: true);

        migrationBuilder.CreateIndex(
            name: "IX_AlertOutboxMessages_PublishedAt_CreatedAt",
            table: "AlertOutboxMessages",
            columns: new[] { "PublishedAt", "CreatedAt" });

        migrationBuilder.CreateIndex(
            name: "IX_Transactions_AccountId_OccurredAt",
            table: "Transactions",
            columns: new[] { "AccountId", "OccurredAt" });

        migrationBuilder.CreateIndex(
            name: "IX_Transactions_MessageId",
            table: "Transactions",
            column: "MessageId",
            unique: true);
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "AlertOutboxMessages");

        migrationBuilder.DropTable(
            name: "Transactions");
    }
}
