using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace TxMonitoring.Detection.Worker.Migrations;

/// <inheritdoc />
public partial class DecisionEvidence : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "IX_Transactions_AccountId_OccurredAt",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "DetectorScore",
            table: "Transactions");

        migrationBuilder.RenameColumn(
            name: "IsAnomaly",
            table: "Transactions",
            newName: "ReviewRecommended");

        migrationBuilder.AlterColumn<string>(
            name: "DetectorVersion",
            table: "Transactions",
            type: "character varying(64)",
            maxLength: 64,
            nullable: false,
            defaultValue: "",
            oldClrType: typeof(string),
            oldType: "character varying(64)",
            oldMaxLength: 64,
            oldNullable: true);

        migrationBuilder.AlterColumn<double>(
            name: "DetectorPValue",
            table: "Transactions",
            type: "double precision",
            nullable: false,
            defaultValue: 0.0,
            oldClrType: typeof(double),
            oldType: "double precision",
            oldNullable: true);

        migrationBuilder.AlterColumn<int>(
            name: "DetectorHistoryCount",
            table: "Transactions",
            type: "integer",
            nullable: false,
            defaultValue: 0,
            oldClrType: typeof(int),
            oldType: "integer",
            oldNullable: true);

        migrationBuilder.AddColumn<long>(
            name: "Amount5mMinor",
            table: "Transactions",
            type: "bigint",
            nullable: true);

        migrationBuilder.AddColumn<long>(
            name: "Count1m",
            table: "Transactions",
            type: "bigint",
            nullable: true);

        migrationBuilder.AddColumn<bool>(
            name: "DetectorIsSpike",
            table: "Transactions",
            type: "boolean",
            nullable: false,
            defaultValue: false);

        migrationBuilder.AddColumn<double>(
            name: "DetectorRawScore",
            table: "Transactions",
            type: "double precision",
            nullable: false,
            defaultValue: 0.0);

        migrationBuilder.AddColumn<int>(
            name: "FingerprintVersion",
            table: "Transactions",
            type: "integer",
            nullable: false,
            defaultValue: 0);

        migrationBuilder.AddColumn<byte[]>(
            name: "PayloadFingerprint",
            table: "Transactions",
            type: "bytea",
            nullable: false,
            defaultValue: new byte[0]);

        migrationBuilder.AddColumn<string>(
            name: "PolicyConfigurationHash",
            table: "Transactions",
            type: "character varying(64)",
            maxLength: 64,
            nullable: false,
            defaultValue: "");

        migrationBuilder.AddColumn<string>(
            name: "PolicyVersion",
            table: "Transactions",
            type: "character varying(64)",
            maxLength: 64,
            nullable: false,
            defaultValue: "");

        migrationBuilder.AddColumn<string>(
            name: "ReasonsJson",
            table: "Transactions",
            type: "text",
            nullable: false,
            defaultValue: "");

        migrationBuilder.AddColumn<int>(
            name: "RiskScore",
            table: "Transactions",
            type: "integer",
            nullable: false,
            defaultValue: 0);

        migrationBuilder.AddColumn<string>(
            name: "VelocityStatus",
            table: "Transactions",
            type: "character varying(16)",
            maxLength: 16,
            nullable: false,
            defaultValue: "");

        migrationBuilder.CreateIndex(
            name: "IX_Transactions_AccountId_Currency_DetectedAt_Id",
            table: "Transactions",
            columns: new[] { "AccountId", "Currency", "DetectedAt", "Id" });

        migrationBuilder.CreateIndex(
            name: "IX_Transactions_AccountId_Currency_OccurredAt",
            table: "Transactions",
            columns: new[] { "AccountId", "Currency", "OccurredAt" });

        migrationBuilder.AddCheckConstraint(
            name: "CK_Transactions_AmountMinor",
            table: "Transactions",
            sql: "\"AmountMinor\" >= 1 AND \"AmountMinor\" <= 100000000000");

        migrationBuilder.AddCheckConstraint(
            name: "CK_Transactions_Currency",
            table: "Transactions",
            sql: "char_length(\"Currency\") = 3");

        migrationBuilder.AddCheckConstraint(
            name: "CK_Transactions_RiskScore",
            table: "Transactions",
            sql: "\"RiskScore\" >= 0 AND \"RiskScore\" <= 100");

        migrationBuilder.AddCheckConstraint(
            name: "CK_Transactions_VelocityCounts",
            table: "Transactions",
            sql: "\"Count1m\" IS NULL OR \"Count1m\" >= 0");

        migrationBuilder.AddCheckConstraint(
            name: "CK_Transactions_VelocitySums",
            table: "Transactions",
            sql: "\"Amount5mMinor\" IS NULL OR \"Amount5mMinor\" >= 0");
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "IX_Transactions_AccountId_Currency_DetectedAt_Id",
            table: "Transactions");

        migrationBuilder.DropIndex(
            name: "IX_Transactions_AccountId_Currency_OccurredAt",
            table: "Transactions");

        migrationBuilder.DropCheckConstraint(
            name: "CK_Transactions_AmountMinor",
            table: "Transactions");

        migrationBuilder.DropCheckConstraint(
            name: "CK_Transactions_Currency",
            table: "Transactions");

        migrationBuilder.DropCheckConstraint(
            name: "CK_Transactions_RiskScore",
            table: "Transactions");

        migrationBuilder.DropCheckConstraint(
            name: "CK_Transactions_VelocityCounts",
            table: "Transactions");

        migrationBuilder.DropCheckConstraint(
            name: "CK_Transactions_VelocitySums",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "Amount5mMinor",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "Count1m",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "DetectorIsSpike",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "DetectorRawScore",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "FingerprintVersion",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "PayloadFingerprint",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "PolicyConfigurationHash",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "PolicyVersion",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "ReasonsJson",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "RiskScore",
            table: "Transactions");

        migrationBuilder.DropColumn(
            name: "VelocityStatus",
            table: "Transactions");

        migrationBuilder.RenameColumn(
            name: "ReviewRecommended",
            table: "Transactions",
            newName: "IsAnomaly");

        migrationBuilder.AlterColumn<string>(
            name: "DetectorVersion",
            table: "Transactions",
            type: "character varying(64)",
            maxLength: 64,
            nullable: true,
            oldClrType: typeof(string),
            oldType: "character varying(64)",
            oldMaxLength: 64);

        migrationBuilder.AlterColumn<double>(
            name: "DetectorPValue",
            table: "Transactions",
            type: "double precision",
            nullable: true,
            oldClrType: typeof(double),
            oldType: "double precision");

        migrationBuilder.AlterColumn<int>(
            name: "DetectorHistoryCount",
            table: "Transactions",
            type: "integer",
            nullable: true,
            oldClrType: typeof(int),
            oldType: "integer");

        migrationBuilder.AddColumn<double>(
            name: "DetectorScore",
            table: "Transactions",
            type: "double precision",
            nullable: true);

        migrationBuilder.CreateIndex(
            name: "IX_Transactions_AccountId_OccurredAt",
            table: "Transactions",
            columns: new[] { "AccountId", "OccurredAt" });
    }
}
