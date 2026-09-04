# TxMonitoringPlatform

.NET transaction monitoring platform:

`load generator -> capture gateway -> ingestion API -> Kafka -> detection/Postgres -> SignalR dashboard`

It **observes** a transaction feed, holds per-account state, scores each transaction for risk, and
raises alerts. It never moves, holds, authorises, or settles money. It is not a ledger and not a
payments system.

> **Port in progress.** This repository is a duplicate of an industrial telemetry project and the
> code on disk still uses the old names (`TelemetryReading`, `EquipmentId`, `Industrial.*`).
> `PORT.md` holds the full rename map and the two semantic corrections. This file already describes
> the *target* state. Where the two disagree, the code is behind, not wrong in principle.

The project is meant to be demonstrated and defended in an interview. Prefer a smaller claim the
code can prove over a larger claim hidden behind comments.

## Reliability boundary

The load generator is intentionally best-effort and is **not a system of record**. It produces
synthetic load; acquisition never waits for the network:

- A full in-memory channel drops the new item and increments `feed.transaction.dropped`.
- The transmission loop uses a bounded HTTP resilience policy. If it never receives a `202`, it
  discards that dequeued item and increments `feed.transaction.delivery_dropped`.
- Nothing upstream of the gateway's `202` claims custody. Do not describe generator-to-gateway
  delivery as at-least-once, and do not claim durable capture of every generated transaction.

The durable contract begins when the capture gateway returns `202 Accepted` for a valid transaction:

1. The origin mints `MessageId` and `OccurredAt`; no downstream component regenerates either.
2. The gateway validates bounded content, writes to SQLite with WAL and `synchronous=FULL`, and
   only then returns `202`.
3. The gateway keeps the row until ingestion names its `MessageId` as accepted or permanently
   rejected. IDs named in neither list remain queued for retry.
4. Ingestion names only deterministic validation failures as rejected. Kafka/producer failures
   leave the ID unnamed so the gateway retains it.
5. Kafka delivery and the detection consumer are at-least-once. Detection commits an offset only
   after processing succeeds and rewinds the record on failure.
6. Detection publishes a malformed record or tombstone to `transaction-events-dlq` before
   committing its source offset. If that publish fails or is ambiguous, it rewinds instead.
7. Postgres has a unique `MessageId` index, so a Kafka replay is a no-op.
8. An alert row and its outbox row commit in one Postgres transaction. The publisher may emit a
   duplicate after an ambiguous Kafka ACK; dashboard alert dedupe uses `MessageId`.

The gateway retains recently settled IDs after deleting live queue rows, absorbing a delayed retry
whose original `202` was lost. Retention is bounded; it is not a permanent global ledger. A
rejection is explicit permanent loss — logged, metered, and copied into a bounded SQLite quarantine
before leaving the live queue so a poison row cannot block newer transactions.

## Rules that protect the contract

- Preserve `MessageId` unchanged across every hop.
- Never turn a transient/internal exception into `rejected_message_ids`; that tells the gateway to
  delete its durable copy.
- Keep gateway receive and settle mutations under `BufferMutationGate`. `BufferDepth.TryReserve`
  is the atomic `MaxDepth` admission decision; do not overwrite it with a racing periodic count.
- Amounts are **integer minor units**. No floats in the money path.
- A poison Kafka record may be committed only after its DLQ copy is acknowledged.
- Protobuf field numbers are frozen. `accepted_message_ids = 2` and `rejected_message_ids = 3`.
  Renaming a field is fine; renumbering is not. Regression tests pin the current tags.
- `ModelEngine` is an online IID spike detector keyed per `AccountId`, **not** a trained or learned
  fraud model. State is isolated per account, warmed from recent Postgres rows, and invalidated
  after a failed DB transaction.
- A Web API replica owns only its local SignalR clients, so every replica uses a distinct Kafka
  group. Do not replace that with a shared group without adding a SignalR backplane.

## Redis is a cache, never the durable path

Redis holds sliding-window velocity counters per `AccountId`. It is derived state:

- Ingestion, Kafka consumption, and the Postgres write never depend on Redis being up.
- If Redis is unavailable, detection degrades: log it, increment
  `detection.velocity.unavailable`, and continue. Do not fail the message, rewind the offset, or
  send it to the DLQ.
- Warm the windows from recent Postgres transactions on startup, the same way `ModelEngine` state
  is warmed.
- Losing Redis loses accuracy for a window, never a transaction.

## Comments and design notes

Explanatory comments are teaching material for the portfolio walkthrough. Preserve useful reasoning
when editing nearby code. If a comment is wrong, correct the fact rather than leaving it as
archaeology. Rejected alternatives belong in `docs/`. `TODO.md` contains only open work.

## Configuration

Runtime services bind typed options and validate them at startup. Docker key `Foo__Bar` becomes
`Foo:Bar`. Workers receive `IOptions<T>`, not unrestricted `IConfiguration`. Use
`[Required(AllowEmptyStrings = false)]` and `[Range]`. Do not silently substitute a fallback for a
missing deployment value. Only the detection worker owns production EF migrations; the gateway uses
`EnsureCreated` because SQLite is a transient queue.

## Required checks

```sh
dotnet build TxMonitoringPlatform.slnx --no-restore
dotnet test TxMonitoringPlatform.slnx --no-restore
cd src/TxMonitoring.Web.Dashboard && npm run lint && npm run build
docker compose config --quiet
dotnet ef migrations has-pending-model-changes --project src/TxMonitoring.Detection.Worker --no-build
```
