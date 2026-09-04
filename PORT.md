# PORT.md — Implementation brief for the transaction-monitoring port

This is the execution specification for converting the duplicated industrial telemetry repository
into **TxMonitoringPlatform**. The implementing agent should treat the decisions marked “required”
as requirements, not suggestions.

Read AGENTS.md before changing anything. AGENTS.md describes the intended transaction-monitoring
system and its reliability boundary; the code on disk still describes the industrial source. Where
they disagree, implement the target deliberately. Do not preserve an industrial idea merely because
a global rename makes it compile.

The reliability invariants in AGENTS.md remain mandatory. This expanded brief supersedes its stale
phrase “the two semantic corrections” and deliberately refines a few target details: risk is not an
input field, Gemini is removed, detector state includes currency, and Redis repair is lazy after
cache miss/outage rather than a one-time startup scan. Update AGENTS.md to match these decisions
before asking a later agent to rely on it.

The finished repository must be a coherent, runnable portfolio project that can survive detailed
interview questioning. Prefer a narrow claim proved by code, tests, metrics, and failure drills over
a broad fintech claim that exists only in prose.

## Mission and scope

Build a near-real-time **transaction risk-monitoring** demonstration:

~~~text
synthetic feed -> capture gateway -> ingestion API -> Kafka -> detection/Postgres
                                                        |              |
                                                        +-> Web API <--+ risk alerts
                                                               |
                                                            SignalR
                                                               |
                                                           dashboard
~~~

One v1 event is an immutable observation of a synthetic **outbound debit attempt** selected by an
upstream source for monitoring. `AmountMinor` is the attempted debit amount. Approval, decline,
posting, reversal, and settlement outcomes are deliberately absent, so do not mix credits, refunds,
declines, or other lifecycle events into these velocity totals. Adding those later requires an
explicit event type/status contract and rule-eligibility policy; it is not a data-only change.

The system maintains per-account state, assigns an explainable heuristic risk score, persists the
decision, and publishes review-recommended alerts. `AccountId` is an account-scoped monitoring key,
not a customer identity: this design cannot correlate activity spread across multiple accounts.

It does **not**:

- move, hold, authorise, capture, clear, settle, refund, or reconcile money;
- maintain balances or double-entry accounts;
- block a transaction or return an approve/decline decision;
- prove fraud, perform KYC, sanctions screening, AML case management, or regulatory reporting;
- store PANs, cardholder data, customer names, bank-account numbers, or real PII;
- provide a trained fraud model, a production deployment, or an exactly-once distributed pipeline.

Capture.Gateway means capture of an **event feed**. In payments, “capture” also means requesting
settlement after an authorisation; this project never performs that operation. State the distinction
wherever the name could be misunderstood.

## Definition of done

The port is complete only when:

1. Every executable project, test, namespace, assembly, container, topic, endpoint, table,
   dashboard, metric, trace, script, and active document uses the target vocabulary.
2. The numbered reliability contract in AGENTS.md remains demonstrably true.
3. Money is integer minor units from origin through persistence and alert payloads.
4. Risk is computed by detection. No producer-provided field is named RiskScore.
5. Each stored decision explains itself with evidence, stable reason codes, detector provenance,
   velocity availability, and content-derived policy provenance.
6. Redis improves velocity detection but cannot prevent Kafka consumption, Postgres persistence,
   offset progress, or alerting from the remaining signals.
7. Compose and the isolated failure harness prove the important success and failure paths.
8. README, design notes, TODO, and resume claims match what the code proves.
9. Every final verification check in this document passes from a clean checkout and fresh data.

## Required architecture decisions

### 1. Coordinated clean cutover, not a compatible upgrade

This repository is a pre-production portfolio fork. Tractor readings have no honest conversion to
financial transactions: 90 °C is not 90 cents, and oil pressure is not a currency or risk score. Do
not create a fake data migration.

The target protobuf deliberately changes the scalar types and meanings of fields 5 and 6. That is a
wire-breaking change even though the field numbers remain the same. Renaming the gRPC service and
method, JSON contracts, topics, and database tables is also breaking. Therefore:

- rebuild and deploy all application components together;
- use new Kafka topics so industrial payloads cannot reach the target consumer;
- create a fresh target EF migration baseline instead of hand-editing generated migrations;
- regenerate model.zip for the amount feature instead of renaming the old binary;
- start the target with fresh Postgres and SQLite volumes;
- prominently warn that the one-time local volume reset deletes local demo data.

Do not claim rolling compatibility, dual-read support, or protobuf compatibility with the
industrial build. Once the port lands, the **target** field numbers and types are frozen and pinned
by descriptor tests.

In a deployed system the compatible alternative would reserve the old fields and add financial
fields at new tag numbers. That is deliberately outside this clean-fork scope.

### 2. The feed is synthetic and best-effort

Keep the bounded in-memory Channel and bounded HTTP resilience behavior. The feed emulator is a load
generator, not a bank, processor, ledger, terminal, or source of truth.

- A full channel drops the newly generated item and increments feed.transaction.dropped.
- A dequeued item that never receives the gateway's exact 202 Accepted after bounded retries is
  discarded and increments feed.transaction.delivery_dropped.
- The generator never claims at-least-once delivery.
- Durable custody begins only after the capture gateway commits a valid event to SQLite and returns
  202.

This conflicts with the current docs/Fintech.md, which says a fintech rewrite must make the emulator
durable. Rewrite that document. A real payment origin would retain an authoritative record; this
repository deliberately begins with a synthetic feed and demonstrates downstream monitoring
custody.

### 3. Risk is an output, never an input

The previous mapping oil_pressure -> risk_score is forbidden. It makes an untrusted producer supply
the answer detection is supposed to calculate and collides with ML.NET's existing raw detector
score.

The input contains facts: identity, account, sequence, event time, amount, and currency. Detection
derives:

- a deterministic heuristic RiskScore from 0 through 100;
- ReviewRecommended;
- stable ReasonCodes;
- raw IID detector score and p-value;
- detector history count and artifact version;
- the velocity-window snapshot and its availability status;
- the risk-policy version.

RiskScore is a ranking score defined by a transparent demo policy. It is **not** a calibrated fraud
probability. ReviewRecommended means “emit a monitoring alert,” not “fraud occurred.” There is no
case queue, assignment, disposition, or regulatory-review workflow in v1.

### 4. Remove Gemini and LLM enrichment

Delete the Gemini package, client, options, API key, Compose/Helm secret wiring, failure metric, and
AI-diagnostics prose. Replace free-form Diagnostics with deterministic reason codes and evidence.

An external LLM adds privacy, availability, latency, cost, and explainability problems and currently
holds an outbox row lock during a network call. It is not part of a defensible monitoring decision
path. A future natural-language summary would belong in a separate, optional, synthetic-data-only
presentation service after the durable decision and alert are published. That is outside this port.

### 5. Redis is derived state, never custody

Redis contains hot velocity state only. It is neither authoritative nor a prerequisite for durable
processing. Missing or syntactically invalid Redis configuration is a startup configuration error;
network unavailability is a supported degraded mode even on a cold start. Use bounded
connect/command timeouts and either lazy connection or `AbortOnConnectFail=false`.

Redis must not participate in application liveness or readiness, and detection must not have a
Compose/Kubernetes startup dependency on Redis health. Its outage must not rewind the
consumer, send a valid event to the DLQ, omit the transaction from Postgres, or block non-velocity
alert signals. Persist Unavailable explicitly; never represent unavailability as zero counts.

### 6. Preserve the stack and avoid architecture theatre

Keep .NET 10, ASP.NET Core, BackgroundService, EF Core, Npgsql, SQLite, Confluent.Kafka, gRPC,
OpenTelemetry, React/TypeScript, Docker Compose, Tilt, Helm, and Terraform. Preserve the existing
vertical-slice layout.

Do not add MediatR, a generic repository/unit-of-work wrapper, event sourcing, Aspire, a new message
bus, or a schema registry during this port. Do not combine the domain conversion with blind package
upgrades.

## Target contracts

### Identity and clocks

- MessageId is the immutable event ID and idempotency key minted by the synthetic origin. The
  generator uses UUIDv7; validators accept canonical GUIDs so tests and demo clients need not forge
  a version.
- There is no separate business TransactionId and no payment lifecycle in v1. One event is one
  monitored observation. Do not imply correlation among authorisation, capture, refund, and
  chargeback events.
- AccountId is an opaque synthetic identifier and the Kafka key.
- SequenceNumber is monotonic per generated account within one emulator run and resets on restart.
  It exposes gaps within an observed run; it is not a ledger sequence or proof that an unseen tail
  never existed. Bound it to 9_007_199_254_740_991 so its JSON representation is exact in
  JavaScript.
- OccurredAt is origin event time. The contract resolution is one millisecond: require an instant
  exactly representable as epoch milliseconds, compare it as an instant, normalise it to UTC before
  custody, and preserve that instant thereafter. Equivalent source offsets are not different data.
- ReceivedAt is delivery metadata stamped once immediately before **each** Kafka publication
  attempt. If a Kafka acknowledgement is lost and the gateway retries the same MessageId,
  ingestion may stamp a later value. It is not part of payload identity; the first durable
  Postgres row wins.
- DetectedAt is minted once with TimeProvider when detection creates the decision. Store that exact
  value on the transaction, use it as outbox CreatedAt, and copy it into the outbox payload; do not
  call the clock twice.

Use TimeProvider for new application-controlled timestamps and time-dependent tests. Normalise UTC
before writing timestamptz.

### Money and currency

- AmountMinor is a positive C# long, protobuf int64, SQLite INTEGER, and Postgres bigint.
- Define one named upper bound and enforce it at gateway admission, ingestion, Kafka-consumer
  validation, and with a Postgres check constraint. Use 100_000_000_000 minor units as the v1
  ceiling unless a smaller documented ceiling is selected.
- Use checked integer arithmetic for sums. Redis sums use Redis integer commands, never Lua
  floating-point addition.
- Currency is the literal `CAD` at every v1 boundary. Put that accepted set in shared contract code;
  gateway, ingestion, detection, generator, tests, and policy must not carry independent lists.
  Adding a currency is a coordinated contract/config/policy change. Still model Currency explicitly
  and validate the uppercase three-letter representation so the design does not bake in cents.
- Do not assume every currency has two fractional digits. The UI formats with currency metadata and
  Intl.NumberFormat, not an unconditional divide-by-100 helper.
- Keep JSON amounts within JavaScript's safe-integer range and require Number.isSafeInteger in the
  dashboard guard. If the accepted ceiling later exceeds that range, change the browser contract to
  a decimal string first.

The canonical monetary value stays long. ML.NET may receive a derived float feature, but that
feature is not money and is never stored or totalled as a financial amount.

### Protobuf

Rename src/Protos/telemetry.proto to src/Protos/transactions.proto and implement this semantic shape:

~~~proto
syntax = "proto3";

package txmonitoring.ingestion.v1;
option csharp_namespace = "TxMonitoring.Ingestion.V1";

import "google/protobuf/timestamp.proto";

service TransactionIngestion {
  rpc UploadTransactions (UploadTransactionsRequest) returns (IngestResponse);
}

message UploadTransactionsRequest {
  repeated TransactionRecord transactions = 1;
}

message TransactionRecord {
  string message_id = 1;
  string account_id = 2;
  int64 sequence_number = 3;
  google.protobuf.Timestamp occurred_at = 4;
  int64 amount_minor = 5;
  string currency = 6;
  string traceparent = 7;
}

message IngestResponse {
  bool complete = 1;
  repeated string accepted_message_ids = 2;
  repeated string rejected_message_ids = 3;
}
~~~

Complete means every submitted record was classified accepted or permanently rejected. It is false
when transient, internal, or Kafka failures leave one or more IDs deliberately unnamed and still in
gateway custody. `accepted_message_ids` means “Kafka acknowledged custody”; it is never a payment
approval or authorisation decision.

Replace the current regex test, which pins only response tags 2 and 3, with descriptor assertions
for the service, RPC, every message field, number, cardinality, and scalar/message type.

Delete the fragile “one reading is exactly 142 bytes” arithmetic. Protobuf varints and bounded
strings vary in size. Define shared field and MaxBatchSize limits, explicitly set the server's
MaxReceiveMessageSize from a validated setting, and make the uploader's bound smaller than that
limit. A CalculateSize() test for the worst valid batch must use those same constants; add live
request/response limit tests instead of relying on the framework's current default.

### HTTP and Kafka JSON

The generator-to-gateway JSON body is TransactionSubmission with the same six business fields as
TransactionRecord. W3C trace context remains an HTTP header captured by the gateway.

The transaction-events value is a closed, versioned TransactionEvent containing exactly:

- SchemaVersion = 1;
- MessageId, AccountId, SequenceNumber, and OccurredAt;
- ReceivedAt;
- AmountMinor and Currency.

Use explicit camel-case wire names and one shared serializer configuration. Require constructor
members on deserialisation. Golden JSON tests pin names, casing, required members, and schema
version. A shared C# record catches repository-local changes at compile time, but retained Kafka
JSON remains a runtime contract.

Key Kafka events by AccountId. This guarantees only per-partition/per-account ordering, not global
ordering. Changing partition count is a controlled, drained operation because key assignment can
change.

The risk-alerts payload is a closed, versioned RiskAlert containing exactly:

- SchemaVersion, MessageId, AccountId, SequenceNumber, OccurredAt, ReceivedAt, and DetectedAt;
- AmountMinor and Currency;
- integer RiskScore and ReviewRecommended;
- Reasons in fixed rule order, each with Code, Metric, ObservedValue, Threshold, and Points;
- VelocityStatus and nullable Velocity evidence;
- detector IsSpike, finite RawScore, finite PValue, HistoryCount, and DetectorVersion;
- friendly PolicyVersion and content-derived PolicyConfigurationHash.

Velocity evidence contains Count1m and Amount5mMinor. Encode both values as invariant base-10 JSON
strings: an aggregate can exceed JavaScript's safe integer even when one AmountMinor cannot. Parse
both with checked `long`/`BigInt` conversions. SequenceNumber and the individual AmountMinor remain
JSON numbers because their explicit maxima are JavaScript safe. Trace context stays in Kafka headers
rather than either retained JSON value.

Do not add convenient fields ad hoc. Changing either JSON shape or meaning requires a schema-version
decision and updated golden fixtures.

Human-readable phrases come from a deterministic map keyed by reason code, preferably in the UI.
Never publish free-form AI advice.

### Kafka topic policy

- `transaction-events`: key by AccountId, `cleanup.policy=delete`, and bounded retention. The key
  selects one partition for an account; ordering additionally depends on a single ordered producer
  path per account, FIFO gateway drain, produce order, producer idempotence, and retry behavior.
- `risk-alerts`: key by MessageId and use bounded `cleanup.policy=delete`. Postgres is the decision
  record. Do not retain the existing compacted-by-account configuration, which silently collapses
  an account's alert history.
- `transaction-events-dlq`: key by stable source identity (topic/partition/offset), use delete
  retention longer than the source topic, and retain source coordinates/exception category in its
  envelope without unbounded exception text.

Apply the same choices in Compose, E2E, Helm, Terraform, and docs. The single-broker local demo uses
replication factor 1 and does not prove broker-loss durability. Production-shaped templates should
declare replication factor 3, `min.insync.replicas=2`, and producers with `Acks.All`, while remaining
honest that no production cluster is exercised here.

Every Web API replica uses a new distinct group with `AutoOffsetReset=Latest`; test this explicitly.
Using Earliest with an ephemeral group would replay retained alerts and contradict the latest-only
dashboard claim.

## Naming map

### Projects and solution

| Current | Target |
| --- | --- |
| Industrial.Shared | TxMonitoring.Shared |
| Industrial.Sensor.Emulator | TxMonitoring.Feed.Emulator |
| Industrial.Sensor.EdgeGateway | TxMonitoring.Capture.Gateway |
| Industrial.Ingestion.Api | TxMonitoring.Ingestion.Api |
| Industrial.Diagnostics.Worker | TxMonitoring.Detection.Worker |
| Industrial.Web.Api | TxMonitoring.Web.Api |
| Industrial.Web.Dashboard | TxMonitoring.Web.Dashboard |
| Industrial.Data.ML | TxMonitoring.ML.DetectorBuilder |
| tests/Industrial.Diagnostics.Tests | tests/TxMonitoring.Detection.Tests |
| tests/Industrial.EdgeGateway.Tests | tests/TxMonitoring.Capture.Tests |
| tests/Industrial.Ingestion.Tests | tests/TxMonitoring.Ingestion.Tests |
| tests/Industrial.Web.Tests | tests/TxMonitoring.Web.Tests |
| IndustrialPlatform.slnx | TxMonitoringPlatform.slnx |

Update directories, csproj names, ProjectReference paths, namespaces, assemblies, solution entries,
test namespaces, InternalsVisibleTo, Docker COPY/entrypoint paths, launch settings, HTTP examples,
CI, and commands. Never edit generated bin/obj protobuf output.

### Types by lifecycle role

Do not globally replace all TelemetryReading occurrences with one ambiguous Transaction type.

| Current role/type | Target |
| --- | --- |
| HTTP TelemetryDto | TransactionSubmission |
| protobuf TelemetryReading | TransactionRecord |
| UploadTelemetryRequest | UploadTransactionsRequest |
| TelemetryResponse | IngestResponse |
| TelemetryIngestion / UploadTelemetry | TransactionIngestion / UploadTransactions |
| Kafka TelemetryEnvelope | TransactionEvent |
| Kafka TelemetryAlertEnvelope | RiskAlert |
| gateway TelemetryRecord | BufferedTransaction |
| QuarantinedTelemetryRecord | QuarantinedTransaction |
| detection entity TelemetryReading | MonitoredTransaction |
| TelemetryConsumerWorker | TransactionConsumerWorker |
| TelemetryReadingValidator | TransactionRecordValidator |
| TelemetryAdmissionValidator | TransactionAdmissionValidator |
| ModelEngine | AmountSpikeDetector |
| TelemetryData | AmountObservation |
| AnomalyPrediction | SpikePrediction |
| MachineHealthResult | AmountSpikeResult |
| SensorMetrics / EdgeMetrics / WorkerMetrics | FeedMetrics / CaptureMetrics / DetectionMetrics |
| TelemetryHub / ITelemetryClient | RiskHub / IRiskClient |
| TelemetryChart / useTelemetry | AmountChart / useTransactionFeed |

Keep AlertOutboxMessage, BufferDepth, BufferMutationGate, DeadLetterPublisher, and generic
infrastructure names where they remain accurate.

### Runtime names

| Current | Target |
| --- | --- |
| sensor-emulator | feed-emulator |
| edge-gateway | capture-gateway |
| diagnostics-worker | detection-worker |
| telemetry-events | transaction-events |
| telemetry-events-dlq | transaction-events-dlq |
| telemetry-alerts | risk-alerts |
| /api/local/telemetry | /api/local/transactions |
| /api/debug/telemetry | /api/debug/transactions |
| /telemetryHub | /riskHub |
| SignalR telemetry_events | transaction_events |
| SignalR telemetry_alerts | risk_alerts |
| industrial_db | tx_monitoring |
| Postgres TelemetryReadings | Transactions |
| SQLite TelemetryRecords | BufferedTransactions |
| SQLite QuarantinedTelemetryRecords | QuarantinedTransactions |
| AlertOutboxMessages | unchanged |

Keep the public project/container name Feed.Emulator/feed-emulator from the map. Internally rename
EmulatorWorker/EmulatorOptions to TransactionGeneratorWorker/TransactionGeneratorOptions and
DeviceCount to AccountCount; do not oscillate the public surface between “emulator” and “generator.”
Rename Cloud to Ingestion and ConnectionStrings:IndustrialDb to
ConnectionStrings:TxMonitoringDb. Retain the useful generic Gateway, Buffer, Uploader, Consumer,
Outbox, Kafka, OTel, TransportSecurity, and CORS section names. Add validated Redis, Velocity, and
RiskPolicy options.

### Metrics and traces

| Current | Target |
| --- | --- |
| sensor.telemetry.acquired | feed.transaction.generated |
| sensor.telemetry.dropped | feed.transaction.dropped |
| sensor.telemetry.sent | feed.transaction.delivery_accepted |
| sensor.telemetry.rejected | feed.transaction.rejected |
| sensor.telemetry.failed | feed.transaction.failed |
| sensor.telemetry.delivery_dropped | feed.transaction.delivery_dropped |
| sensor.telemetry.delivery.duration | feed.transaction.delivery.duration |
| sensor.channel.depth | feed.channel.depth |
| edge.telemetry.received | capture.transaction.received |
| edge.telemetry.duplicate | capture.transaction.duplicate |
| new | capture.transaction.idempotency_conflict |
| edge.telemetry.uploaded | capture.transaction.uploaded |
| edge.telemetry.shed | capture.transaction.shed |
| edge.telemetry.malformed | capture.transaction.malformed |
| edge.telemetry.rejected | capture.transaction.rejected |
| edge.upload.failures / edge.upload.duration | capture.upload.failures / capture.upload.duration |
| edge.queue.depth | capture.queue.depth |
| edge.oldest.message.age | capture.oldest.transaction.age |
| edge.cloud.reachable | capture.ingestion.reachable |
| worker.telemetry.persisted | detection.transaction.persisted |
| worker.telemetry.consumed | detection.transaction.consumed |
| worker.telemetry.processing_failures | detection.transaction.processing_failures |
| worker.telemetry.dead_letter_published | detection.transaction.dead_letter_published |
| worker.telemetry.dead_letter_failures | detection.transaction.dead_letter_failures |
| worker.telemetry.duplicate | detection.transaction.duplicate |
| worker.telemetry.lag | detection.transaction.lag |
| worker.anomaly.detected | detection.transaction.flagged |
| worker.alert.failures / worker.alert.published | detection.alert.failures / detection.alert.published |
| worker.alert.outbox.pending | detection.alert.outbox.pending |
| worker.ai.failures | remove |
| web.messages.relayed / web.relay.failures | unchanged |

Add bounded-cardinality velocity metrics:

- detection.velocity.unavailable;
- detection.velocity.conflict;
- detection.velocity.rebuilds and detection.velocity.rebuild_failures;
- an operation-duration histogram tagged by a closed outcome set.

Also add `detection.policy.evaluated`, `detection.policy.rule_fired`, and
`detection.transaction.review_recommended`, with only stable `rule_id`/outcome tags. These
instruments must back the promised policy/review Grafana panels rather than leaving those panels to
infer behavior from logs.

Rename edge.upload, telemetry.process, and telemetry.relay activities to capture.upload,
transaction.process, and transaction.relay. Rename equipment.id trace tags to account.id and map
the current `edge.batch.size`, `edge.batch.linked_traces`, `edge.batch.bytes`,
`edge.batch.accepted`, `edge.batch.rejected`, and `edge.upload.attempt` attributes to the
`capture.batch.*`/`capture.upload.attempt` vocabulary. Synthetic account/message IDs may appear in
structured logs and traces, but never in metric labels. Never log entire event bodies.

Update every PromQL string, Grafana panel, Prometheus rule, job/service filter, filename, UID/tag,
description, and runbook reference. OpenTelemetry's Prometheus exporter changes dotted instrument
names to underscore metric names; update both sides deliberately.

## Reliability contract to preserve

Do not collapse these into the vague claim “the pipeline is at least once.” Each boundary has a
different owner and failure behavior.

1. The generator mints MessageId, SequenceNumber, and OccurredAt. No downstream service regenerates
   them.
2. Gateway admission validates deterministic bounded content before reserving capacity.
3. BufferDepth.TryReserve remains the atomic MaxDepth decision. A periodic count never overwrites it.
4. Receive and settlement transitions stay under BufferMutationGate.
5. SQLite remains WAL with synchronous=FULL. The gateway returns 202 only after commit and does not
   cancel an admitted durable write because the HTTP client disconnected.
6. A same-ID/same-payload retry is idempotent while live and after successful settlement. A retry
   matching a quarantined terminal rejection returns the same bounded terminal rejection, not 202.
   Same ID with different immutable content is a conflict and never overwrites the first event.
7. The uploader drains oldest first in bounded unary gRPC batches and deletes only IDs explicitly
   named accepted or permanently rejected.
8. IDs in neither response list remain queued. Unknown, overlapping, or contradictory response IDs
   delete nothing.
9. Ingestion puts only deterministic validation failures in rejected_message_ids. Kafka, producer,
   and internal failures leave IDs unnamed.
10. Kafka producers retain Acks.All, idempotence enabled, auto-topic creation disabled, and require
    PersistenceStatus.Persisted before acknowledging custody.
11. Detection manually commits only after Postgres succeeds, an exact duplicate is verified, or a
    poison/conflicting source record is durably acknowledged by the DLQ.
12. A retryable failure after Consume advances position seeks to the exact failed offset. If seek
    fails, stop rather than risk committing past it.
13. Postgres uniquely indexes MessageId. “Effectively once” applies only to one identical persisted
    monitoring row; Kafka and alert publication remain at-least-once.
14. The monitored transaction and optional outbox row commit in one Postgres transaction.
15. The outbox uses FOR UPDATE SKIP LOCKED, waits for a durable Kafka acknowledgement, and then marks
    published. An ambiguous acknowledgement can duplicate an alert; MessageId is its dedupe key.
16. Each Web API replica has a distinct Kafka group because SignalR clients are process-local. The
    UI is a latest-only, best-effort view with reconnect and bounded dedupe, not an audit store.

Settled-ID retention and rejection quarantine stay bounded. A deterministic rejection is explicit
permanent loss from the live monitoring pipeline, not successful delivery. Preserve the payload and
rejection metadata before deleting the live row. Document that production regulated monitoring
would need durable remediation, operator acknowledgement, controlled replay, access control, and
longer retention.

### Required idempotency-conflict hardening

Implement this only after the mechanical port and original reliability suite are green:

- Put one fingerprint implementation in TxMonitoring.Shared. Version 1 encodes, in fixed order: one
  version byte; AccountId as a four-byte big-endian length plus exact UTF-8 bytes; SequenceNumber as
  signed 64-bit big-endian; OccurredAt as signed 64-bit epoch milliseconds; AmountMinor as signed
  64-bit big-endian; and canonical uppercase Currency as length-prefixed UTF-8. Hash those bytes
  with SHA-256. Do not build the input with interpolation, delimiters, culture-sensitive formatting,
  JSON, or database-round-tripped timestamps.
- MessageId selects the identity record and need not be inside the hash. Exclude ReceivedAt,
  DetectedAt, trace context, broker coordinates, and all other delivery metadata. Equivalent
  timestamp offsets therefore produce the same fingerprint.
- Store it on live gateway rows and settled markers; retain it in quarantine.
- Store PayloadFingerprint and FingerprintVersion on the Postgres MonitoredTransaction as well;
  duplicate and unique-race checks compare the stored bytes, not reconstructed EF values.
- Same ID and fingerprint returns 202 for a live/settled event. A quarantined match returns a stable
  terminal 422 with a bounded rejection code and never re-enters the live queue.
- Same ID with a different fingerprint returns 409, increments
  capture.transaction.idempotency_conflict, and preserves the original.
- Detection compares an existing Postgres row before calling it a replay. Differing immutable data
  is a deterministic conflict and goes to the DLQ before source-offset commit.
- Handle the unique-index race by reloading and comparing the winning row.
- Golden vectors pin the encoding and include equivalent-offset timestamps. The hash detects reuse;
  it is not authentication. Gateway absorption lasts only as long as bounded settled/quarantine
  retention; this is not a permanent global idempotency registry.

## Detection and risk policy

### Amount spike detector

Keep the existing online ML.NET IID spike behavior, but name it accurately.

- Isolate state by (AccountId, Currency).
- Warm lazily from recent committed rows ordered by DetectedAt and Id, which reproduces detector
  processing order even when source clocks jump.
- Invalidate after any failed Postgres transaction or duplicate race that occurred after state
  advanced.
- Keep a bounded least-recently-used state cache.
- Regenerate the artifact and persist its SHA-256 as DetectorVersion.
- Never describe DetectIidSpike().Fit() as supervised training. Fit establishes configuration and
  schema; each reference window is populated online.

Create one shared, versioned transformation from AmountMinor to a bounded floating feature for the
artifact builder and runtime. log(1 + AmountMinor) is a suitable v1 feature for skewed amounts. The
original long remains the only monetary value.

Persist raw detector score, p-value, history count, and version with honest names. Do not call the
raw score a probability. Do not allow the spike rule to fire before HistoryLength is warm.
Real transaction amounts are commonly heavy-tailed, seasonal, and conditional on channel/customer
behavior; an online IID spike detector ignores all of that. Present it as a pedagogical per-account
baseline that demonstrates keyed state, warm-up, and recovery—not as a fraud model.

### Deterministic v1 policy

Implement a small policy evaluator separate from transport and persistence. It accepts the event,
spike result, and velocity snapshot and returns an immutable decision.

Use startup-validated, currency-specific thresholds and stable reason IDs. These CAD values are
illustrative thresholds selected to exercise named demo scenarios; they are not derived from loss
data and must not be called “reasonable” or production defaults:

| Rule ID | Demo condition | Points |
| --- | --- | ---: |
| amount.absolute_high | AmountMinor >= 250_000 | 35 |
| amount.account_spike | warmed IID detector flags amount | 40 |
| velocity.count_1m | count including current event >= 20 | 40 |
| velocity.amount_5m | sum including current event >= 1_000_000 | 40 |

The absolute/spike rules are correlated amount signals, and the count/sum rules are correlated
velocity signals. Sum within each signal family but cap each family at 70, then add the two family
contributions with checked arithmetic and cap RiskScore at 100. Set ReviewRecommended when score is
at least 60. This acknowledges correlated evidence while still allowing a combined absolute-plus-
account-spike signal to alert when Redis is down, or a combined count-plus-sum burst to alert without
an amount spike. The generator must produce deterministic steady, high-amount, account-spike, burst,
and combined alert scenarios. Its default steady cadence must remain below every velocity threshold
after warm-up.

Persist every fired reason even below the alert threshold, in the table's fixed rule order, with its
metric, invariant observed value, threshold, and awarded points. Canonicalise the complete effective
policy configuration (currencies, transforms, thresholds, points, family caps, warm-up, and alert
threshold), SHA-256 hash it, and persist that full PolicyConfigurationHash. A friendly PolicyVersion
such as risk-policy-v1 is useful but cannot stand alone because operators can change configuration
without changing a label. A test must prove any effective policy change changes the hash.

### Postgres decision shape

Transactions contains the immutable event plus locally reproducible decision evidence. This is not
a tamper-evident or regulatory audit trail:

- UUIDv7 internal Id and unique MessageId;
- AccountId, SequenceNumber, OccurredAt, ReceivedAt, DetectedAt;
- AmountMinor and Currency;
- integer RiskScore, ReviewRecommended, and ordered structured Reasons;
- detector raw score, p-value, history count, and version;
- nullable Count1m and Amount5mMinor velocity evidence;
- VelocityStatus, PolicyVersion, and PolicyConfigurationHash;
- PayloadFingerprint and FingerprintVersion.

Use explicit EF/Postgres checks for amount bounds, supported currency shape, 0–100 risk score, and
non-negative counts. Add:

- unique MessageId;
- (AccountId, Currency, DetectedAt, Id) for deterministic detector warm-up order;
- (AccountId, Currency, OccurredAt) for velocity reconstruction;
- the existing pending-outbox scan and unique outbox MessageId indexes.

Only TxMonitoring.Detection.Worker owns production migrations. Preserve a single migration-owner/job
story; never let every production replica race migrations. Compose may migrate for the demo, and
readiness verifies the current model.

## Redis velocity design

### Window semantics

For each (AccountId, Currency), return only the evidence the v1 policy consumes:

- Count1m: event count over one minute;
- Amount5mMinor: summed AmountMinor over five minutes.

For event T, the interval is (T - window, T]: exclude the lower boundary and include the current
event. Use OccurredAt, not ingestion time, so draining a gateway outage does not manufacture a
current burst from historical events.

The v1 feed **expects**, but does not blindly assume, monotonic event time per account. AccountId
partitioning is only one prerequisite; the generator's per-account production, FIFO gateway drain,
produce order, producer idempotence, and partial-ack retry behavior must preserve it in the exercised
path. Maintain a per-key event-time watermark. An event at T >= watermark is on-time (equal
timestamps are allowed), advances the watermark, and receives the snapshot
`(watermark - window, watermark]` at millisecond resolution. An event at T < watermark returns
OutOfOrder with null velocity evidence and does not mutate velocity state; detection still persists
and scores it from non-velocity signals. Do not pretend this layout can reconstruct an exact
historical window for a late event after state was trimmed.

Apply the now-relative future-skew check at gateway admission before custody. Ingestion permanently
rejects only representation and immutable-bound failures; “future relative to now” can become valid
on retry and therefore must not enter rejected_message_ids. Detection may compare OccurredAt with
that Kafka record's fixed ReceivedAt for a deterministic poison decision. Do not reject a
chronologically ordered old event merely because a real gateway outage delayed processing.

### Atomic, idempotent API

Expose an interface equivalent to:

~~~text
IVelocityStore.ObserveAsync(AccountId, Currency, MessageId, AmountMinor, OccurredAt)
  -> VelocitySnapshot(Status,
      Count1m, Amount5mMinor)
~~~

VelocityStatus is a closed enum: Available, Unavailable, OutOfOrder, and Rebuilding. Available
requires both evidence fields; every other state requires both to be null. A Redis payload mismatch
is cache corruption, not authoritative proof of an origin conflict: meter it, invalidate/rebuild,
and persist Unavailable. Only gateway or Postgres durable fingerprints can justify 409/DLQ.

Use one singleton StackExchange.Redis ConnectionMultiplexer. Give an observation a short bounded
timeout. Configure Kafka MaxPollIntervalMs above the documented worst bounded database, rebuild,
and processing/retry budget. Translate connection failures, timeouts or ambiguous completion,
script errors, malformed/partially evicted state, capacity/overflow errors, and rebuild failures to
Unavailable inside the adapter after logging/metering. Never swallow caller cancellation, and never
let a Redis exception reach generic Kafka seek/retry handling.

A pipeline is not atomic, and a sorted set of timestamp/message ID cannot produce amount sums. Use
one checked-in, SHA-loaded Lua script; do not leave Lua-versus-Functions as an implementation-time
choice. A defensible exact layout per cache generation has:

- a seen hash mapping MessageId to canonical timestamp|amount;
- one timestamp-sorted set retaining the five-minute horizon;
- one integer five-minute sum key;
- a watermark key;
- a schema/ready marker and a short-lived token-owned rebuild lease.

Every key for one account/currency uses the same Redis Cluster hash tag based on a stable SHA-256 of
the opaque account/currency key. Do not put raw account IDs in key names.

One atomic observation must:

1. Return without incrementing for an identical MessageId/timestamp/amount replay.
2. Return Conflict without mutation when a known ID has different immutable data.
3. Add a new ID to the five-minute ZSET and update the sum with Redis integer INCRBY. Pass amount as
   a decimal string; never convert monetary values with Lua `tonumber`.
4. Remove entries at the five-minute lower boundary, look up each exact amount, decrement with
   Redis integer DECRBY, and then remove its seen entry. Derive Count1m with ZCOUNT against the same
   watermark and interval convention.
5. Advance the watermark monotonically.
6. Refresh a TTL longer than five minutes plus the configured rebuild margin on every related key.
7. Fetch/return a large sum as a Redis bulk string, not a Lua numeric value, and parse counts/sums
   with checked C# long conversion.

Set a validated per-key five-minute member ceiling (10,000 for v1) to bound script work and prove
`ceiling * maximum AmountMinor` fits signed 64-bit. Crossing it degrades velocity; it never rejects
or retries the source transaction. Configure an explicit Redis maxmemory budget and `noeviction` so
multi-key state is not silently made partial; OOM likewise degrades. TTL must exceed five minutes
with recovery margin. Document that account cardinality is bounded only by the memory budget and
that this exact-per-event design favours clarity at portfolio load. A much higher-volume system
would evaluate bucketed counters or a dedicated stream processor.

### Postgres/Redis ordering and repair

There is intentionally no distributed transaction between Redis and Postgres:

1. Validate the Kafka event and check Postgres for MessageId.
2. Advance the amount detector and attempt atomic Redis observation.
3. Evaluate policy.
4. Commit decision and optional outbox row together in Postgres.
5. Commit the Kafka offset.

If Redis succeeds and Postgres fails, invalidate both the in-memory detector and that
account/currency Redis state. Invalidation is best effort; Redis MessageId idempotency prevents
double-counting on retry even if cleanup fails.

Use a process/partition-assignment cache epoch in the Redis namespace. Mint a new epoch on worker
start and on each Kafka partition assignment. A newly assigned owner therefore cannot trust a stale
key left by a crashed or rebalanced owner: its first event for an account/currency lazily rebuilds a
fresh generation from Postgres, while old generations expire. During a live outage, also retain a
bounded process-local dirty set so the current owner rebuilds before reuse. The new epoch is what
makes recovery safe when that in-memory set is lost.

A rebuild first finds the latest eligible committed OccurredAt watermark, then loads eligible rows
in `(watermark - 5m, watermark]` in deterministic order. Rows previously marked OutOfOrder remain
excluded; rows processed while Redis was Unavailable/Rebuilding are included so future decisions
recover their context. Build into a new generation under a token-owned lease. Refresh the lease
while working, release it only with compare-and-delete, and publish the ready/schema marker
atomically after every structure is complete. Observe must never read a partial generation; it
returns Rebuilding with null evidence when another builder owns the lease or the bounded rebuild
budget expires.

If Redis is unavailable, evaluate remaining rules, persist nullable velocity fields with
VelocityStatus=Unavailable, and continue. If rebuild cannot finish in budget, persist Rebuilding and
continue. Recovery repairs state used by **future** decisions; it does not retroactively rescore or
republish outage-period decisions. Cache accuracy may temporarily degrade; durable event custody
may not.

## Component work packages

### Feed emulator

- Rename device/equipment concepts to synthetic accounts.
- Generate positive AmountMinor and supported currency without floating-point money.
- Produce repeatable normal, spike/high-amount, and burst patterns from sequence. Random noise may
  exist only within documented bounds. Prove the default steady scenario stays below the review
  threshold after detector warm-up; the current two-second-per-account cadence would already exceed
  a 20-per-minute rule and must change.
- Preserve UUIDv7 IDs, per-run sequences, event time, bounded Channel, non-blocking acquisition,
  exact-202 success, bounded retry/jitter, and circuit breaker.
- Keep channel-drop and delivery-drop metrics separate.
- Move workers and DTOs out of the large Program.cs; leave Program.cs as composition root.
- Never call this a processor, terminal, account system, or durable source.

### Capture gateway

- Rename routes, entities, raw SQLite, logs, traces, health descriptions, and inspection JSON.
- Store AmountMinor as INTEGER and Currency as bounded text.
- Preserve WAL/FULL, BufferMutationGate, BufferDepth.TryReserve, the non-cancelled post-admission
  commit, oldest-first drain, partial acknowledgement, backoff, and mTLS.
- Preserve live, settled-ID, and quarantine states and their atomic transitions.
- Preserve the 16 KiB pre-model-binding HTTP request-body cap as well as field validation; ignored
  JSON and whitespace must not bypass a byte bound. Test both limits.
- Return a stable terminal 422 for an identical quarantined retry and 409 for conflicting ID reuse.
- Add payload-fingerprint behavior only after the original port tests are green.
- Readiness depends on local durable-queue writability, not ingestion or Redis.
- Initialise a fresh target schema; do not imply EnsureCreated can rename an old industrial table.

### Ingestion API

- Regenerate renamed protobuf client/server contracts.
- Validate GUID, account shape, bounded positive sequence, timestamp representation/precision,
  positive/bounded amount, CAD, trace context, batch count, and serialized size. Do not use a
  now-relative check as a permanent ingestion rejection.
- Produce TransactionEvent keyed by AccountId and stamp ReceivedAt once per publication attempt.
- Start valid produces before awaiting so librdkafka can batch; classify each result independently.
- Name accepted only after PersistenceStatus.Persisted.
- Name rejected only for deterministic validation.
- Leave producer failures and ambiguous statuses unnamed.
- Remove the debug HTTP-to-Kafka bypass. One production-shaped ingestion path makes the custody
  claim and tests much easier to defend; deterministic scenarios enter through the feed/gateway.
- Preserve mTLS client-certificate policy and its exact scope.

### Detection worker

- Keep required-member JSON validation and bounded poison handling before state mutation.
- Normalise timestamps and continue trace context from Kafka.
- Verify exact duplicates and explicitly handle conflicting ID reuse.
- Run isolated amount detection, attempt Redis velocity, evaluate policy, and persist the complete
  decision-evidence snapshot.
- Add an outbox row only when ReviewRecommended.
- Commit transaction and outbox in one SaveChanges transaction.
- Preserve unique-race verification, state invalidation, exact seek-on-failure, and jittered retry.
- Keep malformed JSON, tombstones, invalid runtime contracts, and ID conflicts on the
  DLQ-before-source-commit path.
- Redis failure stays inside velocity evaluation and never reaches generic retry/seek logic.

### Alert outbox

- Remove enrichment and publish the already-complete deterministic RiskAlert.
- Publish risk-alerts with MessageId as key and bounded delete retention, never compaction by
  AccountId.
- Preserve FOR UPDATE SKIP LOCKED, pending-row retention, bounded published history, attempt/error
  audit, Acks.All, idempotent producer, and persisted acknowledgement checks.
- Never expire pending work.
- Keep publication at-least-once and downstream MessageId dedupe.

### Web API and dashboard

- Rename hub, typed client methods, topics, DTOs, guards, state, components, copy, and metadata.
- Keep one distinct Kafka group per Web API replica; a shared group requires a SignalR backplane.
- Keep AutoOffsetReset.Latest for each new replica group and test that startup does not replay
  retained alert history.
- Plot amount per account against OccurredAt. Never draw one line across mixed accounts.
- Format AmountMinor safely with Currency, validate browser-safe individual values, and parse
  velocity count/sum strings with checked BigInt handling.
- Show account, amount, currency, score, reasons, time, and degraded velocity status. Never label an
  alert as fraud, decline, or blocked.
- Preserve infinite reconnect, malformed-payload isolation, and bounded duplicate windows.
- Document that a disconnected dashboard misses live events and has no historical backfill.

### Runtime and repository surfaces

Update more than src:

- docker-compose.yaml and tests/e2e/docker-compose.e2e.yaml;
- every Dockerfile;
- Makefile, Tiltfile, prod.Tiltfile;
- mise.toml and .env.example;
- .github/workflows;
- scripts, verify.sql, every .http example (including stale weather examples), and tests/e2e/README;
- chart metadata, values, templates, README, labels/selectors, images, secrets, and routes;
- infra Terraform names, variables, state-key examples, topics, Argo CD parameters, and docs;
- monitoring configs, dashboard filename/content, Prometheus rules, jobs, and descriptions;
- pgAdmin definitions;
- frontend metadata, dashboard README, and generated-output policy;
- README.md, AGENTS.md, TODO.md, resume.md, and relevant docs.

Delete the obsolete industrial `training_data.csv`; generate any detector fixture from a named,
deterministic amount scenario. Replace the Makefile `train` target with a clearly named
`build-detector` target and update every caller.

Add ephemeral Redis to Compose with a healthcheck for visibility, but no persistence volume and no
detection startup/readiness dependency. Configure maxmemory/noeviction and add it to E2E. Helm and
Terraform remain unproven prototypes: configure
an external/managed Redis endpoint and security TODO rather than claiming a production cache exists.

Add a one-shot production migration Job/owner in Helm and a documented invocation path. Detection
replicas must wait for schema readiness but must never race `MigrateAsync`; only the local Compose
profile may let the single worker migrate for convenience.

AI.md is a generated repository snapshot. Do not hand-edit its embedded copy of every file. Remove
or regenerate it with scripts/AI.sh after the active tree is final, and exclude it from old-name
scans while working.

## Idiomatic .NET/C#/SQL direction

- Keep nullable references and implicit usings. Treat compiler/analyser warnings as errors in CI and
  keep dotnet format verification green.
- Use sealed immutable records for hand-written HTTP/Kafka/alert contracts and decision values.
  Generated protobuf messages remain generated mutable classes. Keep EF entities as
  persistence-only classes with explicit mappings and constraints.
- Keep Program.cs thin. Put workers, options, policies, contracts, and infrastructure in named
  files.
- Use file-scoped namespaces, primary constructors, collection expressions, pattern matching, and
  await using where clearer; do not contort simple code to showcase syntax.
- Use TimeProvider for new time logic. Unit tests advance fake time; never Thread.Sleep. Test actual
  Redis TTL with a bounded real-Redis integration test.
- Pass cancellation tokens to I/O, distinguish shutdown from operational failure, and never swallow
  caller cancellation.
- Keep IDbContextFactory in singleton workers; never capture scoped DbContext in BackgroundService.
- Use EF directly. Add raw SQL only for provider-specific behavior such as SKIP LOCKED or SQLite
  DateTimeOffset limitations.
- Bind typed options, validate data annotations and cross-field invariants at startup, and inject
  IOptions. Validate backoff ordering, TTL versus the five-minute window, policy weights/threshold,
  supported currencies, timeouts, and distinct Kafka topics.
- Never silently supply a deployment fallback for a missing required value.
- Keep Kafka producers and Redis multiplexer singleton; do not create network clients per event.
- Use structured, bounded logs. Never log secrets or full payloads.
- Pin tested dependencies. After behavior is green, centralise versions in
  Directory.Packages.props, align package families, and add global.json for the tested .NET 10 SDK
  feature band/roll-forward. Update every Docker restore layer to copy Directory.Packages.props and
  global.json before restore, and align the SDK image feature band. Do not blindly upgrade to
  “latest.”
- Never hand-edit generated protobuf C#, EF designers/snapshots, chart archives, node_modules, dist,
  bin, obj, or lscache artifacts.

Avoid needless abstractions. The interview value is in visible invariants and failure behavior, not
the number of layers.

## Test plan

Preserve and rename every baseline test by behavior. The source has 42 passing cases; the target
must add tests, so do not pin 42 as the final count.

### Contract and validation

- Descriptor-pin every protobuf service, RPC, field number, cardinality, and type.
- Prove the worst valid batch fits the explicitly configured receive limit, and exercise one request
  on each side of the live byte/count boundary.
- Golden TransactionEvent, RiskAlert, and DLQ JSON fixtures pin version/casing/required members.
- Prove amount is long end-to-end and reject zero, negative, over-limit, unsafe-browser, and
  overflow cases.
- Keep GUID/account/sequence/timestamp/currency/trace bounds consistent across all three validators.
- Pin fingerprint golden vectors; ReceivedAt/trace changes remain identical, equivalent timestamp
  offsets hash identically, and any immutable origin-field change conflicts.
- Mixed ingestion batches classify accepted, rejected, and unnamed IDs correctly. Transient or
  ambiguous producer results are never rejected.
- Prove a now-relative future check is not an ingestion permanent rejection.

### Gateway

- Exact concurrent MaxDepth and no underflow.
- Failed insert releases reservation; commit does not.
- Identical retry returns 202 in live/settled states and the stable 422 terminal outcome in
  quarantine.
- Conflicting ID reuse returns 409 and preserves original data.
- Settlement atomically creates markers/quarantine and removes live rows; depth releases after commit.
- Unknown/overlapping/contradictory acknowledgements delete nothing.
- Retention stays bounded.
- Restart restores queue and depth.

### Detection, policy, and Postgres

- Account/currency states are isolated, cached, warmed, and invalidated on DB failure.
- Cold history cannot trigger the spike rule.
- Builder/runtime amount transforms match golden values.
- Policy score/reasons are deterministic, capped, versioned, and reproducible.
- Any effective threshold, point, family-cap, or transform change changes PolicyConfigurationHash;
  reason order is stable.
- EF maps money/sums to bigint, time to timestamptz, and creates required checks/indexes.
- Identical replay yields one row and one logical alert; conflicting replay goes to DLQ.
- Same immutable event with another ReceivedAt is an identical replay and retains the first durable
  row.
- Transaction/outbox commit and rollback together.
- Postgres failure seeks the exact Kafka offset and invalidates advanced state.
- Poison source offset commits only after persisted DLQ acknowledgement; failed/ambiguous DLQ
  publication rewinds.

### Redis

Put real-Redis integration tests in a self-contained Docker-backed Testcontainers fixture pinned to
the Compose Redis major version; do not silently skip them. Use TimeProvider for logical window
tests and bounded polling only for server TTL behavior. Unit-test the unavailable counter with a
MeterListener; E2E proves persistence/offset/DLQ behavior and need not scrape an absent collector.

- Exact lower/upper boundaries for Count1m and Amount5mMinor at millisecond resolution.
- Exact integer counts/sums and checked overflow.
- Account and currency isolation.
- Replay does not double-count; conflicting data does not mutate.
- Concurrent unique writes lose no updates.
- TTL exceeds the five-minute window/rebuild margin and real Redis expires idle state.
- Related keys share a Redis Cluster hash slot.
- Redis success plus Postgres failure is repairable and retry-safe.
- During Redis outage every valid event persists, offsets progress, fields are Unavailable not zero,
  metric rises, and nothing reaches DLQ because of Redis.
- A combined amount-only signal can still publish an alert while Redis is unavailable.
- Cold start with Redis down works; mid-run outage followed by worker crash/rebalance cannot reuse a
  stale prior epoch.
- Recovery lazily rebuilds dirty state from authoritative Postgres five-minute history; outage rows
  affect future snapshots but are not retroactively rescored.
- Equal timestamps are on-time; an older event is OutOfOrder with null evidence and no state
  mutation.
- Partial batch acknowledgement/retry preserves the exercised monotonic event-time assumption.

### UI and E2E

- Guards reject unsafe amount and malformed shapes without killing SignalR.
- Maximum aggregate strings round-trip exactly through Kafka JSON, SignalR, and TypeScript BigInt.
- Bounded dedupe suppresses replay.
- Reasons and degraded status render deterministically.
- Existing E2E still proves mTLS rejection, ingestion outage, gateway restart, queue drain, Postgres
  retry, unique persistence, DLQ ordering, and outbox recovery.
- Add deterministic spike alert, velocity burst, Redis cold/mid-run outage and recovery, worker
  restart/reassignment, quarantined retry, and ID conflict.
- A newly generated Web API group starts Latest and does not replay retained alerts.
- Leave true Kafka ACK-after-write fault injection in TODO until actually implemented; ordinary
  retry does not prove that ambiguity.

## Observability, health, and security

Grafana should answer:

- Is the feed dropping before custody?
- Is capture backlog growing and how old is its oldest event?
- Are gateway rows rejected or conflicting?
- Is detection consuming, persisting, retrying, or blocked by poison?
- Is velocity unavailable/rebuilding and are decisions degraded?
- Which fixed rule IDs fire and how many events require review?
- Is the outbox backlogged?
- What are p50/p95/p99 end-to-end event-time lags?

Health semantics:

| Component | Readiness meaning |
| --- | --- |
| capture gateway | local SQLite queue is writable |
| ingestion | required Kafka topic/partitions are available |
| detection | Kafka topics/assignment, Postgres, and migrations are available |
| Web API | Kafka topics and consumer assignment are available |
| Redis | never gates readiness; degradation is metrics/logs |

Keep metric cardinality bounded. rule_id, outcome, and velocity_status are closed sets. AccountId,
MessageId, exception text, and raw URLs are not metric labels.

Security boundaries:

- use opaque synthetic account IDs only; no PAN, CVV, names, real bank IDs, tokens, or production
  secrets in events, logs, traces, fixtures, screenshots, DLQ, or quarantine;
- preserve the private dev CA, gateway client cert, ingestion server cert, EKU checks, custom root,
  and fingerprint allowlist on gateway-to-ingestion mTLS;
- explicitly state that generator-to-gateway, Kafka, Postgres, Redis, observability ingestion, and
  local operator UIs remain plaintext/unauthenticated inside Compose;
- keep Redis TLS/auth, workload credentials, rotation, and network policy in production TODO;
- state that the dashboard lacks production authentication and case-level access control;
- state that real DLQ/quarantine data needs access control, redaction, retention, and audited replay.

Never claim PCI DSS or AML compliance from this design. Removing Gemini reduces data egress; it does
not itself establish compliance.

## Documentation and interview handoff

README must explain scope/non-goals, data flow, pre/post-202 boundary, amount/currency, input facts
versus decisions, detector/policy limits, Redis degradation/rewarm, deterministic scenarios,
failure demo, health/security boundary, and Compose as the only exercised runtime.

Reconcile docs/Fintech.md, Bug-AcquisitionCoupling.md, Networking.md, Kafka.md, Observability.md,
UploadFailures.md, database notes, Security.md, and ML/MLOps.md. Preserve useful reasoning but
correct stale facts. TODO contains only open work. Resume contains only demonstrated claims.

| Defensible claim | Evidence | Caveat |
| --- | --- | --- |
| Durable store-and-forward after acceptance | WAL/FULL, 202-after-commit, restart E2E | Feed before 202 is lossy |
| At-least-once plus idempotent persistence | ID ACKs, manual commit/seek, unique MessageId | Not global exactly once; alerts may duplicate |
| Safe poison isolation | quarantine and DLQ-before-commit | No operator replay workflow |
| Atomic decision/alert intent | Postgres row plus outbox transaction | Kafka/UI are at-least-once/best-effort |
| Stateful account monitoring | partition key, isolated detector, velocity | IID and heuristic rules, not trained fraud ML |
| Cache failure isolation | Redis outage E2E and status/metrics | Accuracy degrades |
| Reproducible decisions | evidence, reasons, detector/policy versions | Not a regulatory audit trail; policy is not calibrated |
| Correct money representation | long/int64/INTEGER/bigint | Monitoring data, not ledger balances |
| Event-time awareness | immutable OccurredAt and outage lag | Out-of-order events degrade; this is not a general stream processor |
| Horizontal relay semantics | account partitions and per-replica Web groups | No SignalR backplane/history |
| Scoped workload identity | tested gateway-ingestion mTLS | Other local hops/UIs unsecured |

Never say “AI fraud model,” “fraud detected,” “payment processed,” “no transactions lost,” “exactly
once,” “PCI compliant,” “production Kubernetes,” or “learned model” unless later code makes it true.

## Required execution order

Keep each phase green. Do not mix broad critical-path refactoring into mechanical renames.

### Phase 0 — baseline and characterization

1. Run current restore/build/test/frontend/Compose/EF checks.
2. Inventory names in contents and filenames.
3. Add missing characterization for partial ACKs, unnamed transient IDs, ambiguous Kafka status,
   DLQ-before-commit, DB seek/retry, and outbox atomicity.
4. Identify generated output so tools regenerate it.

### Phase 1 — mechanical identity

1. Rename folders, projects, solution, namespaces, tests, and references.
2. Rename containers, build paths, Make/Tilt/CI paths, and OTel services without changing business
   fields or reliability flow.
3. Build, format, and test; fix only rename fallout.

### Phase 2 — coordinated contracts and storage

1. Replace protobuf, HTTP, Kafka, and browser contracts with amount/currency.
2. Rename routes, topics, hub methods, config, SQLite, and Postgres entities.
3. Add descriptor, golden-contract, and validation tests.
4. Generate a clean EF baseline, fresh schemas, and the new detector artifact.
5. Build/test before changing decision behavior.

### Phase 3 — explainable decisions and conflict hardening

1. Port the amount detector and shared transform.
2. Add decision values, policy/evidence columns, constraints, and tests.
3. Remove Gemini and publish complete alerts directly.
4. Add canonical fingerprint/conflict behavior.
5. Run EF and failure-path tests.

### Phase 4 — Redis

1. Add validated options, singleton connection, interface, atomic store, and metrics.
2. Add exact-window/idempotency/concurrency/integer integration tests.
3. Add dirty-key tracking, bounded lazy rebuild, and DB-failure invalidation.
4. Prove outage and recovery in E2E.

### Phase 5 — UI and observability

1. Port SignalR contracts and runtime guards.
2. Replace temperature/AI UI with amount streams and explainable alerts.
3. Rename/extend metrics, traces, Grafana, Prometheus, and logs.
4. Inspect normal, spike, burst, ingestion, Postgres, Kafka, and Redis failure scenarios.

### Phase 6 — delivery surfaces and docs

1. Update Compose/E2E, Docker, Make, Tilt, CI, scripts/SQL, Helm, Terraform, pgAdmin, examples.
2. Rewrite active docs and resume.
3. Scan contents and filenames; report intentional old terms.
4. Regenerate optional snapshots only after active files are final.

### Phase 7 — full acceptance

Run everything below from clean target data. Report commands/results, tests added, intentional
remaining old terms, open work moved to TODO, and exact supported claims. Do not report completion
with failing checks, a stale migration/dashboard, or Redis stalling Kafka.

## Final verification

The legacy local volumes cannot be migrated honestly. This is a **manual, destructive prerequisite**
for an operator who explicitly chooses to discard only this Compose project's demo data; never run
it automatically or against an unverified project context:

~~~sh
docker compose down --volumes
~~~

Normal automated acceptance begins here:

~~~sh
dotnet restore TxMonitoringPlatform.slnx
dotnet tool restore
dotnet build TxMonitoringPlatform.slnx --configuration Release --no-restore --warnaserror
dotnet format TxMonitoringPlatform.slnx --no-restore --verify-no-changes
dotnet test TxMonitoringPlatform.slnx --configuration Release --no-build

cd src/TxMonitoring.Web.Dashboard
npm ci
npm run lint
npm run build
cd ../..

docker compose config --quiet
docker compose build feed-emulator capture-gateway ingestion-api detection-worker web-api web-dashboard

dotnet ef migrations has-pending-model-changes \
  --project src/TxMonitoring.Detection.Worker \
  --configuration Release --no-build --no-color

./scripts/e2e.sh
bash -n scripts/*.sh
shellcheck scripts/*.sh
~~~

The E2E script must create a blank isolated Postgres data store, run the one migration owner, and
prove the expected schema before starting detection. This is the fresh-migration apply smoke test;
`has-pending-model-changes` alone is insufficient.

Also run the renamed Helm and Terraform static checks:

~~~sh
helm dependency build chart
helm lint chart
helm template tx-monitoring chart --namespace tx-monitoring
helm template tx-monitoring chart --namespace tx-monitoring \
  --set externalSecret.enabled=true \
  --set externalSecret.remoteRefs.postgresConnectionString.key=placeholder \
  --set transportSecurity.enabled=true \
  --set transportSecurity.ingestion.secretName=placeholder-ingestion \
  --set transportSecurity.edgeGateway.secretName=placeholder-gateway

terraform -chdir=infra fmt -check -recursive
terraform -chdir=infra init -backend=false -input=false
terraform -chdir=infra validate -no-color
~~~

These prove static consistency, not a deployed production environment. Run make verify as part of
the manual live-stack demo after the queue, Kafka consumer, and outbox have been exercised.

Scan contents and filenames, excluding generated/vendor/history artifacts while editing:

~~~sh
rg -n -i \
  'Industrial\.|IndustrialPlatform|TelemetryReading|TelemetryEnvelope|TelemetryAlertEnvelope|TelemetryDto|UploadTelemetry|TelemetryResponse|TelemetryIngestion|telemetry-events|telemetry-alerts|sensor-emulator|edge-gateway|diagnostics-worker|EquipmentId|DeviceCount|AcquisitionWorker|EngineTemperature|OilPressure|MachineHealth|industrial_db|EQ-|edge\.(batch|upload)|gemini' \
  . \
  -g '!PORT.md' -g '!AI.md' -g '!**/bin/**' -g '!**/obj/**' \
  -g '!**/node_modules/**' -g '!**/dist/**' -g '!chart/charts/**' \
  -g '!**/.terraform/**' -g '!*.lscache' -g '!.git/**'

find . -path './.git' -prune -o -path '*/bin' -prune -o -path '*/obj' -prune \
  -o -path '*/node_modules' -prune -o -path '*/dist' -prune \
  -o -path '*/.terraform' -prune \
  -o -type f -print | rg -i \
  'Industrial|TelemetryReading|TelemetryEnvelope|UploadTelemetry|sensor-emulator|edge-gateway|diagnostics-worker|EngineTemperature|OilPressure|Gemini|training_data'
~~~

Then manually review a broader prose-only search for `industrial`, `tractor`, `sensor`, `equipment`,
and standalone `telemetry`. `OpenTelemetry`, `System.Diagnostics`, and Microsoft diagnostics APIs
are legitimate framework names and must not be renamed. PORT.md necessarily contains migration
source names. Report intentional exceptions instead of widening exclusions until the scan appears
clean.
