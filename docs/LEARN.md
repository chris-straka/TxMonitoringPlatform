# Learning TxMonitoringPlatform

A guided tour for someone who wants to understand, change, and defend this code in an interview.
It concentrates on what is specific to this repo: the risk policy, velocity windows, money
types, redelivery, and latency. The capture gateway, outbox, and DLQ come from
[IndustrialPlatform](https://github.com/chris-straka/industrial-telemetry-platform), whose
`docs/LEARN.md` covers them in depth.

## 1. The problem

Banks, PSPs, and card issuers watch every outbound payment for patterns that deserve a human look:
an amount far outside an account's habit, many debits in a minute, or a run of large ones that add
up. The monitoring system sits beside the money movement, not inside it. It must keep up with the
feed, explain every score to an analyst or auditor, keep working when a cache dies, and never
count one transaction twice. Production systems (Featurespace, Feedzai, in-house rules engines)
combine exactly these parts: per-entity state, sliding windows, a versioned rule policy, and an
alert queue.

## 2. Concepts you need

**Facts in, decisions derived.** The feed sends only identity, account, sequence, event time,
amount, and currency. Every score and reason is computed by detection, so no producer can inject a
`RiskScore`. See `src/TxMonitoring.Shared/TransactionContracts.cs:64`.

**Integer minor units.** `AmountMinor` is a `long` count of cents. Floats would make sums
order-dependent and lossy; decimals are fine in C# but do not survive JSON and Lua cleanly. Sums
in velocity are integers end to end.

**Explainable, versioned policy.** `RiskPolicyEvaluator.Evaluate` applies four rules in a fixed
order and returns the score, ordered reasons (metric, observed value, threshold, points), the
policy version, and a hash of the effective configuration
(`src/TxMonitoring.Detection.Worker/Features/RiskPolicy/RiskPolicyEvaluator.cs:45`,
`RiskPolicyOptions.cs:108`). Change any threshold and the hash changes, so old decisions stay
attributable.

**Family caps.** Correlated rules share a cap: absolute and spike are the amount family,
count and sum are the velocity family, each capped at 70
(`RiskPolicyEvaluator.cs:130`). Two weak signals from the same family cannot masquerade as
independent evidence, but one from each family can reach review (60).

**Per-account spike baseline.** An ML.NET IID spike detector per (account, currency) over
`log(1 + AmountMinor)`, warmed from Postgres
(`src/TxMonitoring.Detection.Worker/Features/Diagnostics/ML/AmountSpikeDetector.cs:69`). It is a
baseline, not a fraud model. The spike rule only fires with a warm 20-event history
(`RiskPolicyEvaluator.cs:88`).

**Sliding windows in Redis with Lua.** One script adds the event, trims anything older than five
minutes, and returns the one-minute count and five-minute sum atomically, so concurrent replicas
cannot tear a window (`src/TxMonitoring.Detection.Worker/Features/RiskPolicy/RedisVelocityStore.cs:64`).
Windows are keyed by `OccurredAt`, and an event older than the watermark is `OutOfOrder` and not
added, so draining an outage never manufactures a burst.

**Cache, never custody.** If Redis is down, the decision is stored with `VelocityStatus`
Unavailable (never zero counts), a metric increments, and the offset still advances
(`RedisVelocityStore.cs:114`, `RiskPolicyEvaluator.cs:101`). Losing Redis loses accuracy for a
window, never a transaction.

**Redelivery decision table.** A replayed `MessageId` with the same payload fingerprint is a
no-op; the same ID with different content is a conflict that goes to the DLQ. One classifier is
shared by the pre-insert check and the unique-violation race path
(`src/TxMonitoring.Detection.Worker/Features/Diagnostics/Redelivery.cs:19`,
`src/TxMonitoring.Shared/TransactionFingerprint.cs:28`).

**Latency seam.** `scripts/e2e.sh:332` posts a healthy batch and measures `DetectedAt -
OccurredAt` in Postgres, with a 30 s ceiling that fails the run.

## 3. Reading order

1. `src/TxMonitoring.Shared/TransactionContracts.cs` and `TransactionFingerprint.cs`.
2. `src/TxMonitoring.Feed.Emulator/TransactionGeneratorWorker.cs`: the five scenarios and why each
   amount band was chosen.
3. `src/TxMonitoring.Detection.Worker/Features/RiskPolicy/RiskPolicyOptions.cs`: thresholds,
   points, caps, and the configuration hash.
4. `RiskPolicyEvaluator.cs`: the whole policy in about 100 lines.
5. `Velocity.cs`, then `RedisVelocityStore.cs`: the snapshot type, the Lua script, degradation,
   and lazy rebuild.
6. `Features/Diagnostics/ML/AmountSpikeDetector.cs`.
7. `Features/Diagnostics/TransactionConsumerWorker.cs` and `Redelivery.cs`: one message from
   Kafka to a committed decision.
8. `tests/TxMonitoring.Detection.Tests/RiskPolicyTests.cs`, `VelocityDegradationTests.cs`,
   `ScenarioReplayTests.cs`, `PolicyThroughputTests.cs`.
9. `scripts/e2e.sh` from line 320: the latency seam.

## 4. Exercises

1. **Run the scenario replay.**
   `dotnet test tests/TxMonitoring.Detection.Tests -c Release --filter ScenarioReplayTests --logger "console;verbosity=detailed"`.
   Before looking, predict the review rate for Steady and for HighAmount.
   <details><summary>Answer</summary>Steady 0%, HighAmount 100%. HighAmount fires the absolute
   rule (35) every time, and from the fourth $3,000+ debit the five-minute sum passes $10,000
   (40), so 75 reaches review.</details>

2. **Why does Steady never alert when its spike rate is 7%?**
   <details><summary>Answer</summary>A lone spike scores 40, below the review threshold of 60.
   The policy needs corroboration from a second signal. Lower `ReviewThreshold` to 40 and rerun
   exercise 1: Steady starts reviewing about 7% of traffic, which is the false-positive cost of
   trusting one detector.</details>

3. **Raise the family cap.** Set `VelocityFamilyCap` to 80 and predict whether any scenario's
   review rate changes.
   <details><summary>Answer</summary>Burst and Combined already review once both velocity rules
   fire (40 + 40 capped at 70 is above 60), so the rate is unchanged; only the score values move.
   The configuration hash changes, which `RiskPolicyTests` shows.</details>

4. **Kill Redis in a test.** Read `VelocityDegradationTests.cs`, then write a case where Redis
   is down during a Burst. What does the decision record, and is the message committed?
   <details><summary>Answer</summary>`VelocityStatus` is Unavailable with null counts, the
   velocity rules do not fire, the decision is persisted, and the offset is committed. Burst
   traffic stops reviewing, because without velocity evidence its only possible signal is a
   lone spike, which scores 40.</details>

5. **Feed an out-of-order event.** Using the Lua semantics, what happens to an event whose
   `OccurredAt` is older than the account's watermark?
   <details><summary>Answer</summary>It is not added to the window and the snapshot is
   `OutOfOrder`, which the policy treats as missing evidence. This prevents an outage drain from
   looking like a burst.</details>

6. **Measure policy throughput.** Run `PolicyThroughputTests` five times in Release and take the
   median. Why does it vary so much between runs?
   <details><summary>Answer</summary>About 1M decisions/s on an M4, with run-to-run spread from
   JIT tiering and other load on the machine. The test excludes Redis, Kafka, and Postgres, so it
   is a ceiling for the policy, not for the pipeline.</details>

7. **Latency drill (needs Docker).** Run `./scripts/e2e.sh` and find the `latency:` line. Then
   point the detection worker's `Velocity__Configuration` in the e2e compose file at a host that
   does not exist and run it again.
   <details><summary>Answer</summary>Healthy runs measured about 0.8 s average. Without Redis,
   each decision waits out reconnect timeouts before degrading, which is how the original 62 s
   number happened; the 30 s ceiling now fails the run.</details>

## 5. Interview questions

More bullet-specific questions are in [Interview.md](Interview.md).

1. **Is this a fraud model?** No. It is a transparent rule policy over a per-account IID baseline
   and velocity counters. The score ranks; it is not a calibrated probability.
2. **Why integer minor units?** Exact sums in any order, no rounding drift across C#, JSON, Lua,
   and Postgres, and an obvious unit at every boundary.
3. **Why Lua instead of MULTI/EXEC?** The script reads (watermark), branches, writes, trims, and
   computes in one atomic server-side step. MULTI cannot branch on a value read inside it.
4. **What happens when Redis is down for ten minutes?** Decisions keep flowing with Unavailable
   velocity; after recovery, a newly needed window rebuilds lazily from recent Postgres rows.
   Decisions made during the outage are not rescored.
5. **How do you know a policy change happened?** Every decision stores the policy version and a
   content hash of the effective thresholds and points, so two runs with the same version label
   but different config are distinguishable.
6. **Where did the 0.8 s come from, and do you trust it?** From the e2e seam measuring
   `DetectedAt - OccurredAt` for 20 transactions on a healthy stack. The first number was 62 s
   because Redis was missing; the seam now asserts a ceiling so that class of mistake fails.
7. **How is a duplicate different from a conflict?** Same `MessageId` and same payload
   fingerprint is a replay and a no-op. Same ID, different content is a producer bug and goes to
   the DLQ instead of overwriting the first decision.
8. **Why does each scenario exist?** Each isolates or combines named rules so a demo or a test
   can show a specific path. The replay test asserts what each one actually triggers, which is
   how the HighAmount documentation error was found.

## 6. Connections

- [IndustrialPlatform](https://github.com/chris-straka/industrial-telemetry-platform): the parent
  pipeline (edge store-and-forward, outbox, DLQ, chaos harness) plus ML.NET anomaly detection
  and AI diagnosis for IoT telemetry.
- `risk-scorer` is a pre-authorisation scoring hook for PaymentOrchestration; this repo monitors
  after the fact and never blocks a payment.
- `Ledger` and `PaymentOrchestration` move the money this platform would observe; `recon` matches
  PSP files against them.
- `aml-screen` covers sanctions screening, a different monitoring problem with similar alert
  queues.
- `cdc-pipe` is the outbox/inbox pattern on its own.
