# Why this project maps onto payments (and where it stops)

The architecture is a monitoring pipeline with a durability boundary, and that shape — not
any domain vocabulary — is what generalises. The vocabulary did change deliberately:

```
old                                 payments
-----------------------------------------------------------------
feed emulator (synthetic)     ->    synthetic transaction source (NOT a bank)
capture gateway               ->    monitoring capture point / branch store-and-forward
ingestion API                 ->    acquirer / processor gateway (monitoring intake)
transaction observation       ->    outbound debit attempt selected for monitoring
amount spike (online IID)     ->    per-account baseline signal, not a fraud model
AmountMinor (long)            ->    integer minor units (never double, never DECIMAL)
transaction-events (Kafka)    ->    monitoring events (NOT payment-authorizations)
risk-alerts (Kafka)           ->    review recommendations (NOT approvals/declines)
```

What generalises to every data-intensive backend — payments, lending, trading, regtech — is
the correctness machinery, not the topology:

- idempotency keys minted at the origin
- at-least-once delivery plus an idempotent consumer
- event time vs processing time
- sequence numbers that make loss detectable
- an audit trail for anything discarded

For a trading firm specifically, lead with ordering, sequence gaps and replay rather than
store-and-forward.

# The feed is synthetic on purpose

A real payment origin would retain an authoritative record; a terminal in offline mode keeps
accepting cards precisely because it persists locally first. This repository deliberately
begins with a **synthetic load generator** and demonstrates downstream monitoring custody:
the durable contract starts when the capture gateway commits a valid transaction to SQLite
and returns `202`. The feed's bounded in-memory channel drops under pressure and says so out
loud (`feed.transaction.dropped`, `feed.transaction.delivery_dropped`).

Do not claim the feed is a processor, terminal, ledger, or source of truth, and do not claim
durable capture of every generated transaction.

# Money is not a double

Integer minor units in a `long`: protobuf `int64`, SQLite `INTEGER`, Postgres `bigint`,
checked arithmetic for sums, Redis integer commands for window sums.

Binary floating point cannot represent 0.1 exactly, so `0.1 + 0.2 != 0.3`. Accumulate that
over a million transactions and the totals do not balance. Interviewers notice this one
immediately. (The one exception is the ML.NET detector's derived `log(1 + AmountMinor)`
feature: a float used only as a model input, never stored or totalled as money.)

# Card networks really do have two durable layers

The thing that makes the store-and-forward story land in payments:

```
card reader  --->  store controller  --->  acquirer  --->  network  --->  issuer
   DURABLE           DURABLE
```

Both of the first two hops persist locally before acknowledging. Retail terminals support
"offline mode" or "store and forward" explicitly: when the link to the processor is down,
the terminal keeps accepting cards, stores the transactions locally, and forwards them
when connectivity returns. Floor limits, offline PIN verification, and issuer risk rules
exist precisely to bound how much risk the merchant takes on during that window.

Contrast this project, where the synthetic feed holds transactions in RAM and the capture
gateway is the only durable layer. That asymmetry is honest for a monitoring demo and would
be wrong for a payments origin.

## Glossary for the paragraph above

FLOOR LIMIT
  A monetary threshold below which a terminal may approve a sale WITHOUT asking the issuer
  online. Say the floor limit is 30 -- a 20 purchase can be approved locally, a 50 one has
  to go online. Set per merchant and card type according to how much risk the acquirer
  will wear.

OFFLINE PIN
  An EMV chip card can verify the PIN against the CHIP ITSELF rather than sending it to the
  issuer. The card confirms the cardholder is present with no network round trip at all.

THE RISK THEY ACCEPT
  An offline-approved transaction can turn out to be bad: stolen card, closed account, no
  funds. The merchant has already handed over the goods, so they eat the loss or take the
  chargeback. That is the trade -- a shop that stops accepting cards during an outage loses
  revenue with certainty, so the industry chose bounded fraud losses over closed tills.
  Floor limits are the bound.

# The principle

> The durability of a buffer should match the cost of losing one item in it.

That single sentence explains every buffering decision in this repo:

| item | cost of losing one | correct buffer |
| --- | --- | --- |
| synthetic feed transaction | shrug -- load is regenerable and pre-custody by design | RAM, bounded, drop and count |
| captured transaction | a monitoring blind spot over real money movement | disk, fsync'd, never dropped |

A dropped feed transaction is cheap because the feed is synthetic load, not customer intent.
A dropped captured transaction would be a gap in oversight of money movement the system had
already promised to watch — which is why the gateway pays for an fsync per transaction.

# What gets harder, not easier

Do not claim this is a payments system with renamed fields. Things that are genuinely harder
once real money is involved:

- **Reconciliation.** Monitoring observations have no counterparty. Payments have an
  acquirer whose totals must match yours at end of day, and a process for when they do not.
- **Reversals.** An observation is never un-taken. An authorization gets voided, refunded,
  or charged back, so the ledger needs entries that undo without deleting.
- **Lifecycle.** This project has one event type: a monitored debit attempt. Real payments
  correlate authorisations, captures, refunds, and chargebacks — an explicit event-type and
  rule-eligibility contract this repo deliberately does not have.
- **Retention and audit.** A poison transaction goes to `transaction-events-dlq` with its
  error attached, but production regulated monitoring would additionally need durable
  remediation, operator acknowledgement, controlled replay, access control, and much longer
  retention.
- **Real PII and PCI scope.** This project stores only opaque synthetic account IDs. Real
  cardholder data (PANs, names, bank numbers) would bring encryption, access control,
  retained logs, and audit — none of which this demo provides. Removing the old LLM call
  reduces data egress; it does not establish compliance.

Being able to name these is worth more than the rename itself -- it shows you know the
domain has depth rather than assuming a find-and-replace covers it.
