-- End-to-end audit for the store-and-forward demo.
--   make verify
--
-- This can prove that Postgres has no duplicate MessageIds, the gateway queue drained, and the
-- alert outbox drained. It reports sequence gaps, but a Postgres-only query cannot tell whether a
-- gap was an intentional best-effort feed drop before 202 or downstream loss after 202.

\set ON_ERROR_STOP on

\echo '== totals: duplicates must be 0 =='
SELECT COUNT(*)                                          AS ingested,
       COUNT(DISTINCT "MessageId")                       AS unique_ids,
       COUNT(*) - COUNT(DISTINCT "MessageId")            AS duplicates
FROM "Transactions";

\echo ''
\echo '== sequence gaps by inferred generator run (reported, not treated as downstream proof) =='
-- The generator restarts SequenceNumber at 1 per run. A non-increasing value in event-time order
-- starts a new inferred run, so an old run cannot hide gaps in a newer one as MAX(seq)-COUNT(*) did.
WITH ordered AS (
    SELECT *,
           LAG("SequenceNumber") OVER (
               PARTITION BY "AccountId"
               ORDER BY "OccurredAt", "DetectedAt", "MessageId"
           ) AS previous_sequence
    FROM "Transactions"
    WHERE "SequenceNumber" > 0
      AND "AccountId" NOT LIKE 'MANUAL-%'
), marked AS (
    SELECT *,
           CASE
               WHEN previous_sequence IS NULL OR "SequenceNumber" <= previous_sequence THEN 1
               ELSE 0
           END AS begins_run
    FROM ordered
), runs AS (
    SELECT *,
           SUM(begins_run) OVER (
               PARTITION BY "AccountId"
               ORDER BY "OccurredAt", "DetectedAt", "MessageId"
           ) AS run_number
    FROM marked
)
SELECT "AccountId",
       run_number,
       COUNT(*) AS rows,
       MIN("SequenceNumber") AS min_seq,
       MAX("SequenceNumber") AS max_seq,
       MAX("SequenceNumber") - MIN("SequenceNumber") + 1
           - COUNT(DISTINCT "SequenceNumber") AS gaps_inside_observed_range
FROM runs
GROUP BY "AccountId", run_number
ORDER BY "AccountId", run_number;

\echo 'Tail loss after the last observed sequence cannot be inferred without an origin-side run total.'

\echo ''
\echo '== end-to-end lag (ReceivedAt - OccurredAt) =='
-- Flat in steady state. A mountain across the outage, because transactions kept their
-- ORIGINAL event time while they sat in the capture buffer. If this were near zero
-- after an outage it would mean the timestamps were being stamped on arrival --
-- i.e. the demo lying to itself.
SELECT MAX(EXTRACT(EPOCH FROM ("ReceivedAt" - "OccurredAt")))::int          AS max_lag_seconds,
       AVG(EXTRACT(EPOCH FROM ("ReceivedAt" - "OccurredAt")))::numeric(10,2) AS avg_lag_seconds
FROM "Transactions";

\echo ''
\echo '== review-recommended transactions flagged by the deterministic policy =='
SELECT COUNT(*) FILTER (WHERE "ReviewRecommended") AS review_recommended,
       COUNT(*)                                    AS total
FROM "Transactions";

\echo ''
\echo '== alert outbox: pending must be 0 after the system catches up =='
SELECT COUNT(*) FILTER (WHERE "PublishedAt" IS NULL) AS pending,
       COUNT(*) FILTER (WHERE "PublishedAt" IS NOT NULL) AS published
FROM "AlertOutboxMessages";

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM "Transactions") THEN
        RAISE EXCEPTION 'verification has no transaction rows; an empty database is not a pass';
    END IF;

    IF (
        SELECT COUNT(*) - COUNT(DISTINCT "MessageId")
        FROM "Transactions"
    ) <> 0 THEN
        RAISE EXCEPTION 'duplicate MessageIds found in Postgres';
    END IF;

    IF EXISTS (
        SELECT 1 FROM "AlertOutboxMessages" WHERE "PublishedAt" IS NULL
    ) THEN
        RAISE EXCEPTION 'the alert outbox has pending rows';
    END IF;
END;
$$;
