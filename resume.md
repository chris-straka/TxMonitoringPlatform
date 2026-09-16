# track: swe | csa | both. TODO-prefixed lines are not true yet.

projects:
  - id: txmonitoring
    name: "Transaction Monitoring & Distributed Systems"
    context: "Personal Project"
    stack: ".NET 10, Kafka, Postgres, Redis, Docker, ML.NET"
    bullets:
      # working set
      - id: arch
        track: both
        text: "Architected an event-driven transaction risk-monitoring platform using .NET 10, Apache Kafka, Postgres, and Docker to observe a synthetic payment feed and publish explainable review alerts."
      - id: detection-worker
        track: both
        text: "Developed a detection worker consuming Kafka events to run per-account amount-spike checks and a deterministic risk policy, persisting each decision with reason codes and detector provenance."
      - id: signalr
        track: both
        text: "Implemented a SignalR WebSocket bridge to stream live transaction events and risk alerts directly to a React dashboard."
      - id: terraform
        track: csa
        text: "Drafted infrastructure specifications using Terraform (IaC) and defined GitOps deployment workflows via ArgoCD on Kubernetes (Helm, Istio), validated by plan-only CI checks."
      - id: policy
        track: both
        text: "Implemented a versioned risk policy returning integer scores, ordered reasons, and a content-derived configuration hash, with regression tests proving any effective rule change alters the hash."
      - id: observability
        track: both
        text: "Configured OpenTelemetry metrics, logs, and traces with a checked-in Grafana dashboard and Prometheus alerts for loss, backlog, retries, dead letters, velocity outages, and alert-outbox health."
      - id: store-forward
        track: swe
        text: "Engineered a capture gateway with a durable SQLite WAL buffer (synchronous=FULL) to survive total ingestion outages, shedding excess load via HTTP 429 backpressure to keep the buffer bounded."
      - id: idempotency
        track: swe
        text: "Designed durable post-gateway custody with at-least-once gRPC and Kafka delivery for valid transactions, minting UUIDv7 idempotency keys at the feed origin and deduplicating replays in Postgres."
      - id: tracing
        track: swe
        text: "Implemented distributed W3C tracing, metrics, and logging across the pipeline using OpenTelemetry, aggregating into Prometheus, Tempo, and Loki for Grafana dashboards."
      - id: api-relay
        track: swe
        text: "Developed an ASP.NET Web API relaying Kafka events via a SignalR WebSocket bridge, pushing live updates to a React/Vite frontend."


      # pool
      - id: settlement
        track: swe
        text: "Eliminated data loss on ambiguous upload failures by deleting a buffered transaction only after ingestion explicitly named its ID accepted or rejected, retrying everything left unnamed."
      - id: head-of-line
        track: swe
        text: "Implemented per-transaction rejection at ingestion, quarantining a single deterministic rejection at the gateway instead of letting it block the upload queue behind it."
      - id: quarantine
        track: swe
        text: "Built a bounded SQLite quarantine table that preserves rejected transactions with their original payload and reason code, written in the same transaction as the live-row delete so poison rows stay auditable."
      - id: outbox
        track: swe
        text: "Protected alert delivery with a transactional outbox, committing each review-recommended decision and its alert in one Postgres transaction while dashboard consumers deduplicate publisher retries by message ID."
      - id: dlq
        track: swe
        text: "Added a Kafka dead-letter path that durably copies malformed records with their source partition and offset before committing, keeping bad payloads inspectable without stalling the consumer."
      - id: decoupling
        track: swe
        text: "Decoupled feed generation from transmission behind a bounded channel so network latency cannot reduce the generation rate, instrumenting every intentionally dropped transaction."
      - id: admission-concurrency
        track: swe
        text: "Made buffer admission an atomic reservation guarded by a mutation gate, so concurrent writers cannot exceed the depth ceiling or observe a half-applied state change."
      - id: event-time
        track: swe
        text: "Modeled event time and processing time separately so transactions flushed after a 30-minute outage retain their true occurrence time, with sequence numbers exposing dropped ranges."
      - id: health
        track: both
        text: "Added separate liveness and dependency-aware readiness probes across the gateway, ingestion, detection, and web services, covering Kafka topics and partition assignment, current Postgres migrations, and SQLite writability."
      - id: e2e-chaos
        track: swe
        text: "Wrote an isolated Docker failure-injection harness that stops ingestion, Postgres, and Kafka mid-run and asserts queue recovery, one row per accepted ID, consumer retry, DLQ publication, and outbox recovery."
      - id: mtls
        track: csa
        text: "Secured the gateway-to-ingestion gRPC hop in Docker Compose with mutual TLS, a private development CA, and a SHA-256 client-certificate allowlist that models independently revocable gateway identities."
      - id: detector-provenance
        track: swe
        text: "Persisted raw detector score, p-value, history count, detector version, policy version, and policy hash beside each transaction decision so historical classifications retain their runtime provenance."
      - id: schema-evolution
        track: swe
        text: "Enforced Protobuf schema-evolution discipline on the gRPC contract with frozen field numbers, backed by descriptor regression tests that fail CI on a renumbered field."
      - id: input-validation
        track: swe
        text: "Aligned gateway admission bounds with ingestion-side validation so unprocessable input is refused at the boundary, including malformed trace context caught before it reaches a Kafka header."
      - id: db-schema
        track: swe
        text: "Designed the Postgres schema with a unique idempotency index, per-account warm-up and velocity indexes, a pending-outbox scan index, and check constraints on amount, currency, and score ranges."
      - id: kafka-keying
        track: both
        text: "Keyed transaction events by AccountId so each account's transactions stay ordered on one partition, enabling per-account detector state while consumption scales across accounts."
      - id: redis-cache
        track: swe
        text: "Built Redis velocity windows as derived cache only, with atomic Lua observation, lazy Postgres rebuilds, and Unavailable-status degradation so a cache outage never blocks consumption or persistence."
      - id: redelivery
        track: swe
        text: "Unified Kafka redelivery handling so same-fingerprint replays are no-ops and same-ID content conflicts route to the DLQ — one decision table shared by the pre-insert check and the unique-violation race handler, proven by tests."
      - id: policy-perf
        track: swe
        text: "Measured ~67K in-process policy decisions/s (~15µs each) with a repeatability guard proving identical inputs replay identical decisions."
      - id: e2e-latency
        track: swe
        text: "Measured 0.8s average gateway-to-decision latency end to end across a 20-transaction Docker batch (gateway, Kafka, detection, Postgres, Redis; max 1.3s, 20 tx/s)."
      - id: config
        track: csa
        text: "Bound every service's runtime configuration to validated typed options with fail-fast startup checks, so a missing deployment value stops the container instead of silently defaulting."
      - id: ci
        track: both
        text: "Set up GitHub Actions CI running a warnings-as-errors build, format verification, EF Core migration-drift detection, tests with coverage, frontend lint and build, Compose validation, and Helm chart lint and render."
      - id: reliability-boundary
        track: both
        text: "Defined an explicit reliability boundary—best-effort before the gateway's 202 Accepted and durable custody after it, with permanent rejections quarantined and metered—backed by regression tests, an isolated failure harness, chaos targets, and an automated audit."


      # sharper variants of working-set lines
      - id: arch-alt
        track: both
        text: "Architected a multi-service event-driven monitoring platform using .NET 10, Apache Kafka, and Postgres, with a Docker Compose runtime and provisioned metrics, logs, and traces."
      - id: store-forward-alt
        track: swe
        text: "Engineered a capture gateway buffering 500K transactions in SQLite (WAL, synchronous=FULL) to survive total ingestion outages, shedding overflow via HTTP 429 and Retry-After backpressure."
      - id: tracing-alt
        track: swe
        text: "Preserved W3C distributed tracing across a durable buffered hop by persisting trace context in SQLite and replaying it as span links, keeping a transaction's trace intact through multi-hour outages."
      - id: api-relay-alt
        track: swe
        text: "Developed an ASP.NET Web API relaying Kafka events via a SignalR WebSocket bridge, giving each replica its own consumer group so every client receives the full live stream."
      - id: terraform-alt
        track: csa
        text: "Authored a Helm chart with Istio and External Secrets templates, validated by lint and render in CI, plus Terraform definitions for Kafka topics, ArgoCD, and Istio."


      # TODO
      - id: gitops
        track: both
        text: "TODO: Deployed microservices using Helm, ArgoCD (GitOps), and Istio service mesh, with full observability configured via OpenTelemetry, Prometheus, and Grafana."
      - id: arch-k8s
        track: both
        text: "TODO: Operate the monitoring platform on Kubernetes via the checked-in Helm chart and Terraform stack, exercised beyond lint/render/plan against a real cluster."
      - id: terraform-implemented
        track: csa
        text: "TODO: Applied the Terraform stack to a real cluster and implemented GitOps deployment workflows via ArgoCD on Kubernetes (Helm, Istio)."
      - id: api-relay-subsecond
        track: swe
        text: "TODO: Developed an ASP.NET Web API relaying Kafka events via a SignalR WebSocket bridge, pushing sub-second updates to a React/Vite frontend."
      - id: production-security
        track: csa
        text: "TODO: Extended workload identity and encrypted transport to the feed, Kafka, Postgres, Redis, and observability, backed by production certificate enrollment, rotation, and revocation."
      - id: perf
        track: both
        text: "TODO: Sustained N transactions/sec end to end at Xms p95 Kafka-to-dashboard latency (fill in real measured numbers)."
