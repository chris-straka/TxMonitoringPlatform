# track: swe | csa | both. TODO-prefixed lines are not true yet.
#
# Positioning: fintech transaction risk monitoring: explainable risk/velocity rules, Redis as a
# cache that can fail, integer minor units, and a measured end-to-end latency story (Vue). This
# repo began as a copy of IndustrialPlatform (IoT telemetry + ML.NET anomaly detection + AI
# diagnosis). Lead with the bullets below; never list a shared-lineage bullet under both.
# Numbers: Apple M4 Mac mini (10 cores, 16 GB), Release build; commands in README "Results".

projects:
  - id: txmonitoring
    name: "Transaction Risk Monitoring Platform"
    context: "Personal Project"
    stack: ".NET 10, Kafka, Postgres, Redis (Lua), ML.NET, Vue, Docker"
    bullets:
      # lead: unique to this repo and measured
      - id: risk-policy
        track: both
        text: "Built an explainable, versioned transaction risk policy (absolute amount, per-account spike, 1-minute count, 5-minute sum) with family caps, ordered reason codes, and a config hash on every decision; a seeded replay of 5 feed scenarios shows 0% review on steady traffic despite a 7% detector-tail rate."
      - id: velocity
        track: swe
        text: "Implemented sliding-window velocity per account and currency as one atomic Redis Lua script keyed on event time, treated as a derived cache: when Redis is down, decisions persist with Unavailable status and the consumer never stalls."
      - id: e2e-latency-story
        track: swe
        # From the owner's scripts/e2e.sh run on 2026-09-16; not re-run 2026-10-05 (Docker down).
        text: "Measured 0.8s average (1.3s max) gateway-to-decision latency through Kafka, detection, Postgres, and Redis, after catching a harness that reported 62s because Redis was missing, and added a latency ceiling that fails CI on a recurrence."
      - id: policy-perf-measured
        track: swe
        text: "Benchmarked ~1M in-process risk decisions/s (median of 5 runs) with a repeatability guard proving identical inputs replay identical decisions and reason codes."

      # working set (older wording; prefer the lead bullets above)
      - id: arch
        track: both
        text: "Architected an event-driven transaction risk-monitoring platform using .NET 10, Apache Kafka, Postgres, and Docker to observe a synthetic payment feed and publish explainable review alerts."
      - id: detection-worker
        track: both
        text: "Developed a detection worker consuming Kafka events to run per-account amount-spike checks and a deterministic risk policy, persisting each decision with reason codes and detector provenance."
      - id: signalr
        track: both
        text: "Implemented a SignalR WebSocket bridge to stream live transaction events and risk alerts directly to a Vue dashboard."
      - id: terraform
        track: csa
        text: "Drafted infrastructure specifications using Terraform (IaC) and defined GitOps deployment workflows via ArgoCD on Kubernetes (Helm, Istio), validated by plan-only CI checks."
      - id: policy
        track: both
        text: "Implemented a versioned risk policy returning integer scores, ordered reasons, and a content-derived configuration hash, with regression tests proving any effective rule change alters the hash."
      - id: observability
        track: both
        text: "Configured OpenTelemetry metrics, logs, and traces with a checked-in Grafana dashboard and Prometheus alerts for loss, backlog, retries, dead letters, velocity outages, and alert-outbox health."
      - id: idempotency
        track: swe
        text: "Designed durable post-gateway custody with at-least-once gRPC and Kafka delivery for valid transactions, minting UUIDv7 idempotency keys at the feed origin and deduplicating replays in Postgres."
      - id: tracing
        track: swe
        text: "Implemented distributed W3C tracing, metrics, and logging across the pipeline using OpenTelemetry, aggregating into Prometheus, Tempo, and Loki for Grafana dashboards."
      - id: api-relay
        track: swe
        text: "Developed an ASP.NET Web API relaying Kafka events via a SignalR WebSocket bridge, pushing live updates to a Vue/Vite frontend."


      # pool. Shared lineage: arch, observability, idempotency, tracing, api-relay, outbox, dlq,
      # health, config, ci, and reliability-boundary have twins in IndustrialPlatform/resume.md.
      # The edge/delivery bullets (store-and-forward, settlement, quarantine, admission,
      # event-time, chaos harness, mTLS) were removed here: IndustrialPlatform owns them.
      # Add the scenario-replay finding below when space allows.
      - id: scenario-replay
        track: swe
        text: "Replayed every synthetic feed scenario through the production detector and policy with Redis-equivalent windows, catching a documented scenario that actually alerted 100% of the time and pinning each scenario's rule mix in tests."
      - id: outbox
        track: swe
        text: "Protected alert delivery with a transactional outbox, committing each review-recommended decision and its alert in one Postgres transaction while dashboard consumers deduplicate publisher retries by message ID."
      - id: dlq
        track: swe
        text: "Added a Kafka dead-letter path that durably copies malformed records with their source partition and offset before committing, keeping bad payloads inspectable without stalling the consumer."
      - id: health
        track: both
        text: "Added separate liveness and dependency-aware readiness probes across the gateway, ingestion, detection, and web services, covering Kafka topics and partition assignment, current Postgres migrations, and SQLite writability."
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
        text: "Measured ~1M in-process policy decisions/s (~1µs each, median of 5 runs) with a repeatability guard proving identical inputs replay identical decisions."
      - id: e2e-latency
        track: swe
        # Measured by the owner with scripts/e2e.sh on 2026-09-16; not re-run 2026-10-05 (Docker down).
        text: "Measured 0.8s average gateway-to-decision latency end to end across a 20-transaction Docker batch (gateway, Kafka, detection, Postgres, Redis; max 1.3s, 20 tx/s)."
      - id: config
        track: csa
        text: "Bound every service's runtime configuration to validated typed options with fail-fast startup checks, so a missing deployment value stops the container instead of silently defaulting."
      - id: ci
        track: both
        text: "Set up GitHub Actions CI running a warnings-as-errors build, format verification, EF Core migration-drift detection, tests with coverage, frontend lint/test/build, Compose validation, and Helm chart lint and render."
      - id: reliability-boundary
        track: both
        text: "Defined an explicit reliability boundary—best-effort before the gateway's 202 Accepted and durable custody after it, with permanent rejections quarantined and metered—backed by regression tests, an isolated failure harness, chaos targets, and an automated audit."


      # sharper variants of working-set lines
      - id: arch-alt
        track: both
        text: "Architected a multi-service event-driven monitoring platform using .NET 10, Apache Kafka, and Postgres, with a Docker Compose runtime and provisioned metrics, logs, and traces."
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
        text: "TODO: Developed an ASP.NET Web API relaying Kafka events via a SignalR WebSocket bridge, pushing sub-second updates to a Vue/Vite frontend."
      - id: production-security
        track: csa
        text: "TODO: Extended workload identity and encrypted transport to the feed, Kafka, Postgres, Redis, and observability, backed by production certificate enrollment, rotation, and revocation."
      - id: perf
        track: both
        text: "TODO: Sustained N transactions/sec end to end at Xms p95 Kafka-to-dashboard latency (fill in real measured numbers)."
