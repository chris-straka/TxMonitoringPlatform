# TxMonitoring Helm chart

This is a render-validated Kubernetes prototype for the application topology. It is not a
production deployment bundle: image publishing, an OTLP Collector, authentication,
storage-class selection, and secret-store installation are intentionally left to the operator.
The chart can configure capture-to-ingestion mTLS, but certificate issuance and rotation remain
operator responsibilities.

Before installation, create the Secret named by `platform.applicationSecret.name`
(`tx-monitoring-secrets`) with:

- `postgres-password`
- `postgres-connection-string`, using the same password and the dependency Service host
  `tx-monitoring-postgresql` (database `tx_monitoring`, user `txmonitoring`)

There is no Gemini/AI wiring anywhere in this chart. Detection explains itself with
deterministic reason codes and evidence; no external LLM key exists to provision.

Alternatively, install External Secrets Operator and a compatible SecretStore, then enable and
configure `externalSecret`. The chart contains no literal application or database credentials.

## Capture-to-ingestion mTLS

Set `transportSecurity.enabled=true` and provision two Kubernetes Secrets. The Secret names and
key names are configurable under `transportSecurity`; the defaults below describe the required
contents:

- the ingestion Secret contains `server.pfx`, `server-pfx-password`, `ca.crt`, and
  `allowed-client-sha256.txt`;
- the capture gateway Secret contains `client.pfx`, `client-pfx-password`, and `ca.crt`.

`allowed-client-sha256.txt` contains one permitted client-certificate SHA-256 fingerprint per
line. The server certificate must be valid for the ingestion Service DNS name used by
`Ingestion__ApiUrl`. Each PFX password is read directly from its Secret rather than placed in values.
The chart projects only the certificate files needed by each workload, read-only, and never mounts
the server private key into the capture gateway or the client private key into ingestion.

For example, after creating Secrets named `tx-monitoring-ingestion-mtls` and
`tx-monitoring-capture-mtls`, enable the mode with:

```yaml
transportSecurity:
  enabled: true
  ingestion:
    secretName: tx-monitoring-ingestion-mtls
  captureGateway:
    secretName: tx-monitoring-capture-mtls
```

This changes only the internal gRPC endpoint on port 8081 to HTTPS with required client
certificates. REST startup, liveness, and readiness probes remain HTTP on port 8080.

The capture gateway is a StatefulSet with one durable SQLite PVC per pod. The feed emulator is a
single-replica Deployment because `Generator__ReplicaId` defines account ownership; demonstrate
additional ranges as separately configured releases rather than scaling that Deployment.

## Database migrations

Detection owns the `tx_monitoring` schema. Workers in Production never call `MigrateAsync`
(migrate-on-startup is Development-only in code), so replicas cannot race each other; every
detection pod gates on the `postgres_migrations` readiness check and stays unready until the
schema is current.

The one-shot owner is `templates/migration-job.yaml`, a Job using the detection-worker
image with `--migrate-and-exit`: it applies pending EF migrations and exits without starting
the consumer/outbox workers. Run it once per release before the worker Deployment rolls (it is
on by default via `detectionWorker.migration.enabled`); detection pods stay unready until it
succeeds. If the Job is disabled, apply migrations out of band from an operator checkout:

```sh
dotnet ef database update --project src/TxMonitoring.Detection.Worker
```

## Redis velocity cache (prototype)

This chart installs no Redis. `detectionWorker.velocity.configuration` must point at an
external/managed Redis endpoint reachable from the detection pods. Redis holds derived velocity
windows only: an unreachable endpoint degrades detection to `VelocityStatus` Unavailable and
never blocks Kafka consumption, Postgres persistence, offset progress, or non-velocity alert
signals. TODO: TLS/auth for the velocity store. Do not present this endpoint as production
cache without that hardening.

## Kafka durability notes

The bundled single-broker Kafka (`kafka.provisioning.replicationFactor: 1`) is a local demo
that proves nothing about broker-loss durability. Production-shaped topology (see `infra/`,
replication factor 3 with `min.insync.replicas=2`) keeps producers on `Acks.All` with
idempotence enabled. Topics are `transaction-events`, `risk-alerts` (delete retention; Postgres
is the decision record), and `transaction-events-dlq` (longer delete retention).

Validate the chart with:

```sh
helm dependency build chart
helm lint chart
helm template tx-monitoring chart --namespace tx-monitoring
```

Kafka and PostgreSQL are enabled by default. Kafka topic provisioning is a regular Job so installs
using `--wait` cannot deadlock on application readiness before the topics exist. The observability
backend dependencies and optional Istio/ExternalSecret resources are disabled until explicitly
configured.
