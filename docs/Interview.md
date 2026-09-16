# From the "Store-Forward" bullet:

- "You mentioned using a SQLite WAL buffer with `synchronous=FULL`. Why SQLite over just writing to a local text file? What exactly does Write-Ahead Logging (WAL) do for you if the power goes out?"

- "How did you implement HTTP 429 backpressure? How did the client know to back off and retry?"

# From the "Idempotency" bullet:

- "You claim effectively-once processing. Kafka only guarantees at-least-once. How exactly did you guarantee exactly/effectively once?"

- "Why did you use UUIDv7 for idempotency keys instead of standard UUIDv4?"

# From the "API Relay & SignalR" bullets:

- "SignalR holds open WebSocket connections. What happens to the telemetry stream if the ASP.NET server restarts or scales up? How do clients reconnect?"

- "How did you route Kafka events to specific SignalR clients?"

# From the "Observability & GitOps" bullets:

- "How did you pass OpenTelemetry trace IDs from the gRPC origin, through Kafka, and into the ML worker?"

- "How does ArgoCD differ from traditional push-based CI/CD pipelines like GitHub Actions?"

# From the "e2e-latency" bullet:

- "0.8 seconds average gateway-to-decision — what does that number actually cover, and how did you measure it?"

- "Did you trust the first number you measured?"

  The story to tell: no. The first run said 62s average. The e2e compose never started Redis, so every decision silently burned reconnect timeouts before degrading to Unavailable — the harness was measuring degradation, not the healthy path, and nothing failed because nothing asserted latency. The fix made the environment honest (Redis runs in the harness now, detection waits on it healthy) and the assertion honest (a 30s ceiling with 24x headroom over steady state, tight enough to fail loudly on a recurrence). True number after the fix: 0.8s avg, 1.3s max, 20 tx/s, stable across runs. The seam lives in `scripts/e2e.sh`; the lesson is the answer: distrust any latency number from an environment that doesn't assert it.
