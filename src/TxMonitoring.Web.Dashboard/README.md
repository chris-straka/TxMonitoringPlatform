# TxMonitoring dashboard (Vue + TypeScript + Vite)

Live risk-monitoring view: streams `transaction_events` and `risk_alerts` from the
web API's SignalR `/riskHub`, plots per-account amounts, and lists review-recommended
alerts with their reason codes. Latest-only and best-effort: a disconnected client
misses live events, and bounded dedupe collapses at-least-once redelivery by `MessageId`.

Stack: Vue 3 + Pinia + vue-router + Highcharts, tested with Vitest.

```sh
npm ci
npm run dev        # Vite dev server (proxied through Compose on :5173)
npm run lint       # eslint: js + typescript + vue recommended sets
npm test           # vitest run
npm run build      # vue-tsc -b && vite build -> dist/ (served by nginx in the image)
```

`VITE_API_URL` selects the SignalR origin at build time; when unset the app falls back
to `window.location.origin`. Amounts arrive as minor-unit integers and are guarded with
`Number.isSafeInteger`; velocity aggregates arrive as base-10 strings and are validated
as digit strings because they can exceed `Number.MAX_SAFE_INTEGER`.
