# Core

Cross-domain infrastructure packages reused across services and domains (SubmitQueue, Stovepipe, and other repo-local services). These are foundational building blocks — not domain entities, not pluggable extensions — and they carry **no domain dependencies**.

For infrastructure shared only *within* a single domain (across that domain's own gateway and orchestrator), see the domain-scoped `core/` package, e.g. `submitqueue/core/` (which holds the queue `consumer` framework and the `request` lifecycle).

## Packages

- **errs/** — Error classification framework. Classifies errors by origin (user vs. infra) and retryability. Extensions return plain errors; service controllers classify them.
- **httpclient/** — Shared HTTP transport helpers (timeouts, instrumentation) for extensions that talk to external services over HTTP.
- **metrics/** — Metrics utility helpers for `tally.Scope`. Provides standardized counters, timers, and histograms with error-aware tagging via `core/errs` integration.
