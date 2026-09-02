# SubmitQueue

SubmitQueue service layout:

- `gateway/` — Gateway service: entry point for `Ping`, `Land`, `Cancel`, request-summary, request-history, and queue-listing RPCs. It also consumes request-log events and maintains the public request projections.
- `orchestrator/` — Orchestrator service: coordinates validation, dependency analysis, speculation, builds, landing, cancellation, conclusion, hooks, and DLQ reconciliation.
- `extension/` — SubmitQueue-specific extension contracts and implementations, including storage, queue configuration, change providers, validation, conflict analysis, speculation, and build runners.
- `entity/` — SubmitQueue-specific domain entities.
- `core/` — Infrastructure shared across SubmitQueue's own services, including request and batch lifecycle helpers, change-set resolution, and internal topic keys.

Cross-domain building blocks live outside this directory: shared entities in `platform/base/`, shared extensions in `platform/extension/`, and cross-domain infrastructure such as the consumer framework in `platform/`.
