# SubmitQueue

SubmitQueue service layout:

- `gateway/` — Gateway service: entry point for `Ping`, `Land`, `Cancel`, request-summary, request-history, and queue-listing RPCs. It also consumes request-log events and maintains the public request projections. `gateway/core/request` materializes those logs; `gateway/extension/storage` is the aggregate, MySQL implementation, and schema only the gateway resolves.
- `orchestrator/` — Orchestrator service: coordinates validation, dependency analysis, speculation, builds, landing, cancellation, conclusion, hooks, and DLQ reconciliation. `orchestrator/core/request` publishes and terminates requests, `orchestrator/core/batch` moves batches, and `orchestrator/extension/storage` is the aggregate, MySQL implementation, and schema only the orchestrator resolves.
- `extension/` — SubmitQueue-specific extension contracts and implementations, including the shared storage contracts, queue configuration, change providers, validation, conflict analysis, speculation, and build runners. Service aggregates and MySQL backends live under the service that resolves them.
- `entity/` — SubmitQueue-specific domain entities.
- `client/` — Gateway client used by command-line tools and the demo.
- `core/` — Infrastructure shared by the gateway and the orchestrator: change-set resolution (`changeset`), internal queue contracts (`messagequeue`), and topic keys (`topickey`).

Cross-domain building blocks live outside this directory: shared entities in `platform/base/`, shared extensions in `platform/extension/`, and cross-domain infrastructure such as the consumer framework in `platform/`.
