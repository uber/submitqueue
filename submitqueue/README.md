# SubmitQueue

SubmitQueue service layout:

- `gateway/` — Gateway service: entry point for land requests (`Ping`, `Land`, `Cancel` RPCs).
- `orchestrator/` — Orchestrator service: coordinates the land pipeline (batch, score, build, merge, conclude, ...).
- `extension/` — SubmitQueue-specific extension implementations (storage, counter, changestore, mergechecker, pusher, scorer, conflict, queueconfig, buildrunner, ...).
- `entity/` — SubmitQueue-specific domain entities.
- `core/` — Infrastructure shared across SubmitQueue's own services (gateway and orchestrator): the queue `consumer` framework and the `request` lifecycle. The SubmitQueue-scoped analogue of the repo-level `core/`.

Cross-domain building blocks live outside this directory: shared entities in `entity/`, shared extensions in `extension/`, and cross-domain infrastructure in the top-level `core/`.
