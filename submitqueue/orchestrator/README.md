# SubmitQueue Orchestrator

The orchestrator runs the SubmitQueue land pipeline. It consumes the internal topics declared in `submitqueue/core/topickey/` and advances requests and batches through the stages that lead from `accepted` to a terminal state.

## Pipeline stages

The pipeline is queue-driven: each stage consumes one topic, advances one entity, and publishes to the next topic.

- **start** — receives `LandRequest` from the gateway, persists the `Request` entity, and emits `Started`.
- **validate** — checks for duplicates, resolves change metadata, and publishes a `MergeRequest` to Runway's `merge-conflict-check` topic.
- **mergeconflictsignal** — correlates the dry-run result, fails the request on conflict, or forwards it to batching.
- **batch** — groups the request into a `Batch` with its dependencies.
- **speculate** — decides which speculative paths to validate (CI) versus land directly.
- **build** — triggers a CI build for a speculative path.
- **buildsignal** — records the CI result and loops back to `speculate`.
- **merge** — publishes a committing `MergeRequest` to Runway's `runway-merge` topic.
- **mergesignal** — correlates the merge result and fans out to `conclude` and back to `speculate`.
- **conclude** — maps the terminal batch state to the request states.
- **log** — persists gateway-owned request-log events published by the orchestrator.
- **DLQ reconcilers** — one per primary consumed topic, driving stuck requests/batches to a conservative terminal `failed` state.

See [doc/rfc/submitqueue/workflow.md](../../doc/rfc/submitqueue/workflow.md) for the full pipeline diagram and ownership rules.
