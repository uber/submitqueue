# SubmitQueue Orchestrator

The orchestrator runs the SubmitQueue land pipeline. Its complete consumed-stage and publish-only topology is declared in [`pipeline.go`](pipeline.go). It consumes SubmitQueue-owned internal topics, Runway-owned result topics, and the shared hook topic.

## Pipeline stages

The pipeline is queue-driven: each stage consumes one topic, advances one entity, and publishes to the next topic.

- **start** — receives `LandRequest` from the gateway, persists the `Request` entity, and emits `Started`.
- **cancel** — records cancellation intent and hands affected batches to speculation for best-effort cancellation.
- **validate** — checks for duplicates, resolves change metadata, and publishes a `LandRequest` to Runway's `land-conflict-check` topic.
- **land-conflict-check-signal** — correlates the dry-run result, fails the request on conflict, or forwards it to batching.
- **batch** — creates an inert batch attempt and hands it to dependency analysis.
- **dependency-analysis** — enrols requests, computes dependencies, and promotes the selected batch attempt.
- **speculate** — reconciles queue-wide path state, decides outcomes, and allocates speculative builds.
- **build** — triggers a CI build for a speculative path.
- **buildsignal** — polls or receives CI state, records the result, wakes `speculate`, and holds non-terminal deliveries until the next poll.
- **land** — publishes a committing `LandRequest` to Runway's `runway-land` topic.
- **land-signal** — correlates the land result and fans out to `conclude` and back to `speculate`.
- **conclude** — maps the terminal batch state to the request states.
- **submitqueue-hook** — dispatches lifecycle hook events to configured integrations.
- **DLQ reconcilers** — one per primary consumed topic, driving stuck requests/batches to a conservative terminal `failed` state.

The orchestrator publishes request-log entries to `log`, but does not consume or persist them; the gateway owns that stage. It also publishes full cross-service requests to Runway's land-conflict-check and land topics.
