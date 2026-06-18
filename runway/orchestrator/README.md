# Runway Orchestrator

Consumes Runway's merge queues (defined in [`api/runway/messagequeue`](../../api/runway/messagequeue)):

- `merge-conflict-check` — dry-run check that an ordered sequence of merge steps applies cleanly, without committing.
- `merge` — committing merge: apply and commit the ordered steps.

Both controllers currently deserialize the `MergeRequest` off the queue and log it; performing the
merge and publishing a `MergeResult` to the corresponding signal queue is not wired yet.
