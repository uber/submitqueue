# Runway

Runway owns the merge queues defined by the external contract in
[`api/runway/messagequeue`](../api/runway/messagequeue): it consumes merge-conflict-check and merge
requests, performs the work, and (eventually) publishes the result to the corresponding signal queue.
SubmitQueue is a client of these queues.

Runway is a single service (the domain *is* the service); its controllers live directly under
[`controller/`](controller). It consumes Runway's merge queues:

- `merge-conflict-check` — dry-run check that an ordered sequence of merge steps applies cleanly, without committing.
- `merge` — committing merge: apply and commit the ordered steps.

Both controllers currently deserialize the `MergeRequest` off the queue and log it; performing the
merge and publishing a `MergeResult` to the corresponding signal queue is not wired yet.
