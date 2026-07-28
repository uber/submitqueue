# Runway

Runway owns the merge queues defined by the external contract in
[`api/runway/messagequeue`](../api/runway/messagequeue): it consumes merge-conflict-check and merge
requests, performs the work, and (eventually) publishes the result to the corresponding signal queue.
SubmitQueue is a client of these queues.

Runway is a single service (the domain *is* the service); its controllers live directly under
[`controller/`](controller). It consumes Runway's merge queues:

- `merge-conflict-check` — dry-run check that an ordered sequence of merge steps applies cleanly, without committing.
- `merge` — committing merge: apply and commit the ordered steps.

Each controller deserializes the `MergeRequest`, obtains a `Merger` for the request's queue from the [`merger`](extension/merger) extension, applies the ordered steps, and publishes a `MergeResult` to the corresponding signal queue (`merge-conflict-check-signal` / `merge-signal`). `merge` commits and reports the produced revisions; `merge-conflict-check` is a dry run that reports mergeability with empty outputs.

## Merger extension

[`extension/merger`](extension/merger) is the pluggable merge contract. Implementations:

- [`git`](extension/merger/git) — a git-CLI backend that honors each step's strategy (REBASE, SQUASH_REBASE, MERGE, PROMOTE; DEFAULT resolves to a per-instance default). See its [README](extension/merger/git/README.md).
- `noop` — always-succeeds stub for local development.

## Failure handling

A merge outcome the controller can name is published as a `FAILED` result and acked, not retried: a merge conflict (`merger.ErrConflict`) or an invalid request (`merger.ErrInvalidRequest` — unknown strategy, malformed change URI, invalid PROMOTE composition). The `merger.IsTerminal` helper draws that line. Any other error is an infrastructure fault and is nacked for retry.

Because Runway is stateless and the sole responder on the client's correlation id, a request that exhausts retries (or hits an unexpected fault) must still resolve the client. The inbound topics dead-letter by default; the [`dlq`](controller/dlq) reconciler drains those `_dlq` topics and republishes a `FAILED` `MergeResult` to the signal topic so the correlation id never hangs.
