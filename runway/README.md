# Runway

Runway owns the land queues defined by the external contract in
[`api/runway/messagequeue`](../api/runway/messagequeue): it consumes land-conflict-check and land
requests, performs the work, and (eventually) publishes the result to the corresponding signal queue.
SubmitQueue is a client of these queues.

Runway is a single service (the domain *is* the service); its controllers live directly under
[`controller/`](controller). It consumes Runway's land queues:

- `land-conflict-check` — dry-run check that an ordered sequence of land steps applies cleanly, without committing.
- `land` — committing land: apply and commit the ordered steps.

Each controller deserializes the `LandRequest`, obtains a `Lander` for the request's queue from the [`lander`](extension/lander) extension, applies the ordered steps, and publishes a `LandResult` to the corresponding signal queue (`land-conflict-check-signal` / `land-signal`). `land` commits and reports the produced revisions; `land-conflict-check` is a dry run that reports landability with empty outputs.

## Lander extension

[`extension/lander`](extension/lander) is the pluggable land contract. Implementations:

- [`git`](extension/lander/git) — a git-CLI backend that honors each step's strategy (REBASE, SQUASH_REBASE, MERGE, PROMOTE; DEFAULT resolves to a per-instance default). See its [README](extension/lander/git/README.md).
- `noop` — always-succeeds stub for local development.

## Failure handling

A land outcome the controller can name is published as a `FAILED` result and acked, not retried: a land conflict (`lander.ErrConflict`) or an invalid request (`lander.ErrInvalidRequest` — unknown strategy, malformed change URI, invalid PROMOTE composition). The `lander.IsTerminal` helper draws that line. Any other error is an infrastructure fault and is nacked for retry.

Because Runway is stateless and the sole responder on the client's correlation id, a request that exhausts retries (or hits an unexpected fault) must still resolve the client. The inbound topics dead-letter by default; the [`dlq`](controller/dlq) reconciler drains those `_dlq` topics and republishes a `FAILED` `LandResult` to the signal topic so the correlation id never hangs.
