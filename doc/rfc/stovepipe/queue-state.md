# Queue enablement and status lookup

Callers need the enablement covering a commit, including commits inside a validation batch that have no request of their own. A current flag loses earlier enable/disable intervals, request history describes validation work, and last-green describes validation outcomes. Queue status returns the materialized applied enablement and the commit boundary at which it changed.

## Contract

`GetQueueStatus(queue, change_uri)` returns one `state`: the latest materialized enablement when the commit is omitted, or the enablement covering the supplied immutable commit URI. The state contains enabled/disabled enablement, its internally assigned sequence, an inclusive effective commit boundary and an application timestamp in Unix milliseconds. Its version identifies the selected enablement occurrence, not the summary's projection version. See the [proto](../../../api/stovepipe/proto/stovepipe.proto).

Explicit registration establishes disabled version 1 without a boundary. Missing or unknown state is not disabled. Boundaries move forward along the queue lineage; at equal boundaries, the newest enablement version wins. Timestamps do not order state. A later transition at the same boundary can change the answer relative to a newer returned version.

The lookup reads queue summaries and history only. It never updates internal queues or repairs projections. The summary can lag an internal commit when history or projection persistence fails, so “current” means the latest materialized applied state. A retained event newer than the pinned summary does not change that call's answer.

The RPC uses generated `UNIMPLEMENTED` behavior until its lookup controller is wired.

## Error semantics

The endpoint returns `INVALID_ARGUMENT` for malformed selectors, `NOT_FOUND` when no queue summary exists, `FAILED_PRECONDITION` for unresolved commits or unsupported lineage, and `INTERNAL` for inconsistent history or projections. Dependency and cancellation failures preserve their identity for transport classification. A supplied but unresolved commit fails the entire call.


## Scope

Enablement remains separate from validation outcomes, admission and execution controls. Configuration integration, mutation administration, public history/replay and pause are deferred. The contract contains neither pause nor last-green. A future pause control requires its own execution semantics and must not become an enablement enum value. No runtime execution or admission behavior changes when enablement is recorded.
