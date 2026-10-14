# Queue enablement and status

Stovepipe needs to answer two questions: is a queue enabled now, and was it enabled for a particular commit? Each answer includes the commit at which that state took effect.

## Why we need this

A client needs to know whether Stovepipe validation is expected for a commit. Today's setting is not enough: a queue can be disabled now even though an earlier commit was covered, or enabled before its first validation finishes.

The existing information cannot answer this reliably:

- A configuration flag describes the desired setting, and the current queue row holds the latest applied state. Neither alone retains earlier commit intervals.
- Request history describes validation attempts. Batching leaves intermediate commits without their own requests, so a missing request does not imply disabled.
- Last-green and validation results describe outcomes, rather than whether validation was expected.

A dedicated lookup needs durable enablement history with commit boundaries, independent of validation work.

## Enablement and pause

Enablement defines which commits are covered by Stovepipe. Pause is a temporary operational control over whether a queue accepts or advances work. An enabled queue can be paused and still owe validation results for its covered commits; enabled does not promise that work is progressing or that results are green.

For example, if a queue is enabled at A and later paused while validating C, the status for C remains enabled from A. Unpausing resumes work without creating a new enablement boundary. Disabling at D instead changes coverage from D onward, while preserving the answers for earlier commits.

Future pause management should follow the [queue pause RFC](../submitqueue/queue-pause-and-unpause.md), which distinguishes refusing new requests from freezing accepted work. That RFC covers SubmitQueue; Stovepipe's ingestion and processing behavior still needs its own design.

Pause can use the same internal-state, summary and history pattern described below, with separate operational fields and history events. Pause and unpause must not advance the enablement version or change commit boundaries. Keep pause out of the enablement enum, and expose it as current operational status rather than mixing it into a commit-specific enablement answer. A future administration API can manage both controls independently; this lookup remains read-only.

## What the endpoint returns

`GetQueueStatus` takes a queue name and an optional `change_uri`. Omitting the commit selects the latest queue summary; supplying it selects the state covering that commit. The response contains one state:

| Field | Meaning |
|---|---|
| `state` | `ENABLED` or `DISABLED`. |
| `effective_from_commit_uri` | First covered commit, inclusive. |
| `version` | Sequence number that increases for each enablement change. |
| `changed_at_ms` | Recorded change timestamp in Unix milliseconds, distinct from commit time. |

See the [proto](../../../api/stovepipe/proto/stovepipe.proto) for the wire contract.

Suppose commits arrive in order A through G, with enablement changes at A, D and G:

| Lookup | State | Effective from |
|---|---|---|
| No commit supplied | Enabled | G |
| C | Enabled | A |
| D or F | Disabled | D |
| G | Enabled | G |

C and F need no validation requests. Their ancestry relative to the boundaries determines the answer.

Registration starts a queue as disabled version 1, with no boundary; this covers commits before its first enablement. Missing state is an error, not disabled. If changes share a boundary, the highest version wins. Versions order changes; timestamps do not.

## Internal state, summary and history

Follow the model used for request summaries and history, while keeping the internal queue authoritative:

| Record | Purpose |
|---|---|
| Existing `queue` row | Current enablement alongside pipeline coordination fields. |
| `queue_log` | Immutable applied changes, keyed by queue and enablement version. |
| `queue_summary` | Current read view derived from those changes. |

The queue row and summary each have a storage version for optimistic locking. The API exposes a separate enablement version, which advances only for enabled/disabled changes. Routine pipeline updates create no enablement history entries or gaps.

The transition controller captures the queue's tip and a timestamp once when preparing a change. Retries keep those values; requesting the existing state is a no-op. Application first finishes recording the previous state, then conditionally updates the queue and records the new state through `queuelog.Materializer.PersistLog`.

History is written before its summary. If either write fails after the queue update, the queue retains the missing change. Initialization and transition retries repair the read models before another enablement change can replace it. Repeating an identical change is safe, and older changes cannot move the summary backward. Concurrent pipeline updates preserve their coordination fields; competing enablement decisions are rejected as stale.

## Commit lookup and consistency

Lookup reads one summary and its matching history entry, and uses that version throughout the call. It never writes or repairs state. “Current” therefore means the latest state in the read models, which may lag the internal queue until a writer retries.

For a supplied commit, the existing source-control integration verifies that the commit and latest boundary belong to the queue's current lineage. The controller walks changes newest-first and selects the first boundary that is an ancestor of, or equal to, the commit. Before walking backward, it verifies that the commit precedes the boundary; incomparable commits and unsupported history rewrites fail instead of becoming disabled. This resolves intermediate commits without a commit index or local checkout. Current-only lookup needs no source-control call.

| Failure | RPC error |
|---|---|
| Malformed selector | `INVALID_ARGUMENT` |
| No queue summary | `NOT_FOUND` |
| Unresolved commit or unsupported lineage | `FAILED_PRECONDITION` |
| Missing or inconsistent history | `INTERNAL` |

Dependency and cancellation errors retain their transport classification. An unresolved commit fails the whole call.

## Scope

This stack records enablement and exposes status lookup. It does not enforce ingestion or execution controls. Configuration freshness, administration, pause enforcement and public history access remain separate work. Validation results and last-green stay in their existing APIs.
