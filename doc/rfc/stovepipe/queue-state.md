# Queue status

Stovepipe needs to answer two questions: is a queue enabled now, and was it enabled for a particular commit? In both cases, the answer should include the commit at which that state took effect.

## Why we need this

A client deciding whether to expect Stovepipe validation for a commit needs to know whether the queue was enabled for that commit. The queue could be disabled today even though the commit arrived while it was enabled. It could also have just been enabled, before any validation result is available. We need to record enablement changes independently of validation work so clients can distinguish these cases.

The existing information cannot answer this reliably:

- A configuration flag gives the desired setting. It does not retain when Stovepipe applied each change, or which commits an earlier setting covered.
- The current queue row gives the latest internal state. Once it changes, it cannot answer questions about earlier commits on its own.
- Request history describes validation attempts. Stovepipe batches commits, so an intermediate commit may have no request of its own. A missing request does not mean the queue was disabled.
- Last-green and validation results describe outcomes. They do not tell us whether validation was expected in the first place.

The solution is a queue-status endpoint backed by a durable record of enabled/disabled changes and their commit boundaries.

## What the endpoint returns

`GetQueueStatus` takes a queue name and an optional `change_uri`. Without a commit, it returns the latest state available in the queue summary. With a commit, it returns the state that covers that commit. The response contains one selected state, with these fields:

| Field | Meaning |
|---|---|
| `state` | `ENABLED` or `DISABLED`. |
| `effective_from_commit_uri` | The first commit covered by this state, inclusive. |
| `version` | A sequence number that increases for each enablement change. |
| `changed_at_ms` | When the change was recorded, in Unix milliseconds. This is not the commit's timestamp. |

The [proto](../../../api/stovepipe/proto/stovepipe.proto) defines the wire contract.

For example, suppose commits arrive in the order A, B, C, D, E, F, G. The queue is enabled at A, disabled at D, and enabled again at G:

| Lookup | Returned state | Effective from |
|---|---|---|
| No commit supplied | Enabled | G |
| C | Enabled | A |
| D or F | Disabled | D |
| G | Enabled | G |

C and F do not need validation requests. Their position relative to the state-change boundaries is enough to answer the lookup.

When a queue is first registered, it starts as disabled version 1, with no commit boundary. That initial state covers commits before the first enablement. An unregistered queue or missing data is an error; it does not imply disabled. If multiple changes use the same boundary, the highest version wins. Versions order changes; timestamps do not.

## How we store it

Keep internal state separate from the views used by the API, following the summary and history model used for requests:

| Record | Purpose |
|---|---|
| Existing `queue` row | Authoritative current enablement, alongside the queue's pipeline coordination fields. |
| `queue_log` | Immutable history of applied enablement changes, keyed by queue and enablement version. |
| `queue_summary` | The latest enablement change recorded in history, used for status reads. |

The queue's existing row version still protects its complete snapshot from concurrent writes. A separate enablement version increases only when enabled/disabled changes, so routine pipeline updates do not create history entries or gaps. The summary has its own storage version for concurrent projection updates; the API exposes only the enablement version.

## Applying a change

The internal transition controller prepares a change by capturing the current enablement version, resolving the queue's tip through the existing source-control integration, and recording a timestamp. That tip becomes the effective commit boundary. Retrying the prepared change keeps the same boundary and timestamp, even if new commits arrive. Asking for the existing state is a no-op.

Application follows this order:

1. Finish recording the previous state in history and its summary.
2. Conditionally replace the internal queue snapshot with the new enablement, preserving its latest pipeline coordination fields.
3. Append the committed change to history.
4. Advance the queue summary to that change.

`queuelog.Materializer.PersistLog` owns the history and summary writes. Repeating an identical change is safe, and an older change cannot move the summary backward. Conflicting content for the same version is an error.

These writes do not require a transaction across records. If the process stops after updating the queue but before recording history, the queue still contains the missing change. Initialization and transition retries finish recording it before allowing another enablement change. A write error can therefore occur after internal state has changed; retrying the same prepared change completes the work without choosing a new boundary.

Optimistic locking lets routine pipeline updates race with enablement safely. A competing enablement decision makes a prepared change stale and is rejected. The controller does not silently apply it on top of the winner. Startup registers new queues as disabled and preserves already registered state.

## Looking up a commit

The endpoint reads the summary and its matching history entry once. It uses that summary version throughout the lookup, even if a newer change is recorded during the call. Lookup never writes internal state or repairs history. If history or summary writes were interrupted, “current” means the latest state recorded in the read models; it can lag the internal queue until a writer retries.

For a commit lookup, the existing source-control integration checks that the requested commit and the latest recorded boundary belong to the queue's current lineage. The controller then walks state changes from newest to oldest and selects the first boundary that is an ancestor of, or equal to, the requested commit. A lookup without a commit needs no source-control call.

Before moving to an earlier state, the controller verifies that the requested commit actually precedes the boundary being examined. If neither is an ancestor of the other, it cannot assign a state reliably and fails the lookup. This also prevents rewritten history or a merged side branch from being mistaken for an earlier disabled interval. Supporting those cases requires an explicit ordering policy.

This approach uses commit ancestry rather than validation-batch membership. It needs no intermediate-commit index or local checkout; the amount of history examined depends on the number of enablement changes.

## Failure behavior

Missing or inconsistent data must never produce a successful disabled answer.

| Condition | RPC error |
|---|---|
| Malformed queue or commit selector | `INVALID_ARGUMENT` |
| No queue summary | `NOT_FOUND` |
| Unresolved commit or unsupported lineage | `FAILED_PRECONDITION` |
| Missing history or disagreement between history and summary | `INTERNAL` |

Dependency and cancellation errors retain their normal transport classification. If a supplied commit cannot be resolved, the whole lookup fails.

## Scope

This change records queue enablement and exposes status lookup. Recording enablement does not yet change ingestion, admission, or execution behavior. Choosing the configuration source and defining how to administer enabled/disabled transitions remain separate work, including how to reject stale configuration observations. There is no mutation RPC, CLI, or public history endpoint in this stack.

Pause is also deferred. A future pause control should describe execution behavior separately from enablement, so a paused queue can still be identified as enabled. Validation outcomes and last-green remain in their existing APIs.
