# Record stage

`record` turns a terminal build into a durable greenness fact.

- In Phase 1 it records whole-repository greenness, advances the Queue's last-green bookmark when the result is green, releases the Request's validation slot, notifies downstream systems, and completes the Request.
- In Phase 2 the same stage records project greenness and notifies downstream systems at project granularity. Mentioned in this doc, but to be expanded on before future implementation.

See [workflow.md](../workflow.md) for the complete pipeline, [build.md](build.md) for how builds are created, and [buildsignal.md](buildsignal.md) for the terminal-only handoff into this stage.

`record` owns persistence and publication of greenness facts. It does not decide build scope, poll a build, interpret a target graph, or map targets to projects. Those responsibilities belong to `process`, `buildsignal`, and `analyze`.

## Scope of this design

This document fully specifies the Phase 1 path needed for whole-repository greenness. It also outlines how Phase 2 will be accommodated at a high level.

In Phase 1, `record` finishes the Request itself: after persisting the whole-repository fact it moves the Request straight from `processing` to `recorded_green` or `recorded_not_green`. There is no `analyzing` state and no fan-out yet — that machinery arrives with Phase 2, which inserts a non-terminal `analyzing` state between `processing` and the recorded states, retargets record's final CAS at it, and adds a publish to `analyze`, which then owns the terminal transition. The insertion is confined to record's last step and the state enum; facts, Queue reconciliation, and Hooks are untouched.

## Input, partitioning, and re-entry

`record` consumes a `RecordBuild` message containing a build id. `buildsignal` publishes it only after persisting a terminal `Build.Status`. The build id is the runner-minted `Build.ID`, so `record` loads the Build with a direct key lookup and then loads its Request through `Build.RequestID`.

The record topic is partitioned by **request id**, not build id. Phase 1 has one build per Request, so the two are equivalent today; the choice means Phase 2's several project builds per Request arrive serialized, keeping completion bookkeeping single-writer, with no topic change. Partitioning only reduces contention — correctness still comes from immutable facts and optimistic locking.

Phase 1 builds all carry the whole-repository scope (the zero scope) and run the path in this document; Phase 2 branches on the Build's immutable scope to write a project fact instead (see the reservation below). Project identity is deferred to `analyze.md`, with one rule fixed now: a stable project id (what greenness is *about*) is distinct from the opaque build scope (what a runner *builds*), and `record` never infers one by parsing the other.

## Greenness is an immutable fact

A greenness fact answers "how broken was this scope at this Queue URI?" Its identity is:

```
(queue, uri, project)
```

`project` is empty for whole-repository greenness and is a stable project id in Phase 2. The key is derivable from identity the controller already holds, so storage requires no query by attribute or secondary index.

The fact contains:

| Field       | Meaning                                                                             |
| ----------- | ----------------------------------------------------------------------------------- |
| `Queue`     | Stable Queue name that namespaces the validation                                    |
| `URI`       | Opaque commit URI under validation                                                  |
| `Project`   | Empty for the whole repository; stable project id for Phase 2                       |
| `Degree`    | Health degree in the closed interval `[0, 1]`; `0` is green and `1` is fully broken |
| `RequestID` | Request that established the fact                                                   |
| `BuildID`   | Terminal build whose verdict established the fact                                   |
| `CreatedAt` | Millisecond timestamp at which the fact was first recorded                          |

Facts are create-only. A duplicate create for the same identity is reconciled by loading the existing row:

- Same Request → the existing fact is authoritative; continue from it. This absorbs redelivery and duplicate builds, even ones reporting different verdicts. First recorded fact wins.
- Different Request → the `(Queue, URI)` ingest dedup invariant has been violated. Return an error rather than overwrite history.

Absence remains distinct from degree `0`. Callers gating deployments must treat absence as not green.

### Phase 1 degree mapping

MVP whole-repository builds use only the endpoints:

| Terminal build status | Degree |
| --------------------- | ------ |
| `succeeded`           | `0`    |
| `failed`              | `1`    |
| `cancelled`           | `1`    |

Intermediate degrees are reserved for project analysis and deferred with the project mapping contract; Phase 1 does not manufacture fractional values.

## Phase 1 algorithm

For a delivery carrying build id `B`:

```
1. Load Build B.
   - ErrNotFound       -> retryable; buildsignal's write may not be visible yet.
   - other store error -> return raw; the classifier decides.

2. Require a terminal Build.Status.
   - non-terminal -> return a non-retryable invariant error. buildsignal must not publish it.

3. Load Request R = RequestStore.Get(Build.RequestID).
   - ErrNotFound       -> retryable; redelivery converges on a lagging read.
   - other store error -> return raw.

4. Inspect R.State.
   - superseded -> ack; no fact or notification is written.
   - recorded_green / recorded_not_green -> ack; the terminal write is the algorithm's last
     durable step, so a terminal state proves every prior effect already happened.
   - processing -> continue.
   - accepted -> retryable: the Build proves process admitted R, so this is a stale pre-admit
     read (the same lagging-read phenomenon as step 3). A genuine state-machine fault still
     dead-letters at MaxAttempts.
   - anything else -> return a non-retryable invariant error.

5. Map Build.Status to a whole-repository degree and create the Greenness fact keyed by
   (R.Queue, R.URI, empty project).
   - ErrAlreadyExists -> load and reconcile the existing immutable fact.
   - other store error -> return raw.

6. Reconcile the Queue in one CAS retry loop:
   a. Decrement in_flight_count, clamping at zero.
   b. If the persisted fact is green and LastGreenRequestID is empty or older than R.ID
      per entity.CompareRequestID, set LastGreenURI = R.URI and LastGreenRequestID = R.ID.
   c. If no field changes, skip the write.

7. Notify the Hooks extension with the Greenness identity.
   - return errors raw; the hook backend's classifier decides retryability.

8. CAS R: processing -> recorded_green if the persisted fact is green, else recorded_not_green.
   - newVersion = oldVersion + 1; assign only after Update succeeds.
   - ErrVersionMismatch -> retryable; reload and run the algorithm again.

9. ack.
```

Every decision after step 5 uses the **persisted** fact, not the status from the current delivery: if duplicate builds disagree, the first immutable fact controls the Queue bookmark, Hooks event, and final state.

The durable order is fact → Queue → Hooks → Request terminal. The terminal write comes last deliberately: it is the completion marker, so a Request read as recorded proves the fact, slot release, bookmark, and hook all happened, and step 4 can ack terminal states unconditionally. The cost is that a crash between steps 7 and 8 re-notifies the hook on retry — absorbed by the greenness identity key. Everything before the terminal write is recognize-and-skip on retry: the fact reloads, the clamped decrement bottoms out at zero, and the bookmark guard skips equal-or-older candidates.

Phase 2 changes only step 8: the CAS retargets `processing → analyzing` and a publish to `analyze` follows it, with `analyze` owning the terminal transition (see the reservation below).

### Slot release keeps the existing count

`record` releases the build slot in the queue row by CAS-decrementing `in_flight_count`, clamped at zero, before the Request's terminal write.  Since Queue and Request are separate writes - a crash between these two writes may release an extra slot (preferable to holding an extra slot, which could lead to a deadlock). Aligns with existing DLQ reconciler behavior.

As noted in process.md, we may expand this to track leases by request ID in the future.

### Last-green advancement (Queue bookmark)

The bookmark only moves forward. The Queue gains one field, `LastGreenRequestID` — the request id that owns the current `LastGreenURI`. On a green fact, step 6 adopts the pair `(R.URI, R.ID)` only when the stored id is empty or older than `R.ID` per `entity.CompareRequestID` — the same ingest-order comparison `ingest` and `process` already use for coalescing.

A failed or cancelled build releases its slot but never moves the bookmark.

## Request lifecycle

Phase 1 (planned MVP work) uses the states in [stovepipe/entity/request.go](../../../../stovepipe/entity/request.go). `record` sees `processing` on the happy path and CASes it directly to `recorded_green` or `recorded_not_green` per the persisted fact; `superseded` and the recorded states ack at step 4, and `accepted` is the retryable stale read described there.

Phase 2 inserts a non-terminal `analyzing` state ("whole-repository fact recorded, slot released, project analysis in flight") between `processing` and the recorded states, and broadens the recorded states to mean "all planned facts recorded".

## Hooks

Hooks are the notification boundary, not the source of truth; the GreennessStore is authoritative and commits before any hook fires.

Following the extension rule "identity in, resolve internally," the Hooks contract takes a thin greenness identity rather than a controller-assembled external payload:

```
Notify(ctx, GreennessRef{Queue, URI, Project}) error
```

An implementation resolves the immutable fact through dependencies injected at Factory construction, translates it to its external representation, and publishes it. Service wiring owns Factory routing and may select an implementation by Queue name.

Delivery is at-least-once with the greenness identity as the idempotency key; hook implementations or their downstreams must absorb duplicates. "Fire-and-forget" refers to downstream consumption, not the publish itself: `record` never waits for consumers to act on an event, but a failed `Notify` fails the delivery and is retried — the Request cannot complete until the publish succeeds.

## Phase 2 plans

Phase 2 can expand upon the record phase:

- **Pipeline**: the final CAS retargets `processing → analyzing`, and `record` then publishes the Request id to `analyze` — for green and not-green facts alike, since a failed build is when project attribution matters most. `analyze` owns the terminal transition once all planned project facts exist. The message stays id-only and the consumer idempotent.
- **Project builds**: `record` runs the same load-fact-notify flow, keying the fact with the stable project id attached to the Build and notifying Hooks with that identity. The Queue's in-flight count and `LastGreenURI` are whole-repository concerns and stay untouched.

Other activities like determining project identity, target-graph retrieval, completion tracking, intermediate degree semantics — belongs to `analyze.md`. Two storage boundaries are fixed now.

`GreennessStore` is key/value-shaped:

- `Create(ctx, greenness)` creates one immutable fact and returns `ErrAlreadyExists` when its composite identity is taken.
- `Get(ctx, queue, uri, project)` retrieves one fact by full identity and returns `ErrNotFound` when absent. `project` field reserved for future state.

There is no `Update`, list, filter, or query-by-degree operation. A corrected verdict is a new Request/fact, not an in-place rewrite of historical truth.

`QueueStore` and `RequestStore` retain their existing generic CAS `Update` methods. Version arithmetic stays in the controller: compute `newVersion = oldVersion + 1`, pass both versions to the store, and assign the in-memory version only after success.

Phase 2's latest-green project mapping, if adopted, is a separate key/value store rather than an index hidden inside GreennessStore.

## Message-queue additions

| Topic key | Message                 | Producer      | Consumer | Partition key |
| --------- | ----------------------- | ------------- | -------- | ------------- |
| `record`  | `RecordBuild{build_id}` | `buildsignal` | `record` | Request id    |

## Idempotency and competing outcomes

At-least-once delivery is safe by construction:

- **Build or Request not visible yet** — retry until the producing write is visible.
- **Greenness already created** — load it and continue from the authoritative fact.
- **Queue already reconciled** — the clamped decrement bottoms out at zero and the bookmark guard skips equal-or-older candidates.
- **Hook notified, then crash before the terminal write** — retry re-notifies; the greenness identity dedups.
- **Duplicate builds for one Request** — the first Greenness create wins; later builds cannot overwrite it.
- **Request already terminal** — ack. The terminal write is the last durable step, so nothing can be missing.

An existing fact from a different Request or a non-terminal Build is an invariant violation, not an expected control-flow outcome. A Request read as `accepted` is neither: the Build proves admission happened, so it is a stale read that redelivery converges.

## Error classification

Plain errors remain non-retryable by default. Controllers return extension errors raw so the composed backend classifiers decide whether infrastructure failures are retryable. The controller overrides only cases whose meaning is known locally:

| Failure                                         | Disposition   | Reason                                                                  |
| ------------------------------------------------ | ------------- | ----------------------------------------------------------------------- |
| Build not found                                  | retryable     | `buildsignal` may have published ahead of a lagging read                |
| Request not found                                | retryable     | the Build references an older Request write                             |
| Request read as `accepted`                       | retryable     | stale pre-admit version of the row; the Build proves admission happened |
| Queue CAS version mismatch                       | retryable     | reload and reapply idempotent reconciliation                            |
| Request CAS version mismatch                     | retryable     | reload and re-evaluate the state                                        |
| Non-terminal Build or invalid Request state      | non-retryable | producer/state-machine invariant violation                              |
| Hooks, GreennessStore, QueueStore, RequestStore  | raw error     | backend classifier has the required failure knowledge                   |

## DLQ and fail-closed behavior

The record DLQ must not call the current generic `failRequest`: its payload names a terminal Build, so the actual verdict is already known, and replacing a successful build with a conservative not-green fact would falsify durable history.

`record_dlq` runs the same idempotent reconciliation algorithm from the Build id under `errs.AlwaysRetryableProcessor`:

- If the Build is terminal, persist and propagate its actual immutable fact.
- If Build or Request storage is temporarily unavailable, keep retrying.
- If the payload is malformed or the Build is permanently missing, the message is a poison reconciliation item requiring an operational alert; there is no trustworthy Request identity to mutate.

The Queue slot is released before Hooks, so a broken notification backend cannot wedge validation of newer heads; the Request stays `processing` until reconciliation completes the hook and the terminal write.

Earlier-stage DLQ reconciliation still forces a conservative degree `1` when no terminal verdict exists: create the whole-repository fact (with empty `BuildID` — the marker of a conservative rather than observed verdict), release the slot with the same clamped decrement, and transition to `recorded_not_green`. Changing state without writing the fact would leave externally visible greenness absent, failing the fail-closed contract.

First-fact-wins makes the conservative verdict final, deliberately superseding `workflow.md`'s remark that "a late successful update wins cleanly over the conservative one": a late green for a fail-closed URI is dropped, at bounded cost — the branch keeps moving, and the next head re-establishes greenness on its own Request. Revise `workflow.md` alongside this design.

## Edge cases

- **Head equals last-green.** The build still produces a terminal verdict. A green result may share the bookmark's URI; the guard compares request ids, not URIs, so an equal-or-older candidate skips and nothing regresses.
- **Cancelled build.** Stovepipe never initiates cancellation, but a backend may still report it. Record degree `1`, release the slot, complete the Request.
- **History rewrite while a build runs.** The Request keeps the strategy and URI pinned at admission; record stores the fact about that immutable URI. A later head is handled independently by `process`.
- **Late successful result after fail-closed terminal.** The terminal `recorded_not_green` Request wins; record acks without rewriting greenness or reopening the pipeline.
- **Hook succeeds, terminal CAS interrupted.** Retry re-notifies the hook; the greenness identity makes the duplicate safe.
- **Terminal CAS succeeds, ack fails.** Redelivery reads the recorded state at step 4 and acks.
