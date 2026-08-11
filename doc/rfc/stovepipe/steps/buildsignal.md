# Buildsignal stage

`buildsignal` polls the build-runner until a build reaches a terminal state, records the status, and — once the build is terminal — releases the queue's build slot, projects the outcome onto the `Request`, and publishes the **request id** to `record`. See [workflow.md](doc/rfc/stovepipe/workflow.md) for where it sits in the pipeline and [build.md](doc/rfc/stovepipe/steps/build.md) for how the build it polls was triggered.

It handles only the poll loop: it does not decide build strategy, write greenness, or map targets to projects — those are `process`'s, `record`'s, and `analyze`'s jobs. It reports the *fact* (this request's build ended succeeded, failed, or cancelled); `record` derives what that fact *means* for greenness.

`buildsignal` is structurally the same controller as `submitqueue/orchestrator/controller/buildsignal/buildsignal.go`, and the "Flow", "Status delivery", and "Polling primitive" sections of [doc/rfc/submitqueue/build-runner.md](doc/rfc/submitqueue/build-runner.md) are the reference rationale it reuses directly. The entity and storage shapes it depends on are defined in [build.md](doc/rfc/stovepipe/steps/build.md#entity-and-storage-additions-needed); this doc introduces none of its own.

## Input and re-entrancy

`buildsignal` consumes a build id, published by `build` — once per phase, since Phase 1 (whole-repo) and Phase 2 (per-project) each run their own `build` → `buildsignal` cycle against builds tied to the same `Request` (see [workflow.md](doc/rfc/stovepipe/workflow.md#workflow)).

Its logic does not branch on phase: it loads the `Build`, polls it toward terminal, persists the result, and publishes the request id onward to `record`. What differs between phases is what `record` does with that publish (whole-repo vs. per-project greenness) — not anything `buildsignal` decides.

`buildsignal` is the sole writer of `Build.Status`/`Build.Version` after `build` creates the row (see [build.md](doc/rfc/stovepipe/steps/build.md#input-partitioning-and-the-single-writer-property)). It reads `Request` via `RequestStore.Get` (for `R.Queue`, to resolve the build-runner) and writes it exactly once, at the terminal transition, to record the build's outcome — the one `Request.State` write outside `process` and the DLQ reconciler.

Its early-exit guard is deliberately narrower than `State.IsTerminal()`: it proceeds when the request is `processing` **or** already carries a build outcome. The second case matters because a redelivery after the outcome was stamped but before the `record` publish landed must re-publish rather than drop the signal; everything it re-runs is a no-op (the status is unchanged, the outcome is already recorded, the slot is not released twice) and the `record` publish is idempotent.

The states the guard actually rejects are terminal-without-an-outcome — today `superseded` alone, which `process` sets only from `accepted`, so it cannot reach the poll loop at all. The guard is kept rather than dropped because the slot release in step 7a is conditioned on *not* already having an outcome, so a superseded request arriving here would decrement a slot that superseding never claimed.

## Algorithm

For a delivery carrying build id `B`:

```
1. Load Build B from the build store.
   - ErrNotFound       -> return raw; non-retryable (storage is read-after-write consistent; see [storage README](stovepipe/extension/storage/README.md)).
   - other store error -> return raw; classifier decides.

2. Load Request R = store.Get(Build.RequestID) — needed for R.Queue to resolve the build-runner.
   - ErrNotFound -> return raw; non-retryable, same as step 1 — the Build's existence proves the
     Request write is already committed, so a miss here is a storage defect, not a lagging read.

3. If R.State is neither processing nor already an outcome: ack and return.
   - a request reaches the poll loop only after process admitted it, so it is processing —
     or already carries this build's outcome on a redelivery, which falls through so the
     signal is re-published to record instead of dropped.
   - the only other terminal-without-an-outcome state is superseded, and process supersedes
     solely from accepted, so it cannot occur here. Guarded anyway: step 7a would otherwise
     release a build slot the request never claimed, since superseding consumes none.
   - mirrors submitqueue's halted-batch short-circuit in buildsignal.go.

4. Resolve the build-runner: buildRunner = Factory.For(Config{QueueName: R.Queue}).
   - lookup failure -> non-retryable (config error, same as build stage).

5. Poll: status, metadata, err := buildRunner.Status(ctx, entity.BuildID{ID: B})
   - B is Build.ID, the runner-assigned id minted at Trigger — one id end to end (see build.md).
   - Status may fail transiently (runner unavailable, unknown build id) -> return raw; classifier decides.
   - metadata is BuildMetadata (caller-supplied, provider-echoed); the poll loop's own control flow
     (steps 6-8) doesn't need it to decide when to stop polling, but it isn't dropped on the floor —
     see "Status" below for what it's for and how a future check could use it.

6. Reconcile the polled status against the stored row:
   - status == Build.Status -> no-op poll: skip the write, continue with the stored status.
   - Build.Status already terminal, status differs -> terminal is WRITE-ONCE: do not overwrite;
     continue with the STORED status as authoritative (see Edge cases).
   - otherwise persist via BuildStore.Update(ctx, Build{...Status: status}, oldVersion, newVersion):
     - newVersion = Build.Version + 1; assign Build.Version = newVersion only on success.
     - ErrVersionMismatch -> return with its declaration-level retryable classification (a concurrent writer moved the row; reload and re-check).
     - with the write-once rule, accepted -> running -> {succeeded|failed|cancelled} is monotonic
       by mechanism, not by assumption about the backend.

7. If the stored status is terminal, and R does not already carry an outcome:
   a. Release the queue's build slot: CAS-decrement Queue.in_flight_count, clamped at zero.
      - failure here aborts the step: R must not go terminal while still holding a slot.
   b. CAS R from processing to the outcome the stored status projects onto it:
      succeeded -> succeeded, failed -> failed, cancelled -> cancelled. First writer wins.
   Then publish R.ID to the record topic, partitioned by request id; ack, return.
   No re-publish to buildsignal.
   - record loads the Request directly by this key and derives greenness from its outcome,
     so it never reaches a Build and no reverse lookup from Request to its builds is needed.
   - the message id is the request id, so a redelivery republishing the same terminal signal
     dedups into the original message; record is idempotent regardless.
   - publish failure -> return raw (non-retryable); the outcome is persisted, operational
     republish recovers.

8. Else hold the delivery for delayMs (postpone: the same message redelivers after the delay):
   - delayMs = pollDelay(status): shorter while running, longer while accepted.
   - a hold is a postpone, not a nack — the redelivery does not count toward the retry
     limit, so polling never burns retry_count toward the DLQ.
   - the held message is a barrier for its partition (the build id), so each build keeps
     exactly one poll chain; no message ids are minted and no new rows are written per tick.
```

**Why the slot is released before the outcome write, and why a failed release aborts it**: `Queue` and `Request` are separate entities with no cross-entity transaction, so the ordering picks which crash failure mode we accept. Both rules serve one invariant — *the request must not go terminal while still holding a slot* — because a terminal request is skipped by redelivery and by the DLQ reconciler alike, so nothing would ever decrement it. Failing this way leaves the request non-terminal: redelivery re-runs both steps and decrements again, transiently over-admitting by one slot until the zero clamp reconverges. Over-admission is the failure mode this pipeline already prefers, for the same reason and in the same words as the DLQ reconciler (see [process.md](doc/rfc/stovepipe/steps/process.md#in_flight_count-integrity)).

**Why `buildsignal` releases the slot rather than `record`**: the gate `process` claims is a *build* slot — it exists to bound concurrent builds per Queue — and once the build is terminal the build is over. Releasing here also keeps the invariant *a terminal `Request` has already released its slot*, which is what makes the DLQ reconciler's early-return on terminal requests safe.

**Why `record` hears only terminal signals**: `record` has no non-terminal work — by its own contract a non-terminal signal would be a pure no-op — and step 7 already branches on terminality to decide whether to keep polling, so gating the publish costs nothing and spares `record` a no-op delivery on every poll tick of every running build. Crash-safety is unaffected: a crash between the terminal `Update` and the publish redelivers the message; step 5 re-polls (the runner reports the same terminal status), step 6 no-ops, step 7 publishes. This is a deliberate divergence from SubmitQueue, whose buildsignal republishes to `speculate` on every tick — sound there because speculate is a state machine that may act on any signal; stovepipe has no such consumer.

**Why step 6 guards on status and makes terminal write-once**: an unchanged status skips the CAS write entirely, so a long build being polled every couple of seconds doesn't churn `Build.Version` on every tick — the version only advances on a real state transition. The write-once rule exists because CAS alone cannot provide it: optimistic locking defends against *concurrent* writers, but a later delivery that polls a flaky backend and sees a different terminal status would CAS cleanly against the current version and overwrite (see Edge cases). A given `Build` has a single poll partition (see [Partitioning](doc/rfc/stovepipe/steps/build.md#partitioning)), so the only writer racing the CAS is a redelivery of the same message (e.g. after a lapsed visibility lease); `ErrVersionMismatch` carries a retryable classification and converges on redelivery.

## Status: shaped like SubmitQueue's, not shared code

`Status` is not a point of conceptual divergence from SubmitQueue: both domains' `BuildRunner`s poll by the same opaque, runner-minted id with the same `Status(ctx, buildID) (BuildStatus, BuildMetadata, error)` signature. That similarity stays a shape, not a shared `platform/extension/buildrunner` interface — each domain keeps its own `BuildRunner` (including its own local `Status`), with reuse pushed to a shared backend implementation instead (see [build.md](doc/rfc/stovepipe/steps/build.md#alternatives-considered-for-sharing-the-contract)). `BuildMetadata` is caller-supplied and provider-echoed — the runner must not depend on it, but nothing stops a consumer from reading it. `buildsignal`'s own poll loop (steps 6-8) doesn't need to interpret it to decide when to stop polling, same as SubmitQueue's, but that's a statement about what the poll loop happens to need, not a rule that the value is unused: `build-runner.md` describes its purpose as round-tripping to users ([#buildmetadata](doc/rfc/submitqueue/build-runner.md#buildmetadata)), and a future check in either domain's buildsignal is free to read it — e.g. to short-circuit some behavior — without changing the contract.

Returning `TargetGraph` from `Status` in place of `BuildMetadata`, with `buildsignal` persisting it for `analyze` to read later, was considered and set aside — how `analyze` obtains the target graph is left to its own design, not `buildsignal`'s poll loop.

## Polling primitive: hold, not `Nack`

On non-terminal status, step 8 holds the delivery, never `Nack`s:

- **`Nack`** requeues and increments `retry_count`; at `MaxAttempts` the message dead-letters. That is the primitive for "something failed; retry."
- **Hold** postpones the same delivery for a delay; the redelivery restarts failure accounting. That is the primitive for "still working; check back later."

Polling is a scheduled heartbeat, neither failure nor retry, so a long-running build never burns `retry_count` toward the DLQ. A genuine `Status` failure (runner down, bad id) is a *different* path: it returns from step 5 to the classifier, which decides retryability, and a retryable verdict nacks normally. See [consumer-hold.md](doc/rfc/consumer-hold.md) for the primitive's full rationale.

## Poll delays

Two-tier cadence, matching SubmitQueue:

| Status | Delay | Rationale |
|---|---|---|
| `accepted` | `PollDelayAcceptedMs` (default 5000ms) | Queued by the runner, not started — poll infrequently. |
| `running` | `PollDelayRunningMs` (default 2000ms) | Actively executing — poll more often. |

Package-level `var`s (not `const`s) so tests can shorten them; the server always uses the defaults. Future: move these behind a `queueconfig`-style extension so operators can tune cadence per queue without a code change (the same TODO SubmitQueue's buildsignal carries).

## Error classification

Per `platform/errs`'s non-retryable-by-default rule (see [platform/errs/README.md](platform/errs/README.md)), a plain returned error is already non-retryable and rejects straight to DLQ, where the fail-closed path forces a conservative terminal `failed` so a Request never wedges its Queue's slot ([workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work)). So this section documents only the departures from that default, not every failure the algorithm can hit:

| Failure | Disposition | Why |
|---|---|---|
| `Status` call | raw error; classifier decides | Deliberately left open rather than fixed either way — runner timeout/connection is transient, "runner not deployed for this queue" is not, and only a backend classifier can tell them apart. **This means the `BuildRunner` backend has to classify**: an unclassified transport or HTTP error gets the non-retryable default, so one proxy blip ends the poll chain (see below). |
| `Update` CAS conflict (`ErrVersionMismatch`) | declaration-level retryable | A concurrent (redelivered) writer moved the row; reload and re-check converges. |

`Build`/`Request` not found (`storage.ErrNotFound`) are **not** in this table: storage is required to be read-after-write consistent (see [storage README](stovepipe/extension/storage/README.md)), so a miss here is already the correct default (non-retryable, straight to DLQ) rather than a departure worth overriding.

Everything else — factory lookup, an `Update` store error other than a CAS conflict, and the `record` publish — is returned raw with no override, because the default is already correct: a queue with no registered runner is a config error, and storage/queue failures dead-letter and let DLQ reconciliation recover. The poll loop itself no longer has a publish to fail: holding is a local outcome, and a failed postpone write in the framework lapses into a normal visibility-timeout redelivery, so the loop's liveness never rides on an enqueue succeeding.

### What it costs when a backend does not classify `Status` errors

Leaving `Status` to the classifier only works if the backend classifies. A `BuildRunner` whose transport returns plain `fmt.Errorf` values gets the non-retryable default, and here that default is expensive: dead-lettering ends the *only* poll chain for a build that is still running, and the request keeps holding one of the queue's `in_flight_count` build slots until reconciliation gives it back. A single `502` from a proxy in front of the build API then looks exactly like "this build can never be polled".

Two things keep a blip from stalling a queue, and a backend needs both:

- **The backend classifies its own failures.** Transport errors and 5xx/429/408 responses are `errs.NewRetryableDependencyError`. A 4xx about the request itself — unknown build, forbidden — is `errs.NewDependencyError`. Only the layer that sees the status code can tell these apart, which is why the table above leaves the call to it.
- **The retry budget is worth something.** Retryable means nack, and a nacked message comes back on the next poll, so `Retry.MaxAttempts` counts attempts rather than time — the default three are spent in a few hundred milliseconds. Raising `MaxAttempts` on this subscription buys a little more, but each attempt is another request at a dependency that is already failing, so it does not stretch to cover a proxy restart. Until nacks are spaced by the configured retry backoff, it is the reconciler below rather than the retry budget that keeps a longer outage from costing the queue a slot.

When the budget does run out the message dead-letters, and the buildsignal DLQ reconciler (`stovepipe/controller/dlq/buildsignal.go`) is what makes that recoverable: it maps the build back to its request, releases the slot, and marks the request `failed`. A deployment that registers the primary consumers but not that reconciler has no fail-closed path for this stage, and loses a slot for good every time this happens.

## Idempotency

Every branch is safe under at-least-once redelivery:

- **Build not found** — non-retryable; storage's read-after-write guarantee means a miss here is a storage defect, not a lag condition to retry through.
- **Status already persisted** — a redelivery re-runs the whole algorithm from step 1, including a redundant `Status` poll (harmless — the runner reports the same thing); step 6 no-ops on the unchanged status, and the delivery proceeds to hold for the next poll (non-terminal) or republish the request id to `record` (terminal, idempotent). No corruption.
- **Terminal already published** — a redelivery reloads, re-polls, no-ops at step 6, republishes the same terminal signal to `record` (idempotent), and acks. Harmless.
- **Postpone write failed** — the framework abandons the delivery; the visibility timeout lapses into a normal redelivery, which re-runs from step 1 and no-ops at step 6. The poll loop's continuation is framework-owned.

The window to guard is between persisting status (step 6) and ack (steps 7–8); because status writes are CAS-guarded, monotonic, and write-once at terminal, a redelivery always observes a consistent row.

## Edge cases

- **Runner has no record of this build id** (runner restarted without persisting in-flight state; a foreign id leaked in). `Status` returns an error, not a status value — non-retryable by default, unless the classifier has a domain sentinel for "unknown build" it chooses to treat as retryable (a restart may self-resolve). Left to the classifier, not hardcoded, per [build-runner.md](doc/rfc/submitqueue/build-runner.md#error-classification).
- **A later `Status` returns a *different* terminal status than what's stored** (a flaky backend flipping `succeeded`→`failed` between polls). CAS is *not* the defense here: the earlier delivery already committed its write and acked, so a later delivery would CAS cleanly against the current version — optimistic locking guards concurrent writers, not sequential overwrites. The defense is step 6's write-once rule: a stored terminal status is never overwritten, the delivery proceeds with the stored value, and `record` only ever hears one verdict per build. First terminal wins by design; a backend that flip-flops terminal states is broken in a way consumer-side ordering cannot repair, so a deterministic verdict is the most the pipeline can offer.

## Fail-closed interaction

A build that never reaches terminal `Status` — runner outage, a build the runner lost — must not wedge its `Request` forever, since callers gate deployments on greenness reaching a recorded terminal state. `buildsignal` does not implement the forcing function: per [workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work) and the `in_flight_count` slot lifecycle in [process.md](doc/rfc/stovepipe/steps/process.md#concurrency-lifecycle), a `Request` stuck at `buildsignal` past `MaxAttempts` dead-letters, and the DLQ reconciler forces a conservative terminal `failed` and releases the Queue's slot. This is the same posture SubmitQueue's build/buildsignal pair relies on: terminal status is what releases the slot and lets validation progress.

One boundary of that posture is worth stating: the `MaxAttempts` path fires only when polls *fail*. A runner that keeps answering a healthy non-terminal status forever — a hung build on a backend with no timeout of its own — never errors, so the hold chain (whose redeliveries deliberately do not count toward the retry limit) re-polls indefinitely and nothing dead-letters; SubmitQueue's poll loop shares this property. Bounding it requires a poll deadline — a `max_validation_ms` past which `buildsignal` treats the build as failed and lets the normal terminal path run — which pairs naturally with the lease idea [process.md](doc/rfc/stovepipe/steps/process.md#per-queue-concurrency-gate) floats for `in_flight_count`. Deferred with it; until then a too-old non-terminal `Build` is an operational alert, not a self-healing path.

## Entity, storage, and queue additions

No additions beyond [build.md](doc/rfc/stovepipe/steps/build.md#entity-and-storage-additions-needed): `buildsignal` calls `BuildStore.Get`/`Update` and `RequestStore.Get`/`Update` against the `Build`/`Request` shapes defined there — `Build.ID` being the runner-assigned id it hands straight back to `Status` — plus `QueueStore.Get`/`Update` to release the build slot, and consumes/re-produces the `BuildSignal` message on `TopicKeyBuildSignal` introduced there. `Request.State` gains the three build outcomes (`succeeded`, `failed`, `cancelled`), all terminal. The message it publishes to `record`, and the `record` topic key itself, are owned by the `record` stage and land with `record.md`; `buildsignal` only needs that the **request id** reaches the record topic once the build is terminal, partitioned by request id.
