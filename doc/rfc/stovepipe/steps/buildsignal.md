# Buildsignal stage

`buildsignal` polls the build-runner until a build reaches a terminal state, records the status, and — once the build is terminal — publishes the build id to `record`. See [workflow.md](doc/rfc/stovepipe/workflow.md) for where it sits in the pipeline and [build.md](doc/rfc/stovepipe/steps/build.md) for how the build it polls was triggered.

It handles only the poll loop: it does not decide build strategy, write greenness, or map targets to projects — those are `process`'s, `record`'s, and `analyze`'s jobs.

`buildsignal` is structurally the same controller as `submitqueue/orchestrator/controller/buildsignal/buildsignal.go`, and the "Flow", "Status delivery", and "Polling primitive" sections of [doc/rfc/submitqueue/build-runner.md](doc/rfc/submitqueue/build-runner.md) are the reference rationale it reuses directly. The entity and storage shapes it depends on are defined in [build.md](doc/rfc/stovepipe/steps/build.md#entity-and-storage-additions-needed); this doc introduces none of its own.

## Input and re-entrancy

`buildsignal` consumes a build id, published by `build` — once per phase, since Phase 1 (whole-repo) and Phase 2 (per-project) each run their own `build` → `buildsignal` cycle against builds tied to the same `Request` (see [workflow.md](doc/rfc/stovepipe/workflow.md#workflow)).

Its logic does not branch on phase: it loads the `Build`, polls it toward terminal, persists the result, and publishes the build id onward to `record`. What differs between phases is what `record` does with that publish (whole-repo vs. per-project greenness) — not anything `buildsignal` decides.

`buildsignal` is the sole writer of `Build.Status`/`Build.Version` after `build` creates the row (see [build.md](doc/rfc/stovepipe/steps/build.md#input-partitioning-and-the-single-writer-property)); it only reads `Request` via `RequestStore.Get` (for `R.Queue`, to resolve the build-runner) and never writes it.

## Algorithm

For a delivery carrying build id `B`:

```
1. Load Build B from the build store.
   - ErrNotFound       -> retryable (build's Create not visible yet; redelivery converges).
   - other store error -> return raw; classifier decides.

2. Load Request R = store.Get(Build.RequestID) — needed for R.Queue to resolve the build-runner.
   - ErrNotFound -> retryable, like step 1: the Build's existence proves the Request write is older,
     so a miss here is almost certainly a lagging read; redelivery converges. A genuinely orphaned
     Build (integrity fault) still dead-letters at MaxAttempts — the same terminal outcome, without
     rejecting straight to DLQ on a stale read.

3. If R.State is terminal (superseded / recorded-green / recorded-not-green): ack and return.
   - the Request is done (record already ran, or the head was superseded); stop polling.
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
     - ErrVersionMismatch -> retryable (a concurrent writer moved the row; reload and re-check).
     - with the write-once rule, accepted -> running -> {succeeded|failed|cancelled} is monotonic
       by mechanism, not by assumption about the backend.

7. If the stored status is terminal: publish B (the build key, Build.ID) to the record topic,
   partitioned by request id; ack, return. No re-publish to buildsignal.
   - record loads the Build directly by this key (Build->Request gives it R for the per-Queue writes),
     so no reverse lookup from Request to its builds is ever needed.
   - a redelivery republishes the same terminal signal; record is idempotent.
   - publish failure -> return raw (non-retryable); status is persisted, operational republish recovers.

8. Else PublishAfter(B -> buildsignal, delayMs), partitioned by build id:
   - delayMs = pollDelay(status): shorter while running, longer while accepted.
   - a fresh message (retry_count resets to 0), not a nack — polling is not failure.
   - ack.
```

**Why `record` hears only terminal signals**: `record` has no non-terminal work — by its own contract a non-terminal signal would be a pure no-op — and step 7 already branches on terminality to decide whether to keep polling, so gating the publish costs nothing and spares `record` a no-op delivery on every poll tick of every running build. Crash-safety is unaffected: a crash between the terminal `Update` and the publish redelivers the message; step 5 re-polls (the runner reports the same terminal status), step 6 no-ops, step 7 publishes. This is a deliberate divergence from SubmitQueue, whose buildsignal republishes to `speculate` on every tick — sound there because speculate is a state machine that may act on any signal; stovepipe has no such consumer.

**Why step 6 guards on status and makes terminal write-once**: an unchanged status skips the CAS write entirely, so a long build being polled every couple of seconds doesn't churn `Build.Version` on every tick — the version only advances on a real state transition. The write-once rule exists because CAS alone cannot provide it: optimistic locking defends against *concurrent* writers, but a later delivery that polls a flaky backend and sees a different terminal status would CAS cleanly against the current version and overwrite (see Edge cases). A given `Build` has a single poll partition (see [Partitioning](doc/rfc/stovepipe/steps/build.md#partitioning)), so the only writer racing the CAS is a redelivery of the same message (e.g. after a lapsed visibility lease); `ErrVersionMismatch` there is handled as retryable and converges.

## Status: shaped like SubmitQueue's, not shared code

`Status` is not a point of conceptual divergence from SubmitQueue: both domains' `BuildRunner`s poll by the same opaque, runner-minted id with the same `Status(ctx, buildID) (BuildStatus, BuildMetadata, error)` signature. That similarity stays a shape, not a shared `platform/extension/buildrunner` interface — each domain keeps its own `BuildRunner` (including its own local `Status`), with reuse pushed to a shared backend implementation instead (see [build.md](doc/rfc/stovepipe/steps/build.md#alternatives-considered-for-sharing-the-contract)). `BuildMetadata` is caller-supplied and provider-echoed — the runner must not depend on it, but nothing stops a consumer from reading it. `buildsignal`'s own poll loop (steps 6-8) doesn't need to interpret it to decide when to stop polling, same as SubmitQueue's, but that's a statement about what the poll loop happens to need, not a rule that the value is unused: `build-runner.md` describes its purpose as round-tripping to users ([#buildmetadata](doc/rfc/submitqueue/build-runner.md#buildmetadata)), and a future check in either domain's buildsignal is free to read it — e.g. to short-circuit some behavior — without changing the contract.

Returning `TargetGraph` from `Status` in place of `BuildMetadata`, with `buildsignal` persisting it for `analyze` to read later, was considered and set aside — how `analyze` obtains the target graph is left to its own design, not `buildsignal`'s poll loop.

## Polling primitive: `PublishAfter`, not `Nack`

On non-terminal status, step 8 reschedules with `PublishAfter`, never `Nack`:

- **`Nack`** requeues and increments `retry_count`; at `MaxAttempts` the message dead-letters. That is the primitive for "something failed; retry."
- **`PublishAfter`** emits a fresh message with `retry_count` reset to 0, deferred by a delay. That is the primitive for "still working; check back later."

Polling is a scheduled heartbeat, neither failure nor retry, so a long-running build never burns `retry_count` toward the DLQ. A genuine `Status` failure (runner down, bad id) is a *different* path: it returns from step 5 to the classifier, which decides retryability, and a retryable verdict nacks normally. See [build-runner.md](doc/rfc/submitqueue/build-runner.md#polling-primitive-publishafter-not-nack) for the full rationale.

## Poll delays

Two-tier cadence, matching SubmitQueue:

| Status | Delay | Rationale |
|---|---|---|
| `accepted` | `PollDelayAcceptedMs` (default 5000ms) | Queued by the runner, not started — poll infrequently. |
| `running` | `PollDelayRunningMs` (default 2000ms) | Actively executing — poll more often. |

Package-level `var`s (not `const`s) so tests can shorten them; the server always uses the defaults. Future: move these behind a `queueconfig`-style extension so operators can tune cadence per queue without a code change (the same TODO SubmitQueue's buildsignal carries).

## Error classification

Per `platform/errs`'s non-retryable-by-default rule (see [platform/errs/README.md](platform/errs/README.md)), a plain returned error is already non-retryable and rejects straight to DLQ, where the fail-closed path forces a conservative not-green so a Request never wedges its Queue's slot ([workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work)). So this section documents only the departures from that default, not every failure the algorithm can hit:

| Failure | Disposition | Why |
|---|---|---|
| `Build` not found | retryable (`errs.NewRetryableError`) | `build`'s `Create` not visible yet; redelivery converges. |
| `Request` not found | retryable (`errs.NewRetryableError`) | The Build's existence proves the Request write is older, so a miss is a stale read; a genuine orphan still dead-letters at `MaxAttempts`. |
| `Status` call | raw error; classifier decides | Deliberately left open rather than fixed either way — runner timeout/connection is transient, "runner not deployed for this queue" is not, and only a backend classifier can tell them apart. |
| `Update` CAS conflict (`ErrVersionMismatch`) | retryable | A concurrent (redelivered) writer moved the row; reload and re-check converges. |
| `PublishAfter` re-poll | retryable | The poll heartbeat; it runs only after status/persist/record all succeeded, so a transient enqueue blip is worth replaying to `MaxAttempts` before dead-lettering. |

Everything else — factory lookup, an `Update` store error other than a CAS conflict, and the publish to `record` — is returned raw with no override, because the default is already correct: a queue with no registered runner is a config error, and storage/queue failures dead-letter and let DLQ reconciliation recover.

## Idempotency

Every branch is safe under at-least-once redelivery:

- **Build not found** — retryable; converges as the row becomes visible.
- **Status already persisted** — a redelivery re-runs the whole algorithm from step 1, including a redundant `Status` poll (harmless — the runner reports the same thing); step 6 no-ops on the unchanged status, and the delivery proceeds to re-schedule the poll (non-terminal) or republish to `record` (terminal, idempotent). No corruption.
- **Terminal already published** — a redelivery reloads, re-polls, no-ops at step 6, republishes the same terminal signal to `record` (idempotent), and acks. Harmless.
- **`PublishAfter` failed, then retried** — the nacked delivery re-runs from step 1; there is no way to resume mid-algorithm, so it re-polls the runner too, but the row already carries the non-terminal status and step 6 no-ops. Only the final enqueue does new work.

The window to guard is between persisting status (step 6) and ack (steps 7–8); because status writes are CAS-guarded, monotonic, and write-once at terminal, a redelivery always observes a consistent row.

## Edge cases

- **Runner has no record of this build id** (runner restarted without persisting in-flight state; a foreign id leaked in). `Status` returns an error, not a status value — non-retryable by default, unless the classifier has a domain sentinel for "unknown build" it chooses to treat as retryable (a restart may self-resolve). Left to the classifier, not hardcoded, per [build-runner.md](doc/rfc/submitqueue/build-runner.md#error-classification).
- **A later `Status` returns a *different* terminal status than what's stored** (a flaky backend flipping `succeeded`→`failed` between polls). CAS is *not* the defense here: the earlier delivery already committed its write and acked, so a later delivery would CAS cleanly against the current version — optimistic locking guards concurrent writers, not sequential overwrites. The defense is step 6's write-once rule: a stored terminal status is never overwritten, the delivery proceeds with the stored value, and `record` only ever hears one verdict per build. First terminal wins by design; a backend that flip-flops terminal states is broken in a way consumer-side ordering cannot repair, so a deterministic verdict is the most the pipeline can offer.

## Fail-closed interaction

A build that never reaches terminal `Status` — runner outage, a build the runner lost — must not wedge its `Request` forever, since callers gate deployments on greenness reaching a recorded terminal state. `buildsignal` does not implement the forcing function: per [workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work) and the `in_flight_count` slot lifecycle in [process.md](doc/rfc/stovepipe/steps/process.md#concurrency-lifecycle), a `Request` stuck at `buildsignal` past `MaxAttempts` dead-letters, and the DLQ reconciler forces a conservative not-green and releases the Queue's slot. This is the same posture SubmitQueue's build/buildsignal pair relies on: terminal status is what releases the slot and lets validation progress.

One boundary of that posture is worth stating: the `MaxAttempts` path fires only when polls *fail*. A runner that keeps answering a healthy non-terminal status forever — a hung build on a backend with no timeout of its own — never errors, so the `PublishAfter` chain (which resets `retry_count` by design) re-polls indefinitely and nothing dead-letters; SubmitQueue's poll loop shares this property. Bounding it requires a poll deadline — a `max_validation_ms` past which `buildsignal` treats the build as failed and lets the normal terminal path run — which pairs naturally with the lease idea [process.md](doc/rfc/stovepipe/steps/process.md#per-queue-concurrency-gate) floats for `in_flight_count`. Deferred with it; until then a too-old non-terminal `Build` is an operational alert, not a self-healing path.

## Entity, storage, and queue additions

No additions beyond [build.md](doc/rfc/stovepipe/steps/build.md#entity-and-storage-additions-needed): `buildsignal` calls `BuildStore.Get`/`Update` and `RequestStore.Get` against the `Build`/`Request` shapes defined there — `Build.ID` being the runner-assigned id it hands straight back to `Status` — and consumes/re-produces the `BuildSignal` message on `TopicKeyBuildSignal` introduced there. The message it publishes to `record`, and the `record` topic key itself, are owned by the `record` stage and land with `record.md`; `buildsignal` only needs that the **build id** (the build key, so `record` loads the `Build` directly) reaches the record topic once the build is terminal, partitioned by request id.
