# Record stage

`record` turns a terminal build outcome into a durable validation fact.

- Phase 1 records whole-repository greenness. On green it also advances the Queue's last-green bookmark and promotes the commit onto the Queue's promotion ref. This is implemented: see [stovepipe/controller/record/record.go](../../../../stovepipe/controller/record/record.go).
- Phase 2 records greenness per project instead of per repository. Sketched here, to be expanded before implementation.

Notifying downstream systems is **not** implemented in either phase. It will ride the cross-domain hook framework instead of a Stovepipe-specific extension; see [Hooks](#hooks).

See [workflow.md](../workflow.md) for the whole pipeline, [build.md](build.md) for how builds are created, and [buildsignal.md](buildsignal.md) for the terminal-only handoff into this stage.

`record` owns the validation facts and the two caches derived from them, the last-green bookmark and the promotion ref. It does not decide build scope, poll a build, release the Queue's build slot, interpret a target graph, or map targets to projects. Those belong to `process`, `buildsignal`, and `analyze`.

## Phase 1 algorithm

For a delivery carrying a `Record` payload:

```
1. Deserialize the payload into (request id R, queue name Q).
   - malformed -> return raw; non-retryable.

2. Resolve the queue's storage from Q.
   - unresolvable -> return raw; non-retryable. A queue with no storage is a
     malformed message or a config error, not a transient condition.

3. Load Request R.
   - ErrNotFound       -> return raw; non-retryable.
   - other store error -> return raw; the classifier decides.

4. If Q is non-empty and disagrees with R.Queue, return a non-retryable error.
   The Request is the authoritative binding; Q is a routing hint, empty on
   payloads written before the field existed.

5. Inspect R.State.
   - succeeded / failed -> continue. buildsignal stamps the outcome before it
     publishes here, and both values are verdicts about the code.
   - cancelled -> ack, no fact: the build decided nothing about the commit (see
     "When to record an outcome").
   - superseded -> ack, no fact. Unreachable in practice.
   - accepted / processing / anything else -> return a non-retryable invariant error.

6. Map R.State to a whole-repository degree and create the ValidationFact keyed by
   (R.URI, empty project) in the queue-bound fact store.
   - ErrAlreadyExists -> load and reconcile the existing immutable fact.
   - other store error -> return raw.

7. If the persisted fact is not green, ack. Report how long the break went undetected
   first, but only if step 6 is the write that created the fact (see "Observability").

8. Advance the bookmark to (R.URI, R.ID) in a CAS retry loop, which also reports
   whether R holds the bookmark afterwards:
   - stored id empty or older than R.ID -> write under the version guard; R holds it.
   - stored id equals R.ID              -> R set it on an earlier delivery; no write,
                                           and R still holds it.
   - stored id newer                    -> no write, and R does not hold it.
   - ErrVersionMismatch                 -> reload and re-evaluate.

9. If R holds the bookmark, ask SourceControl to point the promotion ref at R.URI.
   Otherwise ack: whichever commit holds the bookmark owns the ref.
   - ErrNotFound -> count and ack. A rewritten history dropped the commit from the
     ref, and no retry can promote it.
   - other error -> return raw.

10. ack.
```

Every decision after step 6 uses the persisted fact, not the outcome read from this delivery's Request. The first immutable fact controls the bookmark and the ref, and will control the hook event. Once hooks land, the publish becomes a new step between 9 and 10.

## Validation facts

A fact answers "how broken was this scope at this Queue URI?" Its identity is `(queue, uri, project)`, where `project` is empty for whole-repository greenness and a stable project id in Phase 2.

`queue` is the binding of the store the fact lives in, not a field on the entity. Storage is resolved per queue through `storage.Factory.For`, so a fact only ever exists inside one queue's store and does not need to name it. The column still leads the primary key, `PRIMARY KEY (queue, uri, project)`, because every domain table here has to be shardable by queue: the primary key leads with the queue column and no secondary index leads with anything else. That keeps one queue's rows unreachable through another queue's binding and makes every read a primary-key-prefix scan inside a single queue. It is an invariant rather than a per-table choice, and `make lint-queue-shard` enforces it. It is not MySQL table partitioning; no schema here uses `PARTITION BY`.

So `entity.ValidationFact` carries only the rest of the identity:


| Field       | Meaning                                                                                                           |
| ----------- | ----------------------------------------------------------------------------------------------------------------- |
| `URI`       | Commit URI under validation                                                                                       |
| `Project`   | Empty for the whole repository; stable project id for Phase 2                                                     |
| `Degree`    | Health degree in the closed interval `[entity.DegreeGreen, entity.DegreeBroken]` — `0` is green, `1` fully broken |
| `RequestID` | Request that established the fact                                                                                 |
| `CreatedAt` | Millisecond timestamp at which the fact was first recorded                                                        |


Greenness is read through `ValidationFact.IsGreen()` rather than compared against a bare literal, and the endpoints are the named constants `entity.DegreeGreen` and `entity.DegreeBroken`.

Facts are create-only, so `ErrAlreadyExists` on create means one of exactly two things:

- Same Request: a redelivery. The stored fact is this delivery's own earlier write, and it carries the same degree, since the Request's outcome is immutable once stamped. Load it and continue.
- Different Request: the `(Queue, URI)` ingest dedup invariant is broken. Return an error rather than overwrite history.

Competing verdicts never reach this point. Duplicate builds for one Request are resolved a stage earlier, where `buildsignal`'s first-writer-wins outcome write discards the losing build's verdict.

Absence is still distinct from degree `0`. Callers gating deployments must treat absence as not green.

### When to record an outcome

A fact is written only when the Request reaches `succeeded` or `failed`. A `cancelled` build is acked with no fact.

The fail-closed path is subtler than "no fact". A DLQ reconciler forces the Request to `failed` and does not itself publish here, so reconciliation on its own records nothing. That is not the same as nothing being recorded: a `buildsignal` delivery still in flight can reach this stage afterwards, and it will write `DegreeBroken` from the forced state. See [What fail-closed actually guarantees](#what-fail-closed-actually-guarantees).

### Phase 1 degree mapping

Whole-repository builds use only the endpoints, mapped from the outcome `buildsignal` stamped on the Request:


| Request outcome | Result                        |
| --------------- | ----------------------------- |
| `succeeded`     | fact at `entity.DegreeGreen`  |
| `failed`        | fact at `entity.DegreeBroken` |
| `cancelled`     | no fact                       |


Intermediate degrees are reserved for project analysis and deferred with the project mapping contract. Phase 1 does not manufacture fractional values.

## Last-green advancement (Queue bookmark)

The bookmark only moves forward. `entity.Queue` carries `LastGreenURI` alongside `LastGreenRequestID`, the request id that owns the current URI, plus `InFlightCount` and `LatestRequestID`. The guard compares ids with `entity.CompareRequestID(R.Queue, …)`, the same ingest-order comparison `ingest` and `process` use for coalescing. It errors on an id that does not match the queue's format, which is non-retryable: re-parsing the same ids cannot start succeeding.

The write goes through `QueueStore.Update(ctx, queue, oldVersion, newVersion)`, so version arithmetic stays in the controller: it computes `newVersion = oldVersion + 1` and the store does a pure conditional write. `ErrVersionMismatch` is absorbed by the loop rather than returned, since a concurrent writer moved the row and reloading converges.

**Why the bookmark moves only after the fact is durable.** The bookmark is a cache of "newest green URI" derived from the facts, so it must never lead them. Losing the advance to a crash is recoverable, since the redelivery reloads the same fact and retries. A bookmark with no fact behind it would point at greenness nothing recorded, and `process` would pick a baseline URI that no validation supports.

A failed or cancelled build never moves the bookmark, and so never promotes either.

## Promotion ref

A green commit is also promoted: `record` asks `SourceControl` to point the Queue's promotion ref, a stable branch name like `verified-main`, at that commit. This is the pull-shaped counterpart to the hook event's push, so a deploy gate or cache warmer can resolve a name and know nothing about Stovepipe, URIs, or degrees. See [Promotion ref](../workflow.md#promotion-ref--the-last-green-commit-by-name) in the pipeline overview. The stage names only the commit; which ref a Queue promotes to, and whether it has one at all, is integrator configuration injected into the `SourceControl` implementation alongside the endpoint and credentials, so a backend with no promotion target makes `Promote` a no-op.

**Promotion is gated on holding the bookmark**, and only the holder promotes. Without that gate an older green commit recording late would drag the ref backwards: the ref has no version guard of its own, and `Promote` lands the URI it is given rather than refusing a non-descendant, so the caller has to enforce monotonicity. Deciding it from the bookmark instead of a fresh comparison means both caches move on one already-serialized decision.

The equal case matters as much as the newer one. A redelivery that finds its own id on the bookmark reports that it *holds* it rather than skipping, so the promotion behind it is retried. That is what makes a crash between the bookmark write and the promotion recoverable, and it is safe because `Promote` is idempotent. Promotion follows the durable fact for the same reason the bookmark does: the ref is a cache of the facts and must not lead them.

`ErrNotFound` from `Promote` means a rewritten history dropped the commit from the ref. It is counted and acked rather than retried, since no retry will put that commit back and the next green commit corrects the ref.

## Observability

Two things are measured here, because this is where greenness becomes known. Both need a `SourceControl` lookup of a commit's creation time.

- **Last-green age.** A gauge carrying the creation timestamp of the commit the bookmark now points at, emitted once the bookmark write is durable. It is a timestamp rather than an elapsed time so that subtracting it at scrape time gives the current age, and a queue that stops going green keeps reporting a staler value without the stage re-emitting anything.
- **Failure-detection latency.** A histogram of how long the break a build failed on went undetected, measured from the creation time of the base URI it validated against. A distribution rather than a gauge, because what matters is how long breaks typically survive, not how long the last one did. There is no later moment to sample it from, since an elapsed time only means something against the failure that just became known, so this lookup cannot move off the delivery path onto a clock.

Only the delivery that *wrote* the fact reports the latency. That is why `recordFact` returns whether it created the fact and not just the fact itself: a redelivery adopts the stored fact, and a second sample would count one break twice. A full build pins no base URI, so its failures are counted as unmeasurable instead of timed; absent is the ordinary case there, not a fault.

Both are best-effort. Every way the lookup can fail is counted, logged, and swallowed, because an observability fault must not turn an already-recorded outcome into a retry. A non-positive creation timestamp is treated as a broken extension contract and dropped, since emitting it would put a 1970 timestamp in a gauge that reads as an infinitely stale queue, or a decades-long sample in the latency distribution.

## Build slot release

`Queue.in_flight_count` is released by `buildsignal` before it stamps the outcome and publishes to the record step. The DLQ reconciler releases the slot on the fail-closed path for the same reason. `record`'s only Queue write is the bookmark.

## Hooks

Recording a fact is when the rest of the company can learn "this URI is now green (or not green)". `record` publishes that as a `HookEvent` on Stovepipe's durable `hook` topic; it does not call a notification extension inline.

The mechanics are already settled in [hook-framework.md](../../hook-framework.md) — the envelope, the delivery promise, the per-domain dispatcher stage, the `hook_dlq`, and the reasoning behind each. This section covers only what Stovepipe has to decide for validation facts. The earlier design here, a Stovepipe `Hooks` extension called with `Notify(ctx, ValidationFactRef{…})` as the last algorithm step, is rejected there on both halves: inline calls couple pipeline latency to third-party integrations and drop the notification on a crash between the state write and the call, and a per-domain contract multiplies schemas and sinks for no gain.

None of it is built. It needs the shared `HookEvent` contract at `api/base/hook/` and the hook extension at `platform/extension/hook/`, plus Stovepipe's own share: a `hook` topic key, a dispatcher stage, a `hook_dlq` reconciler, and the wiring for all three.

### Where the publish belongs

Last thing before the ack, after the fact write and after both caches derived from it have moved:

```
create fact → advance bookmark (green only) → promote (bookmark holder only) → publish HookEvent → [Phase 2: publish to analyze] → ack
```

Last because the payload carries no entity snapshot and hooks resolve entities from stores: a hook reacting to "URI is green" by reading `LastGreenURI`, or by fetching the promotion ref, must not find either still pointing at the previous commit. Inside the delivery rather than after it, because that is what makes the event lossless without an outbox — the state writes are recognize-and-skip on redelivery, so a crash before the ack replays the whole chain.

### Event shape


| Envelope field | Value for a validation fact                                     |
| -------------- | --------------------------------------------------------------- |
| `source`       | `stovepipe`                                                     |
| `type`         | `validation.repository.recorded` (see below)                    |
| `version`      | `0` — a fact is create-only and has no version to report        |
| `timestamp_ms` | Publish time; the fact's own `CreatedAt` travels in the payload |
| `id`           | `source` / `type` / request id (see below)                      |


The **subject** is the Request — a payload fact rather than an envelope field, but it is what `id` is minted from and what the event partitions on. The Request over the URI keeps partitioning identical to the `record` topic's own, so per-request ordering carries through the seam, and it hands a consumer a way back into the pipeline. The two are near-interchangeable anyway: ingest dedups on `(queue, uri)`, so one Request means one URI.

The payload carries the fact's identity and value: `queue`, `uri`, `project`, `degree`, `request_id`. `queue` has to be there because neither the envelope nor the fact entity carries one, so the event is the only place a cross-queue consumer sees it. `degree` is there even though a hook could read it from the store: the fact is immutable, so the staleness objection behind the no-snapshots rule does not apply, and it is what a consumer branches on to tell green from broken. Build failure detail stays off, since the payload is reserved for facts persisted nowhere else and a failed build's detail is durable on the `Build` row.

### What the `type` carries

Data belongs in `type` when consumers need to avoid receiving the event, and in the payload when they need to interpret it. The type names the scope, `validation.repository.recorded`, with `validation.project.recorded` beside it in Phase 2.

### What a consumer can and cannot assume

Ordering is per-subject only and the subject is the Request, so events for *different* Requests can arrive out of order. A consumer must not infer "the newest green commit" from arrival order; it should compare request ids by ingest order (`entity.CompareRequestID`) or read the bookmark, which is monotonic by construction.

Absence of an event is not a signal: a cancelled build records nothing, and a Request abandoned before any build went terminal never reaches this stage, so a consumer waiting for one event per ingested commit waits forever on those. Gating keeps treating "no recorded fact" as not green. The converse holds too — an event is not proof the code was tested, since a fail-closed Request can produce a broken fact without a build having failed.

Hooks here must be idempotent on `id`, as everywhere. "Fire-and-forget" describes downstream consumption, not the publish: `record` never waits for a hook, but a failed *publish* fails the delivery. Per `[platform/errs](../../../../platform/errs/README.md)` rule 4 it is not wrapped retryable just because replaying it is convenient, so it dead-letters, which is where the missing `record_dlq` reconciler stops being theoretical: the fact is durable and only the notification is lost.

## Request lifecycle

Phase 1 uses the states in [stovepipe/entity/request.go](../../../../stovepipe/entity/request.go). `record` runs *after* the Request is terminal: `buildsignal` projects the build's terminal status onto it as `succeeded`, `failed`, or `cancelled` (`RequestState.HasBuildOutcome()`), and only then publishes. So `record` reads an outcome and writes no state. `superseded` is terminal without an outcome.

Phase 2 broadens "complete" to "all planned facts recorded", which needs a marker this stage does not own; see [Completion marker: open](#completion-marker-open).

## Storage and queue contract

`ValidationFactStore` is key/value-shaped, and queue-bound rather than queue-parameterized:

- `Create(ctx, fact)` writes one immutable fact for the bound queue. It returns `ErrAlreadyExists` when the composite identity is already taken, leaving the stored fact untouched.
- `Get(ctx, uri, project)` reads one fact by the rest of its identity, and returns `ErrNotFound` when none exists. The `project` argument is reserved for Phase 2.

There is no `Update`. The first fact written for an identity is the permanent answer, and a caller that needs to know whether it won the race reads `ErrAlreadyExists` and then loads the winner.

The topic key, the message, and the consumer all exist. The DLQ consumer does not (see [DLQ and fail-closed behavior](#dlq-and-fail-closed-behavior)).


| Topic key | Message                  | Producer      | Consumer | Partition key | Message id |
| --------- | ------------------------ | ------------- | -------- | ------------- | ---------- |
| `record`  | `Record{id, queue_name}` | `buildsignal` | `record` | Request id    | Request id |


The payload carries the request id plus the queue name, so the consumer can resolve per-queue storage before it loads any state. `queue_name` is empty on payloads written before the field existed, which is why step 4 only enforces the match when it is set.

Partitioning by request id keeps completion bookkeeping single-writer per Request, and reusing the request id as the message id dedups a redelivered signal into the original message instead of enqueuing a second one.

## Error classification


| Failure                                            | Disposition          | Reason                                                                            |
| -------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------- |
| Malformed `Record` payload                         | non-retryable        | a malformed message will never succeed regardless of retries                      |
| Queue name that resolves to no storage             | non-retryable        | malformed message or missing config, not a transient condition                    |
| Payload queue disagreeing with the Request's queue | non-retryable        | malformed message; the Request is the authoritative binding                       |
| Request not found                                  | non-retryable        | the publish follows the committed outcome write, so a miss is a storage defect    |
| Request carrying no build outcome                  | non-retryable        | producer/state-machine invariant violation                                        |
| Existing fact owned by a different Request         | non-retryable        | ingest dedup invariant violated; the stored fact is immutable                     |
| Malformed request id at bookmark comparison        | non-retryable        | re-parsing the same ids cannot succeed                                            |
| Queue CAS version mismatch                         | absorbed, not raised | `storage.ErrVersionMismatch` is handled by the bookmark loop: reload and re-apply |
| Promotion target unknown to the ref                | absorbed, not raised | `sourcecontrol.ErrNotFound` means a rewritten history dropped the commit          |
| SourceControl resolution or `Promote` failure      | raw error            | backend classifier has the required failure knowledge                             |
| Commit-timestamp lookup for a metric               | swallowed            | counted and logged; observability must not retry a recorded outcome               |
| ValidationFactStore, QueueStore, RequestStore      | raw error            | backend classifier has the required failure knowledge                             |




## Edge cases and idempotency

Every effect is recognize-and-skip, so a redelivery after a complete run re-runs each step as a no-op. Once hooks land the publish re-fires, and the framework's dedupe on `id` absorbs it.

- **Request not visible.** A storage defect rather than lag, since the publish follows the committed outcome write. Non-retryable.
- **Fact already created.** Load it and continue from the stored fact. A fact from a *different* Request, or a Request carrying no build outcome, is an invariant violation rather than an expected outcome.
- **Duplicate builds for one Request.** Absorbed a stage earlier: `buildsignal`'s outcome write is first-writer-wins, so the Request carries one immutable verdict and `record` never sees a competing one.
- **Head equals last-green.** The build still produces a terminal verdict. A green result may share the bookmark's URI, but the guard compares request ids rather than URIs, so an equal-or-older candidate skips and nothing regresses.
- **Green fact recorded out of order across Requests.** An older green commit can reach this stage after a newer one. Its fact is written as usual, since facts are per-URI and independent, but the bookmark guard skips it, and because it does not hold the bookmark it does not promote either. Neither cache moves backwards.
- **Cancelled build.** Stovepipe never initiates cancellation, but a backend may still report it. No fact is written; the slot was already released and the Request already stamped `cancelled`. The fact identity stays unclaimed, and nothing can claim it today, since `cancelled` is terminal and re-validation does not exist. Recovery in practice is the next commit; the unclaimed identity only matters to a future re-run mechanism (see [Supporting re-run of the same URI](#supporting-re-run-of-the-same-uri)).
- **History rewrite while a build runs.** The Request keeps the strategy and URI pinned at admission, and record stores the fact about that immutable URI. A later head is handled independently by `process`. If the rewrite dropped the commit from the ref, the fact and the bookmark still stand, since they describe a commit and not a ref, and only the promotion is skipped.
- **A fail-closed terminal outranks a build that passed.** The degree derives from `R.State`, so a Request forced to `failed` by DLQ reconciliation records `DegreeBroken` even when one of its builds reports success afterwards. Reachable today, and permanent once written; see [What fail-closed actually guarantees](#what-fail-closed-actually-guarantees).
- **Crash between the fact write and the bookmark advance.** Redelivery reloads the existing fact and re-applies the idempotent guard.
- **Crash between the bookmark advance and the promotion.** Redelivery finds its own id on the bookmark, reports that it holds it, and retries the idempotent promotion.



## DLQ and fail-closed behavior

**Neither** `record_dlq` **nor** `build_dlq` **has a consumer today, and both topics are already receiving messages.** Every primary subscription comes from `DefaultSubscriptionConfig`, which enables dead-lettering with the `_dlq` suffix, so a message that is rejected outright *or* runs out of retries moves to its stage's dead-letter topic. The wiring registers only `process_dlq` and `buildsignal_dlq`, so messages pile up unread on the other two.

Two different things put a message there, and only one is a poison payload. A delivery that fails with its retry budget spent is dead-lettered by the nack itself, carrying the reason it actually failed. A delivery that never reaches a nack, because it crashed or because its **ack failed** and the visibility timeout redelivered it, is dead-lettered by the poll loop once `retry_count` reaches `MaxAttempts` (3 by default), without the controller running on that final attempt and with only a generic reason recorded. So a missing reconciler exposes more than malformed messages: a fact can be lost to a storage failure that would have succeeded on a later retry, or to an ack that never landed even though the write did.

Gating stays safe, because everything this stage can lose reads as not-green: a Request with no fact is indistinguishable from one not yet validated. What is lost is the *fact*. A green build whose fact write permanently failed leaves the URI looking unvalidated, which costs the queue an incremental baseline and forces a full build at the next head. Once hooks land, a lost notification joins that list, and unlike the fact it gets no second chance from a later commit.

This is the same failure shape [buildsignal.md](buildsignal.md#what-it-costs-when-a-backend-does-not-classify-status-errors) describes for a deployment that registers primary consumers without their reconciler. When the reconciler is built it should re-run this same idempotent algorithm from the request id, under `errs.AlwaysRetryableProcessor`: write and publish the immutable fact as usual if the Request carries a build outcome, keep retrying if Request storage is temporarily unavailable, and treat a malformed payload or a permanently missing Request as poison, which needs an operational alert rather than more retries.

## Future Items



### Coverage of intermediate commits

Coalescing means most commits never become a validated Request: a verdict on head `H` with base `B` is implicitly a verdict on every commit in the range `(B, H]`. Downstream tooling still needs prior and next green for any commit, including commits never ingested directly and commits that were superseded.

The rough idea is to track this with additional stores written during this stage, to be expanded in a separate doc:

- `CoverageStore` — on every verdict, green or failed, one row per commit in the covered range: `(queue, uri)` → the covering request and the commit's position within its range. Gives commits with no Request of their own a place in the queue's history.
- `GreenLogStore` — on green verdicts only, one row keyed by that position. The key is ordered, so "previous green" and "next green" become two seeks: the nearest entry below or above a commit's position.

Neither fits `ValidationFactStore`. Facts are looked up by exact identity and URIs do not sort, while previous/next-green needs an ordered seek over positions, a different key shape. A genuinely needed reverse lookup getting its own first-class store is the established pattern here; `RequestURIStore` is the existing example (see [storage README](../../../../stovepipe/extension/storage/README.md#key-value-contract)).

### Completion marker: open

`record` makes no `Request` write in Phase 1, so the stage has no durable marker saying it finished. `buildsignal` drives the Request terminal before `record` ever runs, leaving no non-terminal window to occupy.

### Supporting re-run of the same URI

Widen the key with the `RequestID` the facts already record, so each attempt is its own immutable row, and add a pointer store from `(queue, uri, project)` to the authoritative attempt. Advance to a newer attempt unless the current one is green, since a green build proved the code passed and a later failure only proves the build is non-deterministic.

### Phase 2

- **Pipeline**: `record` publishes the request id onward to `analyze`, for green and not-green facts alike, because a failed build is when project attribution matters most. An earlier draft had `record` retarget the Request from `processing` to `analyzing`, with `analyze` owning the terminal transition. That no longer fits, because the Request is already terminal before `record` runs, so tracking "all facts recorded" belongs to the `analyze` design (see [Completion marker: open](#completion-marker-open)). The message onward carries the request id and queue name, exactly as `record`'s own does, and the consumer stays idempotent.
- **Project facts**: `record` runs the same create-fact-then-notify flow. The fact is keyed by the stable project id carried on the per-project signal, and one `validation.project.recorded` event is published per project identity (see [What the](#what-the-type-carries) `type` [carries](#what-the-type-carries)). Each per-project signal needs its own message id (see [Storage and queue contract](#storage-and-queue-contract)). The Queue's `last_green_uri` describes the whole repository and stays untouched.

Deciding project identity, retrieving the target graph, tracking completion, and defining intermediate degrees belong to `analyze.md`.
