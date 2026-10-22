# Record stage

`record` turns a terminal build outcome into a durable validation fact.

`record` resolves optional project results before selecting the repository degree. It records the repository fact and project facts, then promotes a green repository fact while its Request still owns the successful-build baseline, and finally publishes the repository hook. See [record.go](../../../../stovepipe/controller/record/record.go).

Notifying downstream systems rides the cross-domain hook framework rather than a Stovepipe-specific extension; see [Hooks](#hooks).

See [workflow.md](../workflow.md) for the whole pipeline, [build.md](build.md) for how builds are created, and [buildsignal.md](buildsignal.md) for the terminal-only handoff into this stage.

`record` owns validation facts and Git promotion. `buildsignal` owns the incremental-build baseline and build-slot completion. A successful build becomes available to the next build before project resolution finishes; external validation facts and promotion wait for resolution.

## Record algorithm

1. Decode the request id and queue name, resolve storage, and load the Request. Reject a mismatched queue identity.
2. A succeeded or failed Request proceeds. A cancelled Request publishes `validation.repository.cancelled` without a fact. A superseded Request is acknowledged without effects; a nonterminal Request is an invariant error.
3. Resolve project results using the durable Request and its `TerminalBuildID`. Validate the entire result set before writing facts. Resolver errors leave facts and promotion untouched.
4. Select the repository degree: succeeded → `DegreeGreen`; failed with nonempty project results → `DegreePartial`; failed without project results → `DegreeBroken`.
5. Create the immutable repository fact and its history event. On `ErrAlreadyExists`, adopt the stored fact if it belongs to this Request; a different owner is an invariant error.
6. Create each project fact with its resolver-provided degree, then retain the project-facts-recorded history event. A partial write is completed on replay; existing facts remain authoritative.
7. For a green repository fact, read the Queue and promote only if both `LastGreenRequestID` and `LastGreenURI` still match the Request. Report the last-green timestamp when promotion is eligible. A non-green fact never promotes; the creator reports failure detection latency.
8. Publish `validation.repository.recorded` and acknowledge. A failed operation returns its raw error for backend classification.

The repository fact can become visible before the final project fact is durable; recording is not a transaction across facts. Promotion and the recorded hook occur after all project writes and history complete. Project resolution does not delay the stored baseline, which was already advanced by build completion.

## Validation facts

A fact answers "how broken was this scope at this Queue URI?" Its identity is `(queue, uri, project)`, where `project` is empty for whole-repository greenness and a stable project id.

`queue` is the binding of the store the fact lives in, not a field on the entity. Storage is resolved per queue through `storage.Factory.For`, so a fact only ever exists inside one queue's store and does not need to name it. The column still leads the primary key, `PRIMARY KEY (queue, uri, project)`, because every domain table here has to be shardable by queue: the primary key leads with the queue column and no secondary index leads with anything else. That keeps one queue's rows unreachable through another queue's binding and makes every read a primary-key-prefix scan inside a single queue. It is an invariant rather than a per-table choice, and `make lint-queue-shard` enforces it. It is not MySQL table partitioning; no schema here uses `PARTITION BY`.

So `entity.ValidationFact` carries only the rest of the identity:


| Field       | Meaning                                                                                                           |
| ----------- | ----------------------------------------------------------------------------------------------------------------- |
| `URI`       | Commit URI under validation                                                                                       |
| `Project`   | Empty for the whole repository; stable project id                                                     |
| `Degree`    | Health degree in the closed interval `[entity.DegreeGreen, entity.DegreeBroken]` — `0` is green, `0.5` is partial, `1` fully broken |
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

The fail-closed path is subtler than "no fact". A DLQ reconciler forces the Request to `failed` and does not itself publish here, so reconciliation on its own records nothing. That is not the same as nothing being recorded: a `buildsignal` delivery still in flight can reach this stage afterwards, and it will write `DegreeBroken` from the forced state. See [DLQ and fail-closed behavior](#dlq-and-fail-closed-behavior).

### Phase 1 degree mapping

Whole-repository builds use only the endpoints, mapped from the outcome `buildsignal` stamped on the Request:


| Request outcome | Result                        |
| --------------- | ----------------------------- |
| `succeeded`     | fact at `entity.DegreeGreen`  |
| `failed`, project results available | fact at `entity.DegreePartial` (`0.5`) |
| `failed`, no project results | fact at `entity.DegreeBroken` (`1`) |
| `cancelled`     | no fact                       |


`DegreePartial` is hard coded to `0.5`; it records that project results are available for a failed repository, not the fraction of projects failing. Project degrees are preserved exactly as returned by the resolver.

## Last-green advancement (Queue bookmark)

`buildsignal` advances `LastGreenURI` and `LastGreenRequestID` from the winning durable successful Request outcome, atomically with removal of its slot ownership. The bookmark moves only to a newer ingest id. It is a build baseline and may precede project results, validation facts, or Git promotion; absence of a fact remains fail-closed for deployment gating. Failed and cancelled outcomes never advance it. See [buildsignal.md](buildsignal.md).

## Promotion ref

Promotion requires a finalized green repository fact, all resolved project facts and history, and ownership of the current build baseline. Equal Request id and URI permit retrying an idempotent promotion; a newer baseline causes an older record delivery to skip promotion. This read guard and the external Git write are separate operations, not an atomic transaction.

`SourceControl.Promote` returning `ErrNotFound` means a history rewrite removed the commit from the ref; count and skip it. Other failures return to the classifier.

## Observability

Two things are measured here, because this is where greenness becomes known. Both need a `SourceControl` lookup of a commit's creation time.

- **Last-green age.** A gauge carrying the creation timestamp of the commit the bookmark now points at, emitted after facts are durable while the Request owns the baseline. It is a timestamp rather than an elapsed time so that subtracting it at scrape time gives the current age, and a queue that stops going green keeps reporting a staler value without the stage re-emitting anything.
- **Failure-detection latency.** A histogram of how long the break a build failed on went undetected, measured from the creation time of the base URI it validated against. A distribution rather than a gauge, because what matters is how long breaks typically survive, not how long the last one did. There is no later moment to sample it from, since an elapsed time only means something against the failure that just became known, so this lookup cannot move off the delivery path onto a clock.

Only the delivery that *wrote* the fact reports the latency. That is why `recordFact` returns whether it created the fact and not just the fact itself: a redelivery adopts the stored fact, and a second sample would count one break twice. A full build pins no base URI, so its failures are counted as unmeasurable instead of timed; absent is the ordinary case there, not a fault.

Both are best-effort. Every way the lookup can fail is counted, logged, and swallowed, because an observability fault must not turn an already-recorded outcome into a retry. A non-positive creation timestamp is treated as a broken extension contract and dropped, since emitting it would put a 1970 timestamp in a gauge that reads as an infinitely stale queue, or a decades-long sample in the latency distribution.

## Build slot release

`buildsignal` first persists the winning Request outcome, then completes its slot and successful baseline in one Queue update, before publishing to record. The DLQ reconciler repairs the same completion even for an already-terminal Request. `record` never writes the Queue.

## Hooks

Recording a fact is when the rest of the company can learn "this URI is now green (or not green)". `record` publishes that as a `HookEvent` on Stovepipe's durable `hook` topic; it does not call a notification extension inline.

The mechanics are already settled in [hook-framework.md](../../hook-framework.md) — the envelope, the delivery promise, the per-domain dispatcher stage, the `hook_dlq`, and the reasoning behind each. This section covers only what Stovepipe has to decide for validation facts. The earlier design here, a Stovepipe `Hooks` extension called with `Notify(ctx, ValidationFactRef{…})` as the last algorithm step, is rejected there on both halves: inline calls couple pipeline latency to third-party integrations and drop the notification on a crash between the state write and the call, and a per-domain contract multiplies schemas and sinks for no gain.

The shared `HookEvent` contract at `api/base/hook/`, the hook extension at `platform/extension/hook/`, and Stovepipe's dispatcher stage and `hook_dlq` reconciler are all in place, and this stage publishes to them. What no deployment has yet is a hook that does something with the event: the resolver in `service/stovepipe/server/main.go` returns `noop`.

### Where the publish belongs

Last thing before the ack, after the fact write and after both caches derived from it have moved:

```
resolve projects → create repository fact and history → create project facts and history → promote (green baseline holder only) → publish HookEvent → ack
```

Last because hooks resolve entities from stores and must find the facts already durable and any eligible promotion complete. A newer successful build may already own the baseline while its record work is pending. Inside the delivery rather than after it, because that is what makes the event lossless without an outbox — the state writes are recognize-and-skip on redelivery, so a crash before the ack replays the whole chain.

### Event shape


| Envelope field | Value for a validation fact                                                     |
| -------------- | --------------------------------------------------------------------------------- |
| `source`       | `stovepipe`                                                                     |
| `type`         | `validation.repository.recorded` or `validation.repository.cancelled` (see below) |
| `version`      | `0` — a fact is create-only and has no version to report                        |
| `timestamp_ms` | Publish time                                                                    |
| `id`           | `source` / `type` / request id / `0`                                            |


The **subject** is the Request — a payload fact rather than an envelope field, but it is what `id` is minted from and what the event partitions on. The Request over the URI keeps partitioning identical to the `record` topic's own, so per-request ordering carries through the seam, and it hands a consumer a way back into the pipeline. The two are near-interchangeable anyway: ingest dedups on `(queue, uri)`, so one Request means one URI.

Both types carry identity and nothing else: `queue` and `request_id`. A hook resolves the fact, its degree, and the bookmark from the stores. `queue` has to be there because neither the envelope nor the fact entity carries one, and a hook needs it to resolve per-queue storage before it can load anything by `request_id`. Identity rather than a snapshot because a snapshot is never fresher than its publish moment, so a consumer acting on one may be reasoning about state that has since moved; the cost is a store read per hook.

So a hook reads state as it is when the hook runs, not as it was when the event was published. That is the same ordering the step list depends on, and a consumer needing the values a green commit was recorded with reads the immutable fact rather than the bookmark or the ref.

### What the `type` carries

Data belongs in `type` when consumers need to avoid receiving the event, and in the payload when they need to interpret it. The type names the scope, `validation.repository.recorded`, with `validation.project.recorded` beside it in Phase 2.

`validation.repository.cancelled` is a separate type rather than a `state` on the recorded one, because the two differ in what a consumer can do with them: a recorded event carries a verdict to act on, a cancelled event only says to stop waiting. A sink that gates on greenness subscribes to one and ignores the other, and naming a cancellation "recorded" would claim a fact that was never written.
### What a consumer can and cannot assume

Ordering is per-subject only and the subject is the Request, so events for *different* Requests can arrive out of order. A consumer must not infer "the newest green commit" from arrival order; it should compare request ids by ingest order (`entity.CompareRequestID`) or read the bookmark, which is monotonic by construction.

Absence of an event is not a signal. A Request abandoned before any build went terminal never reaches this stage, and a superseded one publishes nothing, so a consumer waiting for one event per ingested commit waits forever on those. Gating keeps treating "no recorded fact" as not green. The converse holds too — an event is not proof the code was tested, since a fail-closed Request can produce a broken fact without a build having failed.

Hooks here must be idempotent on `id`, as everywhere. "Fire-and-forget" describes downstream consumption, not the publish: `record` never waits for a hook, but a failed *publish* fails the delivery. Per `[platform/errs](../../../../platform/errs/README.md)` rule 4 it is not wrapped retryable just because replaying it is convenient, so it dead-letters. Once primary retries are exhausted, `record_dlq` stops trying to complete the original work: it reloads the already-terminal Request, retains `record_abandoned` history, logs and counts the abandonment, and acknowledges. It does not create a fact, advance the bookmark, promote, or publish a hook. Any partial durable effects already written remain authoritative; absence of a fact remains fail-closed, and a later request or operator action may repair external state. Promotion is one example of this case: an outbound call rejected for persistent permissions is not attempted again from the DLQ, and its diagnostic remains in structured logs.

## Request lifecycle

Phase 1 uses the states in [stovepipe/entity/request.go](../../../../stovepipe/entity/request.go). `record` runs *after* the Request is terminal: `buildsignal` projects the build's terminal status onto it as `succeeded`, `failed`, or `cancelled` (`RequestState.HasBuildOutcome()`), and only then publishes. So `record` reads an outcome and writes no state. `superseded` is terminal without an outcome.

Phase 2 broadens "complete" to "all planned facts recorded", which needs a marker this stage does not own; see [Completion marker: open](#completion-marker-open).

## Storage and queue contract

`ValidationFactStore` is key/value-shaped, and queue-bound rather than queue-parameterized:

- `Create(ctx, fact)` writes one immutable fact for the bound queue. It returns `ErrAlreadyExists` when the composite identity is already taken, leaving the stored fact untouched.
- `Get(ctx, uri, project)` reads one fact by the rest of its identity, and returns `ErrNotFound` when none exists. The `project` argument is empty for the repository and otherwise names a resolved project.

There is no `Update`. The first fact written for an identity is the permanent answer, and a caller that needs to know whether it won the race reads `ErrAlreadyExists` and then loads the winner.

The topic key, message, primary consumer, and DLQ consumer all exist.


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
| Promotion target unknown to the ref                | absorbed, not raised | `sourcecontrol.ErrNotFound` means a rewritten history dropped the commit          |
| SourceControl resolution or `Promote` failure      | raw error            | backend classifier has the required failure knowledge                             |
| Commit-timestamp lookup for a metric               | swallowed            | counted and logged; observability must not retry a recorded outcome               |
| ValidationFactStore, QueueStore, RequestStore      | raw error            | backend classifier has the required failure knowledge                             |




## Edge cases and idempotency

Every effect is recognize-and-skip, so a redelivery after a complete run re-runs each step as a no-op. The publish re-fires, and the queue's dedupe on the derived `id` absorbs it.

- **Request not visible.** A storage defect rather than lag, since the publish follows the committed outcome write. Non-retryable.
- **Fact already created.** Load it and continue from the stored fact. A fact from a *different* Request, or a Request carrying no build outcome, is an invariant violation rather than an expected outcome.
- **Duplicate builds for one Request.** Absorbed a stage earlier: `buildsignal`'s outcome write is first-writer-wins, so the Request carries one immutable verdict and `record` never sees a competing one.
- **Head equals last-green.** The build still produces a terminal verdict. A green result may share the bookmark's URI, but the guard compares request ids rather than URIs, so an equal-or-older candidate skips and nothing regresses.
- **Green fact recorded out of order across Requests.** An older green commit can reach this stage after a newer one. Its fact is written as usual, since facts are per-URI and independent, but the bookmark guard skips it, and because it does not hold the bookmark it does not promote either. Neither cache moves backwards.
- **Cancelled build.** Stovepipe never initiates cancellation, but a backend may still report it. No fact is written; the slot was already released and the Request already stamped `cancelled`. The fact identity stays unclaimed, and nothing can claim it today, since `cancelled` is terminal and re-validation does not exist. Recovery in practice is the next commit; the unclaimed identity only matters to a future re-run mechanism (see [Supporting re-run of the same URI](#supporting-re-run-of-the-same-uri)).
- **History rewrite while a build runs.** The Request keeps the strategy and URI pinned at admission, and record stores the fact about that immutable URI. A later head is handled independently by `process`. If the rewrite dropped the commit from the ref, the fact and the bookmark still stand, since they describe a commit and not a ref, and only the promotion is skipped.
- **A fail-closed terminal outranks a build that passed.** The degree derives from `R.State`, so a Request forced to `failed` by DLQ reconciliation records a non-green degree even when one of its builds reports success afterwards. Reachable today, and permanent once written; see [DLQ and fail-closed behavior](#dlq-and-fail-closed-behavior).
- **Crash during fact persistence.** Redelivery resolves projects again, adopts existing immutable facts, and completes missing facts and history before promotion.
- **Crash before promotion.** Redelivery retries promotion if the Request still owns the baseline.



## DLQ and fail-closed behavior

Every primary subscription comes from `DefaultSubscriptionConfig`, which enables dead-lettering with the `_dlq` suffix, so a message that is rejected outright or runs out of retries moves to its stage's dead-letter topic. Stovepipe registers a reconciler for every pipeline DLQ, including `record_dlq`.

Two different things put a message there, and only one is a poison payload. A delivery that fails with its retry budget spent is dead-lettered by the nack itself, carrying the reason it actually failed. A delivery that never reaches a nack, because it crashed or because its **ack failed** and the visibility timeout redelivered it, is dead-lettered by the poll loop once `retry_count` reaches `MaxAttempts` (3 by default), without the controller running on that final attempt and with only a generic reason recorded. So a missing reconciler exposes more than malformed messages: a fact can be lost to a storage failure that would have succeeded on a later retry, or to an ack that never landed even though the write did.

Gating stays safe, because everything this stage can lose reads as not-green: a Request with no fact is indistinguishable from one not yet validated. What is lost is the *fact*. A green build whose fact write permanently failed leaves the URI looking unvalidated, while its successful-build baseline remains available to the next head. A lost notification joins that list, and unlike the fact it gets no second chance from a later commit.

The reconciler does not re-run the stage. It loads the Request only to identify existing durable state and retain a stable `record_abandoned` event. The DLQ consumer uses `errs.AlwaysRetryableProcessor`, so a transient Request read or history persistence failure keeps retrying until that observable abandonment is durable. A malformed payload, invalid or unresolvable queue identity, or missing Request cannot be repaired by redelivery; those cases are logged, counted, and acknowledged so poison cannot occupy the DLQ indefinitely. No DLQ path creates a fact, advances a bookmark, invokes source control, or publishes a hook.

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
