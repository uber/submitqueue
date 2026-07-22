# Process stage

`process` is the first pipeline consumer after ingest. For each newly accepted head it decides *how* to validate (incremental since last-green vs. full monorepo), enforces per-Queue concurrency, coalesces a backlog of heads down to the latest, and publishes the winner to `build`. See [workflow.md](../workflow.md) for where it sits in the pipeline.

It handles everything *before* the first build is triggered: it does not run builds, parse URIs, or record greenness.

## Input, partitioning, and the single-writer property

Ingest publishes a `ProcessRequest` (request id only — producer and consumer share the store) to the process topic, **partitioned by Queue name**. Two consequences the design relies on:

1. **One partition per Queue**, and `BatchSize = 1` (strict serialization; see [sql-queue-rfc.md](../../sql-queue-rfc.md#strict-serialization-opt-in)) — so at most one `process` invocation runs per Queue at a time. The read-modify-write on the Queue row (gate check, count increment) is therefore race-free **without** a transaction, the same property SubmitQueue's `validate` gets from per-queue partition leasing.
2. **`process` is not the only Queue-row writer.** `ingest` stamps `latest_request_id` (the request id of the newest accepted head, not a URI; see [Backlog coalescing](#backlog-coalescing)) and `record` advances the `last_green_uri` bookmark and releases slots. They touch different fields under optimistic-locking CAS, so concurrent writes converge instead of clobbering.

## Process algorithm

For a delivery carrying request id `R`:

```
1. Load Request R from the request store.
   - not found -> non-retryable (storage is read-after-write consistent; see [storage README](../../../../stovepipe/extension/storage/README.md)).
2. If R.State is terminal (superseded / recorded-green / recorded-not-green):
   - ack and return (idempotent no-op).
3. If R.State is processing (strategy already recorded):
   - re-publish R to build (the prior publish may have failed), ack, return.
4. R.State is accepted. Load the Queue row Q.
5. Coalesce: if CompareRequestID(R.Queue, R.ID, Q.latest_request_id) < 0:
   - a newer head exists -> mark R superseded, ack, return. (No slot consumed.)
6. R is the latest head. Gate: if Q.in_flight_count >= max_concurrent (from queue config; see below):
   - defer (Option 1 or Option 2 below) -> re-check until the slot frees (admit) or a newer head supersedes it. See [Waiting for a slot](#waiting-for-a-slot).
7. Admit R:
   a. Derive build strategy + baseline (see "Build-strategy decision").
   b. CAS the Queue row: in_flight_count += 1.
   c. CAS the Request: accepted -> processing, persist build_strategy + base_uri.
   d. Publish R to build.
   e. ack.
```

Step 5 runs regardless of the gate: an intermediate head is superseded on sight (even mid-validation), because superseding consumes no slot.

## Build-strategy decision

To admit a Request (step 7a), `process` reads the Queue's **last-green URI** and asks the same `SourceControl` implementation ingest uses:

1. **No** last-green URI (cold start) → **full**, no baseline.
2. Last-green URI present → call `SourceControl.IsAncestor(lastGreen, R.URI)`:
   - **true** — fast-forward: **incremental-since-green**, baseline = last-green URI; `build` validates only the delta.
   - **false** — history rewrite (force-push, rebase): **full**, no baseline. The stale bookmark must not be a build baseline.
   - **`ErrNotFound`** — a URI isn't on the ref (ancestry uncertainty). A full build is always a valid superset, so fall back to **full** and log a warning; not a retryable error.
   - **any other error** (connection/timeout) — return it raw; the consumer's classifier decides retryability (see [errs](../../../../platform/errs/README.md)).

Strategy and baseline are persisted on the Request and are **immutable**: a redelivery sees `processing` at step 3 and never re-derives them. `SourceControl` owns ancestry; `process` never parses a URI.

## Per-Queue concurrency gate

Validation is expensive and shares a baseline, so heads arriving while an earlier one runs `build → buildsignal → record` must not all start builds at once.

**Runtime state** lives on the Queue row; the **concurrency cap** does not — it is deployment configuration, resolved at gate-check time the same way SubmitQueue separates `storage` from `queueconfig` (see [submitqueue/extension/queueconfig/README.md](../../../../submitqueue/extension/queueconfig/README.md)): pipeline stages read mutable state from the store and read knobs like `max_concurrent` from a config `Store` (or a wiring default for MVP). Config is not written by ingest/process/record and does not need optimistic locking.

| Source | Field | Meaning |
|---|---|---|
| Queue row | `last_green_uri` | Bookmark `record` advances on whole-repo green; empty until first green. |
| Queue row | `in_flight_count` | Requests past `process` and not yet terminal. `process` increments on admit; `record` (or DLQ reconciliation) decrements on terminal. |
| Queue config | `max_concurrent` | Cap on concurrent in-flight validations. **Default 1** (global wiring default for MVP; per-queue override when a Stovepipe `queueconfig` extension lands). |

A slot is held for the **entire** Phase 1 cycle (`process → build → buildsignal → record`), not just while `process` runs. It is released when the Request reaches **any** terminal state and `in_flight_count` is decremented — `record` writing green *or* not-green, or the DLQ reconciler forcing a terminal not-green (see [integrity](#in_flight_count-integrity)). A build *failure* frees the slot just like a success; only a Request that never terminates keeps its slot.

**Liveness — a stuck Request wedges the whole Queue.** The slot is shared per-Queue, so a Request admitted but never driven terminal holds the only slot and stalls the whole Queue. The fail-closed path in [workflow.md](../workflow.md#fail-closed-on-unprocessable-work) prevents this: every admitted Request terminates — `build`/`buildsignal` errors retry to `MaxAttempts`, then dead-letter, and the DLQ reconciler forces a conservative terminal not-green, freeing the slot. Residual risk is operational: a **poison DLQ message** the reconciler can't process loops forever (it runs always-retryable), wedging that Queue until an operator removes it. The gate makes the blast radius the whole Queue, not one Request — monitor and alert on the DLQ.

**Future alternative — time-bounded leases.** To self-heal instead of relying on operator cleanup, `in_flight_count` could become a set of `{owner, expires_at}` leases: the gate counts only unexpired leases, terminal transitions drop the owner's lease, and a leaked lease is reclaimed on expiry — bounding any stall to a `max_validation_ms`. A *list* of leases would also generalize to `max_concurrent > 1`. Deferred; `in_flight_count` plus the fail-closed path is enough for MVP.

`max_concurrent` is a scalar, and `= 1` is an MVP simplicity choice, **not** a correctness requirement — see [Raising `max_concurrent`](#raising-max_concurrent-speculative-validation).

## Raising `max_concurrent` (speculative validation)

Setting `max_concurrent = N > 1` overlaps validations to start work sooner, and it is **safe** — because Stovepipe validates **already-landed, linear trunk heads**, not pre-merge candidates. Successive commits (`G0 → A → B → C …`) each contain everything below them, so validating `G0..B` already tests A+B together. (A pre-merge queue serializes to catch "two changes green alone, broken combined"; that risk isn't present here.) A green result for head `H` on baseline `B` is an immutable property of `H`, true no matter where last-green moves afterward.

The scheme: each admit pins its baseline to last-green *at admit time*; a green head is adopted even if last-green has since advanced. A late green result is either the newest (adopt) or already behind the pointer (**moot** — dropped, never a regression).

Correctness rests on four rules, all with MVP primitives already in place:

1. **Pin to last-green at admit** — a known-green baseline, which `process` already reads.
2. **Advance last-green by ingest order, monotonically** — `record` adopts the highest-counter green (per `CompareRequestID`) and never regresses. (Free at N=1, since verdicts land in order; explicit at N > 1.)
3. **Coalesce to the latest N** instead of the latest 1 — otherwise intermediates are superseded on sight and nothing overlaps.
4. **Fall back to serial on a rewrite** — the linear/superset property is what makes "adopt highest green" sound; rewrites already force a full build with no baseline.

The only cost is speculation: a build on an older baseline re-tests deltas a concurrent build already greened past — correct but wasteful, growing with how far the baseline lags. Bounding that lag ("drain before adopting a new baseline") is a **cost governor, not a safety gate**. It inherits, but doesn't worsen, the incremental-build soundness assumption already used at N=1.

So per-baseline concurrency isn't an unsolved semantics problem — it's speculative validation with a lag-bounded baseline. It's deferred only for the per-lineage bookkeeping (rules 2–4, derivable from request-id ingest order + `Request.BaseURI`) and a coalesce-latest-N policy, neither of which the MVP forecloses.

## Backlog coalescing

While a validation runs, ingest keeps admitting newer heads (distinct Requests, deduped on `(Queue, URI)`). Only the **latest** is worth validating; intermediates are skipped.

"Latest" is the **monotonic ingest order**, not VCS history:

- Ingest mints ids from a per-Queue counter (`request/<queue>/<n>`). The counter suffix is the order key (higher = later ingest = newer head); there is no separate `Sequence` field on `Request`.
- Ingest also CASes `Queue.latest_request_id` to the accepted request's id when it is newer (via `CompareRequestID`) — the **out-of-band latest pointer** (a request id, distinct from `last_green_uri`) that makes coalescing a single-row comparison.

Why not `SourceControl.History`: a history walk is expensive, and after a rewrite the superseded URIs may be off-ref entirely — exactly where history order is meaningless. Ingest order authoritatively says which head we learned of last.

**Why not `SourceControl.Latest`** (rejected): comparing `R.URI` to the live ref head leaks work outside the ingested set. The ref moves whenever a commit lands — independent of what the poller reported or ingest minted — so whenever it is ahead of the newest Request, *every* Request differs from `Latest` and is superseded, admitting nothing and chasing a commit `process` was never handed. Invariant: **`process` only validates a commit ingest identified.** Ingest order is defined over exactly that set; `Latest` isn't. Nor can `counter.Peek` stand in: ingest spends a counter value on a dedup race-loss, so the highest *minted* counter can exceed the highest *real* Request's — an equality test against the pointer would never match. Stamping `latest_request_id` only after a successful request create keeps it aligned.

Ordering caveat: `counter.Next` doesn't guarantee assignment order, so under concurrent same-Queue ingest (rare — one serial poller) "highest sequence" may not equal "most recently reported". They agree in practice, and a rare inversion self-corrects next poll. Acceptable for MVP.

**The pointer prevents deadlock.** Under `BatchSize = 1`, Option 1 blocks its partition while waiting, so `process` can't learn of newer heads *from the stream* during that wait. Option 2 unblocks the partition, so newer ingest deliveries can arrive immediately. Both options read `latest_request_id` from the Queue row on every wake-up (step 5), so a stale waiter — blocked or delayed — still supersedes correctly. Ingest stamps the pointer independently of the partition (see [Backlog coalescing](#backlog-coalescing)).

**Progress (no starvation).** Superseding is always forward motion toward the newest head, and the newest head is never superseded (nothing is newer). So as long as `process` supersedes faster than ingest adds heads — it does, since superseding is a CAS + ack with no build, far cheaper than the poll cadence — a build always starts; a high commit rate just coalesces more intermediates away.

Superseded Requests reach an explicit terminal `superseded` state, so "not yet validated" is never confused with "skipped for a newer head".

## Concurrency lifecycle

The gate is **not** tied to `process` returning; a slot taken at admit is held until Phase 1 terminates at `record` (or DLQ reconciliation).

**Rules**

1. **One slot per in-flight validation** (MVP: one per Queue). `process` increments `in_flight_count` on admit; `record` decrements on terminal.
2. **No skip-ahead while in-flight.** The latest head waits for a slot until the running validation completes; it never preempts.
3. **Intermediates are superseded on sight**, gate open or closed — no slot consumed (step 5).
4. **Coalesce-to-latest on gate open.** When a slot frees, the waiting latest head is admitted.
5. **The cycle repeats** for whatever accumulated during the previous validation.

**Worked example** — Queue `monorepo/main`, `max_concurrent = 1`, poller reporting heads A→F:

1. **A** admitted (`in_flight_count = 1`), published to `build`.
2. While A runs, **B**, **C**, **D** are ingested (`latest_request_id = D.id`).
3. B: older than D → **superseded** (acked), though A is still in flight. Same for **C**. D is latest but the gate is closed → **waits for slot** (Option 1 or 2).
4. A's build finishes → `record` records A's greenness, `in_flight_count → 0`.
5. D's re-check → gate open, D still latest → **D admitted**, published to `build`.
6. While D runs, **E**, **F** ingested (`latest_request_id = F.id`). E superseded on sight; F waits for slot.
7. D completes → slot frees → **F admitted**.

A, D, F each get a full cycle; B, C, E end `superseded`. No intermediate is validated individually — intentional for MVP.

**What does not happen**

- `process` returning does **not** free a slot — only `record` (or DLQ reconciliation) does.
- A newer head does **not** preempt an in-flight validation.
- Deferred messages are **not** failed or dead-lettered — they wait for the gate (see [Waiting for a slot](#waiting-for-a-slot)).

## Idempotency and at-least-once delivery

Every branch is safe under redelivery:

- **accepted, no strategy** → full admit path. On a crash after incrementing `in_flight_count` but before persisting `processing`, redelivery re-reads `accepted` and re-runs; the increment re-applies only if the count CAS hasn't already moved (see integrity below).
- **processing** → re-publish to `build` and ack. The `build` consumer is keyed on the request id and idempotent, so a duplicate publish is harmless.
- **terminal** (superseded / recorded) → ack, no-op.
- **deferred (waiting for slot)** → no state or count change; pure deferral (re-enters on renewal or reschedule).

The window to handle is "count incremented, state not yet `processing`". Admit does the increment and the state transition as two ordered CAS writes, and the decrement is tied to the state transition, not a side counter (see integrity below).

## `in_flight_count` integrity

`in_flight_count` is a cache; the source of truth is **the set of non-terminal Request rows for the Queue**. Two rules keep it from drifting:

1. **Decrement is bound to the terminal transition.** The single CAS that moves a Request non-terminal → terminal (in `record` or the DLQ reconciler) also decrements. Being CAS-guarded, it fires exactly once per Request even under redelivery.
2. **Increment is bound to the admit transition.** `process` increments only on the `accepted → processing` CAS; a redelivery of an already-`processing` Request takes step 3 and does not increment again.

On a crash between admit and `record`, the Request stays non-terminal; visibility-timeout redelivery drives it forward, and the fail-closed DLQ path eventually forces it terminal, decrementing as it does. The count can drift high only transiently and self-heals as stuck Requests terminate. A reconciler that recomputes the count from non-terminal rows can be added later if drift proves real, but isn't required for MVP.

## Edge cases

- **Re-ingest of a superseded URI.** Ingest dedups on `(Queue, URI)` and returns the existing (now terminal `superseded`) id; `process` acks it as a no-op (step 2). Correct: a URI is only superseded for a *strictly newer* head, so re-validating it is never wanted.
- **Gate closed, no newer head.** The single latest head waits for a slot until the in-flight validation completes — the steady state, not an error.
- **Head equals last-green.** `IsAncestor(lastGreen, R.URI)` with `R.URI == lastGreen` is degenerate; treat as already-green, or (simpler) run an incremental build with an empty delta. Left to `build`.
- **Queue row missing.** First head for a Queue: ingest get-or-creates the row with defaults (`in_flight_count = 0`, empty `last_green_uri`). `process` treats a missing row as non-retryable — storage's read-after-write guarantee means ingest's write is already visible by the time `process` reads it, so a miss is a storage defect, not lag.

## Entity model

### Queue (new persisted entity)

Runtime coordination only — fields the pipeline writes under CAS:

| Field | Role | Written by |
|---|---|---|
| `name` | Stable logical id (`monorepo/main`); the string ingest accepts | ingest (create) |
| `last_green_uri` | Bookmark; empty until first green | record |
| `in_flight_count` | Active Phase 1 validations | process (+1), record/DLQ (−1) |
| `latest_request_id` | Request id of the newest head ingest accepted | ingest |
| `version` | Optimistic-locking version | all writers |

Per-queue knobs such as `max_concurrent` live outside this row — see [Per-Queue concurrency gate](#per-queue-concurrency-gate).

### Request (additions to the existing entity)

| Field | Role |
|---|---|
| `ID` | Globally unique id (`request/<queue>/<n>`); the counter suffix is the ingest-order key for coalescing |
| `BuildStrategy` | `incremental_since_green` \| `full`; immutable once set by `process` |
| `BaseURI` | Last-green URI used as the incremental base; empty for full builds |

**States** (extending today's `accepted`-only machine):

| State | Meaning | Terminal? |
|---|---|---|
| `accepted` | Ingested, awaiting `process` | no |
| `processing` | Admitted; strategy recorded; build in flight | no |
| `superseded` | Skipped by coalescing for a newer head | **yes** |
| *(owned by record)* recorded-green / recorded-not-green | Phase 1 outcome | **yes** |
| *(later)* building, recording, … | Finer states as downstream stages need them | — |

Transitions use the repo's optimistic-locking pattern: compute `newVersion = oldVersion + 1`, call `RequestStore.Update(ctx, req, oldVersion, newVersion)`, assign `req.Version = newVersion` only on success (see [storage README](../../../../submitqueue/extension/storage/README.md) and [CLAUDE.md](../../../../CLAUDE.md)).

## Storage contract additions

New key/value-shaped operations (single-key reads/writes, no server-side filtering or aggregation):

- **`QueueStore`** (new): `Create(ctx, queue)`, `Get(ctx, name)`, and `Update(ctx, queue, oldVersion, newVersion)` (CAS). Callers orchestrate get-or-create; ingest CASes `latest_request_id`; `process` CASes `in_flight_count`; `record` CASes `last_green_uri` + `in_flight_count`.
- **`RequestStore`**: no new methods — the added `Request` fields ride the existing `Create`/`Update` CAS.

No "list requests by queue/state" query is introduced; coalescing uses the single-row `latest_request_id` pointer instead, keeping the contract satisfiable by a plain KV backend.

## Waiting for a slot

When the gate is closed, `process` must defer the latest head without admitting it (no `in_flight_count` increment, no publish to `build`). Two options:

- Park until a build slot opens, and extend visibility
- Use PublishAfter to re-enqeue the current head if no build slot is available

Both re-run the same **coalesce-then-gate** checks on every wake-up (steps 5 → 6):

1. **Stale? (checked first.)** If `CompareRequestID(R.Queue, R.ID, Q.latest_request_id) < 0`, `R` is no longer latest → supersede it (ack). A newer head is admitted by its own delivery when its slot attempt runs.
2. **Slot free?** If `in_flight_count < max_concurrent` (from config) and `R` is still latest → admit (step 7).

Neither option admits to `build` until the gate opens.

### Option 1: park and extend visibility

**Mechanism.** Keep the in-flight delivery alive. Loop: call `ExtendVisibilityTimeout` on an interval (renews the lease **without** incrementing `retry_count`), reload the Queue row, run coalesce-then-gate. Never ack or nack while waiting. Honor context cancellation — on shutdown return promptly; the head resumes on redelivery.

**Partition behavior.** Under `BatchSize = 1`, the delivery stays in-flight and **blocks the partition** until it admits or supersedes. Newer ingest messages queue behind it in the log; coalescing for the waiting head relies on `latest_request_id` from the Queue row, not on newer deliveries arriving while blocked.

**Walkthrough** — Queue `monorepo/main`, `max_concurrent = 1`, heads A→F, Option 1 chosen:

1. **A** admitted, published to `build` (`in_flight_count = 1`). `process` returns (acks); A continues through `build → buildsignal → record`.
2. **B**, **C** ingested. Their deliveries run behind A's in-flight validation (not behind a gate wait yet) → superseded on sight (step 5), acked.
3. **D** ingested (`latest_request_id = D.id`). D's delivery: latest, gate closed → **park** (extend loop begins; partition blocked).
4. While D waits, **E**, **F** ingested (`latest_request_id = F.id`). Their process messages sit in the log behind D's blocked delivery — they do not run yet.
5. D's renew loop re-reads the Queue row → D is older than F → **supersede D**, ack (partition unblocks).
6. **E**'s delivery runs → superseded. **F**'s delivery runs → latest, gate still closed → **park**.
7. A completes at `record` → `in_flight_count → 0`. F's renew loop sees gate open → **admit F**, publish to `build`, ack.

**Supersede reasoning.** Simple while blocked: the waiter periodically re-reads `latest_request_id` and supersedes itself when a newer head appears. Intermediates (B, C, E) only run once the partition unblocks enough to reach their offsets.

**Tradeoffs.**

| Pros | Cons |
|---|---|
| Delivery API only — no publisher in `process` | Goroutine blocked in renew loop per waiting head |
| One delivery, minimal log churn | Partition blocked — newer heads wait in the log |
| `ExtendVisibility` does not increment `retry_count` | Lease lapses (missed renewal) increment `retry_count` → `MaxAttempts` risk |
| Strict in-partition serialization while waiting | Tune renewal interval inside `VisibilityTimeoutMs` |

**Safety.** If renewal lapses, another worker may redeliver the same head concurrently. Harmless: admission is CAS-guarded; one admit wins, the other sees `processing` (step 3) and no-ops.

### Option 2: ack and `PublishAfter`

**Mechanism.** On gate closed and still latest: **ack** the current delivery, then **`PublishAfter`** the same `ProcessRequest` to the process topic (same partition key = queue name) after a short delay. Each wake-up is a **fresh** message (`retry_count` starts at 0). Run coalesce-then-gate at the top of every wake-up; if still latest and gate still closed, ack and `PublishAfter` again. Only reschedule when both conditions hold — otherwise supersede or admit immediately.

**Partition behavior.** Acks free the partition. Newer ingest deliveries (E, F, …) are processed and superseded while the latest head waits on a timer. Deferred rows use `visible_after` and are skipped until due — the same non-blocking property as a nacked message — so a reschedule at offset 11 can run *after* a newer ingest message at offset 12. Delivery order reshuffles relative to strict log order; coalescing keys off `latest_request_id`, not offset.

**Walkthrough** — same scenario, Option 2 chosen:

1. **A** admitted, published to `build` (`in_flight_count = 1`).
2. **B**, **C** ingested → delivered and **superseded** on sight.
3. **D** ingested (`latest_request_id = D.id`). D's delivery: latest, gate closed → **ack + `PublishAfter(D, delay)`**. Partition free.
4. **E**, **F** ingested (`latest_request_id = F.id`). **E** delivered → superseded. **F** delivered → latest, gate closed → **ack + `PublishAfter(F, delay)`**. D's pending reschedule is now redundant.
5. D's timer fires → D is older than F → **supersede D**, ack (cheap no-op).
6. A completes at `record` → `in_flight_count → 0`. F's timer fires → gate open, still latest → **admit F**, publish to `build`, ack.

**Supersede reasoning.** Straightforward: every wake-up (immediate or delayed) runs step 5 first. Stale delayed messages (D) supersede on sight. Only the current latest (F) should schedule the next wait. Older delayed rows are expected no-ops, not errors.

**Tradeoffs.**

| Pros | Cons |
|---|---|
| No `MaxAttempts` burn while waiting (each cycle acks; reschedule is fresh) | Ack + new log row per poll cycle while gate is closed |
| Partition stays hot — intermediates supersede immediately | `process` needs publisher + topic registry |
| Worker returns between waits — no blocked goroutine | Delivery order ≠ ingest order (correctness unaffected) |
| Stale delayed waiters self-clean via step 5 | Redundant delayed rows if multiple heads reschedule before timers fire |

### Comparison

| | **Option 1: park + extend** | **Option 2: ack + `PublishAfter`** |
|---|---|---|
| **`MaxAttempts`** | Safe when renewals keep up; lease lapses increment `retry_count` | Safe — waiting never increments `retry_count` |
| **Partition (`BatchSize = 1`)** | Blocks until admit or supersede | Unblocks; newer heads process immediately |
| **Supersede** | Waiter polls `latest_request_id` in loop | Immediate deliveries + stale timers supersede on wake-up |
| **Churn** | One delivery, periodic extends | One new log row per wait cycle |
| **Wiring** | `ExtendVisibilityTimeout` on `Delivery` | Publisher + topic registry in controller |
| **Worker** | Goroutine in renew loop | Returns; timer brings work back |

Implementation picks one option and wires step 6 to it. A future `consumer.ErrHold` primitive would resemble Option 2 (release + redeliver) without self-republish; neither option is chosen here yet.

## Batch consume

Coalescing uses the latest-request pointer one delivery at a time. Intermediates are each delivered once and superseded. The latest head adds no extra rows under Option 1 (it waits in place); under Option 2 it adds one reschedule row per wait cycle while the gate is closed.

If that churn matters at scale, an optional `BatchController` (receiving `[]Delivery` per poll) would let `process` supersede all intermediates in a single tick — an optimization over the single-delivery path that can land later without changing the state machine or storage contract.
