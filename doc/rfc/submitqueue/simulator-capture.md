# Simulator: Data Capture

What a SubmitQueue simulator has to record from production: what each record is keyed on, where capture attaches, and what shape the records take.

## Problem

Changing orchestrator logic today leaves two options: trust a unit test against synthetic fixtures, or ship the change and watch production. Neither says whether the change behaves differently from what is running now, or whether the difference is an improvement.

Replaying production against a candidate does. Record what production fed each component and what each component decided, run a candidate over the same input, compare.

The first phase runs **extension replay with no candidate at all** — identical code on both sides, production captured live and replayed on a shadow environment. Both sides run the same implementation, so any difference is a bug in the capture or replay code. Until that run is clean there is no way to tell a real behavioural difference from a capture bug.

All of it rests on the captured data, and capture is harder than it looks for two reasons.

**The database does not retain what a component saw.** State is overwritten: [`batch`](../../../submitqueue/extension/storage/mysql/schema/batch.sql) and [`speculation_path_set`](../../../submitqueue/extension/storage/mysql/schema/speculation_path_set.sql) are updated in place, so a later read returns current state, not the state a past run acted on. Output is reduced: the [dependency-analysis controller](../../../submitqueue/orchestrator/controller/dependencyanalysis) turns `conflict.Analyzer`'s result into a bare list of batch IDs, discarding the conflict type and any duplicates. ([`change`](../../../submitqueue/extension/storage/mysql/schema/change.sql) is the exception, write-once and immutable, and that matters later.)

**A record keyed by the call that produced it stops resolving once the calls change.** Switch [`pathoverlap`](../../../submitqueue/extension/conflict/pathoverlap) from `ByFile` to `ByDirectory` and more batches conflict, batches stay in flight longer, and `conflict.Analyzer` is invoked with an in-flight set that never occurred in production. A store keyed by call arguments has no entry for that call and never will.

Throughout, **corpus** means the store these records go into. **Runway** is the separate service that performs merge-conflict checks and the merges themselves on SubmitQueue's behalf; it holds no storage of its own.

## Replay modes

Three ways a simulator will replay what it captured, all served by the same capture:

| Mode | What it drives | What it answers |
|---|---|---|
| **Extension replay** | one extension — a `conflict.Analyzer`, a `Scorer` — from recorded input | does this candidate decide differently on identical input? Cheap enough to run per change |
| **Controller replay** | one controller, from a recorded queue snapshot | does it emit the same actions and writes? Reaches logic living in controllers, not extensions |
| **Whole-workflow replay** | every controller forward over a captured trace | what does the difference do to land time and CI cost, once batching and speculation react? |

All three span SubmitQueue and Runway. Whole-workflow replay also needs a build-outcome model, a clock, and metrics we can defend; those are modelling problems and are not settled here. What is settled is that one capture mechanism feeds all three; which records a given replay loads is covered in [Scoping a replay](#scoping-a-replay).

## Three kinds of capture

Capture is three things, told apart by two different tests:

| | Resolution | Decision | External outcome |
|---|---|---|---|
| Example | file list and line counts for a change | `conflict.Analyzer`'s `[]Conflict` | a CI verdict, a merge result, a request arrival |
| **Key survives a policy change** | yes | **no** — keyed on the call that produced it | yes, where content-hashed |
| **Recomputable from what we own** | yes | only if its input is | **never** |
| Role | the input | the baseline | the boundary |

The first test — does the key survive — separates resolution from decision, and the [granularity rule](#the-granularity-rule) is how you apply it. The second — can we recompute it — sets external outcome apart from both.

Sorting by direction instead is tempting and wrong. `heuristic`'s score is *returned*, yet what gets stored is the per-change content behind it, which survives. The `speculate` snapshot is *read*, yet every identity in it is constructed, so nothing survives. Sort by the key, not by which way the data flowed.

**External outcomes must be captured or faked.** Nothing recovers a CI verdict from a build that ran on a fleet whose state that afternoon is gone. [`BuildRunner`](../../../submitqueue/extension/buildrunner) is the clearest case: its result depends on the CI backend at execution time, and no identity pins that. The same holds for Runway's merge result and for request arrival timing. These are the only places in the pipeline where nothing can be recomputed, because everything else is our own deterministic logic over our own data.

**Decision capture is recoverable only if its input is, and often it is not.** The `Scorer`'s input is change content, which is immutable, so its output can be recomputed. `conflict.Analyzer`'s input includes the in-flight set — mutable, overwritten, and recorded nowhere but in the decision record itself. Recovering that record by re-running the incumbent would require the record. So decision capture is disposable exactly where its input is durable, and nowhere else.

Either way it buys an integrity check. If the incumbent stops reproducing its own recorded output on an input that *is* durable, that entry is stale and every number derived from it is void — whatever the candidate does.

## The granularity rule

A key made from the call itself does not survive a candidate that makes different calls.

> Key each captured value on what the value is a function of, after subtracting anything a candidate's decisions could change — a batch ID, a path ID, an attempt number, an in-flight set. What survives is the key. If nothing survives, the value is decision capture, not resolution.

Call the subtracted part **constructed** identity: it exists only because of a decision the system made, and a candidate could decide differently.

Note what that excludes. A request ID is minted by the gateway's counter but is not constructed in this sense — requests arrive from outside, so the same ones exist whatever a candidate decides, and a request ID still resolves in a replay that batches differently. A batch ID is constructed, because the batching decision produces it.

### Applied to each seam

| Seam | Value is a function of | Struck from the key | Corpus key | One call yields |
|---|---|---|---|---|
| [`DetailedForBatch`](../../../submitqueue/core/changeset/changeset.go) | each claimed URI's content, as recorded by its owning request | the batch grouping | `(queue, URI, request ID)` — the change store's primary key | N entries |
| [`ChangesForBatch`](../../../submitqueue/core/changeset/changeset.go) | which change URIs each request carries | the batch grouping | `(queue, request ID)` | N entries |
| change provider | a PR's files at its head commit | the request wrapper | `(queue, URI, request ID)` | N entries |
| build duration | the set of changes built | speculation path, attempt, base | content hash of the change set | 1 entry |
| [`heuristic`](../../../submitqueue/extension/scorer/heuristic)'s `Score` | per-change content, reduced | the batch grouping, if the reduction is pure | `(queue, URI, request ID)` per change | N entries |
| [`conflict.Analyzer.Analyze`](../../../submitqueue/extension/conflict/conflict.go) | candidate content × in-flight content | nothing survives the subtraction | `(queue, batch ID, occurrence)` — constructed, so usable for storage but not for lookup across variants | 1 entry |
| `MergeRequest` / `MergeResult` | the work Runway did on a given tree | none — already self-contained | the payload's `id`, Runway's client-owned correlation id | 1 entry |

Every key leads with the queue, matching the storage convention that keeps tables shardable by queue.

Two rows carry a caveat. **Build duration** drops its base on purpose: the change set determines build time in practice, so a per-content-hash average stands in for an exact function. That shortcut is not extended to pass/fail, where an unbuilt combination has no safe substitute and real CI is the only option. **`heuristic`'s `Score`** only reaches per-change granularity if its batch-level reduction is pure. The extension contract's move of that reduction inside the scorer makes it so today; if it ever depends on the batch as a whole, the row falls back to decision capture.

### Why `Analyze`'s key does not survive

Its answer is a function of the candidate batch's changed files crossed with every in-flight batch's. Subtract the in-flight set — swap the analyzer and the queue drains differently, so a different set is live at any moment — and subtract the batch ID, and nothing invariant is left. Compare `DetailedForBatch`, where striking the batch grouping still leaves `(queue, URI, request ID)`.

**Nothing is lost: the in-flight set is still recorded, in the value.** The record says "given this exact call, the incumbent answered X" — usable for comparing a candidate on an identical call, useless as a lookup across variants. Neither consumer needs the lookup. Whole-workflow replay builds its own in-flight set as it runs; extension replay feeds the candidate the recorded call and diffs against the recorded answer.

### "In flight" means two different things

Take a batch in state `Cancelling`. [`dependencyanalysis`](../../../submitqueue/orchestrator/controller/dependencyanalysis) asks *what must a new batch serialize behind?* and **excludes** it, reading `DependencyBatchStates()` — `Created`, `Speculating`, `Merging`. The `speculate` controller asks *what is consuming build budget?* and **includes** it, reading `ActiveBatchStates()` — those three plus `Cancelling`, since its builds hold CI slots until they stop.

Both are right; they answer different questions. So the same batch legitimately appears in one captured in-flight set and not another taken moments apart. The record therefore names which query produced it, in the captured value. Without that field, the difference looks like a dropped record.

## Where capture attaches

Four attachment points, each chosen by what makes that surface capturable:

```
                                                      capture attaches
  ──────────────────────────────────────────────────────────────────────────
  gateway:Land                                   ✗   at-most-once; see ④
       │
       ▼
  start                                          ④   request arrival
       │
       ▼
  validate ───── MergeRequest ─────►┐            ①   change provider (resolution)
       │                            │ runway
  mergeconflictsignal ◄── Result ───┘            ②   both payloads, as they cross
       │
       ▼
  batch                                          —   mints a bare row; no decision extension
       │
       ▼
  dependencyanalysis                             ①   resolver reads   (resolution)
       │                                         ①   analyzer verdict (decision)
       ▼
  speculate  ◄──────────────────┐                ③   the run's snapshot, at read
       │                        │                ①   scorer, via the generator
       ├───────────┐            │
       ▼           ▼            │
     build       merge          │
       │           │            │
       ▼           ▼            │
  buildsignal   [runway]        │                ④   CI verdict + duration
       │           │            │                ②   merge payloads
       └───────────┴────────────┘
```

### ① Extension seams — decorate the wiring

Every decision/action extension is built by a `Factory` from injected dependencies rather than reaching for them directly; `pathoverlap.New(cfg, resolver, key)` is the reference shape ([extension-contract.md](extension-contract.md)). That gives one decorator point per capture kind: resolution wraps the **injected dependency** (a recording `changeset.Resolver`), decision wraps the **`Factory`** (a recording `conflict.Analyzer` around the real one).

Both live in the wiring layer beside the per-queue routing already there, per the repository rule that `Factory` implementations belong in wiring. No extension is modified or aware. **A new extension is captured automatically**, provided it resolves through injected dependencies instead of reaching outside them.

For the conflict seam this is inside [`dependencyanalysis`](../../../submitqueue/orchestrator/controller/dependencyanalysis), not the `batch` controller: `batch` mints a bare row with an empty dependency list and hands it on, and `resolveDependencies` reads the in-flight set, calls the analyzer, and reduces the result. Both halves of the capture happen in that one function.

The scorer is reached indirectly. No controller calls it; `bestfirst.New(sc)` holds it and `speculate` reaches it through the generator, so its capture attaches at the same injected-dependency seam even though the call site is two layers down.

**Which level to attach at is a wiring fact, not an interface one.** Extensions nest — `standard.New(cfg, gen, alloc)` composes a Speculator from a [generator](../../../submitqueue/extension/speculation/generator) and an [allocator](../../../submitqueue/extension/speculation/allocator), the generator holds the scorer, and [`composite`](../../../submitqueue/extension/scorer/composite) adds a config-defined tree of child scorers below that. Attach at every level meant to be swapped independently: wrap only the outer one, and a candidate that swaps a leaf replays against a record the old leaf produced.

### ② Cross-service boundaries — capture the payload

`validate` → Runway and `merge` → Runway publish full payloads, not IDs, because Runway cannot read the orchestrator's storage. Runway could not resolve a batch ID or a path ID either, so these payloads never carry one. They already satisfy the granularity rule as published. Capture is: subscribe to the crossing topic, store the payload as sent.

That covers Runway's `Merger` with no capture code inside Runway at all. Same reasoning for `start`, `buildsignal` and `log`, which [workflow.md](workflow.md) documents as carrying full payloads because there is no row to fetch yet.

### ③ The `speculate` snapshot — serialize what exists

[`snapshot.go`](../../../submitqueue/orchestrator/controller/speculate/snapshot.go) assembles one run's complete working state in a single consistent pass before `finalize` or `ask` touch it — every in-flight batch, each head's path set, the finalized batches still named as dependencies. It is several store reads, not one query, but the run never re-reads, so two decisions in a run cannot disagree about the world. Serialize the assembled snapshot; no decorator needed.

It is decision capture: every key inside is constructed, so it reproduces one controller invocation exactly and says nothing about a different batching policy.

### ④ External-outcome edges — record or fake

CI verdicts are captured where `buildsignal` observes them, or supplied by a fake keyed on content hash. Merge results are covered by ②.

Request arrivals are captured at `start`, not at the gateway's RPC entry, because the hook framework documents RPC-caused transitions as at-most-once. `start` is the first point the durability guarantee holds, at the cost of missing anything rejected before it.

### What these four do not cover

They cover every extension seam and every storage-boundary crossing, not the whole pipeline:

| Not covered | Why it matters | What would gate covering it |
|---|---|---|
| Controller logic outside `speculate` — `conclude`'s state mapping, the signal controllers' correlation | Replayable in principle by ③'s mechanism, but no snapshot is assembled for these controllers today | Policy moving into one of them. They hold no extension today — only `storage.Factory` — and whole-workflow replay runs them for real, so this is a trigger rather than a date |
| DLQ reconcilers | They drive entities terminal, so they are observable behaviour on failure paths — where a candidate is most likely to differ | Whole-workflow replay's failure model, and a count of how often a real capture window contains a DLQ event |
| The `cancel` controller | A cancellation in a captured trace replays wrong without it: the batch goes on speculating instead of cancelling | Nothing structural. Its whole input is a `CancelRequest` arriving on the cancel topic, so it is ④'s mechanism and one more subscription; deferred to keep the first phase minimal, and needed by whole-workflow replay |
| Joining the orchestrator and Runway streams | Each domain publishes to its own topic, so one request's records arrive split. The correlation ids exist; the joining is not designed here | Splits in two: pairing a `MergeRequest` with its `MergeResult` is needed as soon as a Runway extension is replayed, since that pair *is* the input and expected output; full causal ordering across both streams is whole-workflow |

## The record shape

Four attachment points, one row shape:

| Field | Holds |
|---|---|
| `key` | per the granularity rule — invariant identity, constructed identity, or a correlation id |
| `occurrence` | an ordinal, for keys that recur; absent where the key is content-addressed |
| `value` | what was read, returned, or received, plus any qualifier the key omits — such as which in-flight population a set came from |
| `provenance` | implementation name and version, queue configuration, the commit that produced it |

**`occurrence` applies to constructed keys only.** A resolution key like `(queue, URI, request ID)` addresses immutable content: repeated reads produce byte-identical records, so they collapse to one row and must not be given distinct ordinals. A constructed key recurs with *different* state — the pipeline has two cycles, `speculate → build → buildsignal → speculate` and `merge → runway → mergesignal → speculate`, so a batch visits `speculate` several times — and without an ordinal those visits collapse under one key, destroying the sequence a controller replay needs.

Where the subject is a versioned entity, the [hook envelope](../hook-framework.md)'s `version` field supplies the ordinal directly. It does not generalise: a content-hash key has no entity behind it and needs no ordinal, and a resolution key needs none by the argument above.

One shared row shape beats a schema per surface, which would make every new capture point a migration.

What the corpus is stored in, how long records are kept, and whether high-volume seams are sampled are all unsettled. All three want a volume figure first, measured against real traffic.

### Values, never references

A tempting optimization: `change` rows are immutable, so store the key and let replay read the real table. **Rejected — the corpus stores the value in every case.**

The saving is smaller than it looks. Copying looks like it would multiply reads, since `pathoverlap` calls `DetailedForBatch` once per in-flight batch on every analysis. But those reads produce identical keys and identical content, so the writes collapse: **one row per change, not one per read.**

What copying buys:

- **No live dependency on production storage at replay.** A shadow environment runs from the corpus alone, instead of reaching back into the production `change` table.
- **Immunity to a store's mutability changing.** Referencing is correct only while `ChangeStore` has no update path; add one and every reference silently returns a different answer than the call saw.
- **One code path.** The decorator need not know which table it read, and no reviewer must verify a store is still immutable before trusting a capture.

Mutability still matters, but only for *how* a value reaches the corpus, which is a [delivery](#delivery) question.

## A worked example

Request `#1204` touches `service/pricing/config.go` and is batched as `go-monorepo/batch/2`. Request `#1201`, in flight as `go-monorepo/batch/1`, touches `service/pricing/handler.go`. The queue runs `pathoverlap` with `ByFile`.

**In production, capture records:**

```
① resolution   (go-monorepo, github://…/pull/1204/a1b2…, go-monorepo/4)
                 → { files: ["service/pricing/config.go"], +8/−3, author: Y }

① decision     (go-monorepo, go-monorepo/batch/2, occurrence 1)
                 → { candidate: { id: go-monorepo/batch/2, contains: [go-monorepo/4], deps: [] },
                     inFlight:  [ { id: go-monorepo/batch/1, contains: [go-monorepo/1],
                                    deps: [], state: speculating } ],
                     population: "DependencyBatchStates",
                     conflicts: [] }     ← stored as []Conflict, not flattened to []string

④ external     content-hash({#1204})
                 → { status: "passed", duration_ms: 512000 }
```

The decision record preserves what storage does not: `dependencyanalysis` reduces `[]Conflict` to a bare `[]string`, so conflict type and duplicates are lost in `batch.Dependencies`.

**Replaying with `ByDirectory`**, the candidate resolves both changes to `service/pricing` and reports a conflict where production reported none. `go-monorepo/batch/2` now depends on `go-monorepo/batch/1`, speculates two paths instead of one, and cannot merge until the first lands. The keying is what makes that work:

- **Resolution** is keyed `(queue, URI, request ID)` — no batching decision in the key, so it resolves unchanged for a candidate that groups differently.
- **Decision** does *not* match: the candidate's call has a different in-flight set, so no recorded answer exists. Expected — comparison happens in aggregate, not call-for-call.
- **External outcome** still resolves, because build duration is keyed on the change set's content hash, `{#1204}` either way, not on the path it was built along.

## Delivery

### One event per delivery, not one per read

A decorator produces records at moments that are not lifecycle transitions — a `changeset.Resolver` read happens mid-controller, several times per invocation. Any transport built around transitions needs a bridge.

The bridge is a **collector scoped to the delivery**: decorators append to a buffer on the request context, and the controller flushes it once, at the end, as a single record set. Publishing per read would instead multiply volume by the repeated reads the keys are designed to collapse, fire events for something that is not a transition, and break atomicity — a delivery failing partway would leave some reads recorded and the rest not.

Flushing at end-of-delivery also puts the capture write beside the state write, so capture inherits that delivery's crash semantics instead of needing its own. For a corpus reader, the unit that arrives is one delivery's records: a delivery contributed all of them or none, which makes a gap diagnosable.

### Choosing a transport

Four requirements narrow the field:

1. **Off the hot path.** No added latency, and request success must not depend on capture success.
2. **Durable.** A missing record fails a replay rather than degrading it.
3. **Ordered enough to carry an occurrence.**
4. **Able to carry a value.**

| Ruled out | Why |
|---|---|
| Synchronous write from the controller | Fails 1 on both counts. Already [rejected](#rejected). |
| In-process buffer flushed asynchronously | Meets 1, fails 2 — a crash silently drops whatever has not flushed. |
| Structured logs into the log pipeline | Genuinely off the hot path, but log pipelines sample and shed under load by design. Fails 2; an untyped schema makes 3 and 4 awkward. |

Two candidates survive, and they run on **the same infrastructure**: [hooks](../../../platform/extension/hook) execute behind a durable queue rather than inline in the pipeline, and `TopicKeyHook` resolves through the same `consumer.TopicRegistry` as every pipeline stage. The choice is not transport, it is which contract the payload obeys.

| | **A — [hook framework](../hook-framework.md)** | **B — dedicated capture topic** |
|---|---|---|
| Envelope | inherited, with a derived id | ours to define, under `submitqueue/core/messagequeue` |
| Requirement 4 | needs [the payload question](#the-payload-question) resolved | satisfied by construction |
| Added contract | none | one, over ground hooks already cover |
| Risk | renegotiating a contract this design does not own | ours to get wrong |

**Neither saves emissions.** The appealing form of reuse — persist the events the pipeline already publishes and add nothing — reaches lifecycle transitions only. Resolution capture is about reads, and no event exists for a `changeset.Resolver` read. New publishes are needed either way, which removes the strongest argument for A.

**One thing is worth taking from the hook framework whichever is chosen: its derived event id.** The same transition mints the same id, so a redelivery dedupes with no publisher-side outbox, and the unversioned form names the causing message plus an ordinal instead — which is exactly the shape of the per-delivery flush above. Idempotent capture under at-least-once delivery is what makes a corpus trustworthy.

### The payload question

The framework's position is that hooks resolve entities from stores rather than carrying snapshots, since a snapshot goes stale on redelivery and competes with the store. The corpus always stores values, but that does not force every payload to carry one.

**From an immutable source the payload may carry a reference**, resolved by the corpus writer before the value lands. For `change` details it carries `(queue, uri, request_id)`; redelivery re-resolves to the same answer because the row cannot change, so the framework's rule holds as written and the corpus is still self-contained.

**From a mutable source the payload must carry the value.** The `speculate` snapshot is the case: `batch` and `speculation_path_set` are overwritten, so a reference resolved later returns current state, not what the run read. The capture event is the only durable record of it.

That second case is what option A turns on, and it needs a ruling before anything is built on it. It is narrow — point-in-time state of overwritten entities, not a general licence — and the framework's own requirement that a payload carry facts that are *"their only durable record"* appears to admit it, but it has never been written as an exception and should not be assumed. **If the ruling goes the other way, option A is closed.**

## Scoping a replay

A replay must know which records to load before starting. Get it wrong and a run dies partway through on a record never captured — and a mid-run failure on missing data looks exactly like a defect in the candidate.

Choosing by **time window** fails: a batch near the edge depends on one outside it, that dependency has no record, the run breaks. The instinct is then a transitive closure over the dependency graph, which looks unbounded because `batch.Dependencies` chains backward with no obvious floor. **No mode needs that walk**, though each still needs more than one record.

**Controller replay — the snapshot, plus resolution for what the run scores.** A `speculate` snapshot holds every in-flight batch, each head's path set, and the finalized batches still named as dependencies, which [`read`](../../../submitqueue/orchestrator/controller/speculate/run.go) hydrates explicitly. A batch that already finished contributes only its `State`, through `snapshot.batchState`, so nothing outside the snapshot is needed for it. But the run also asks the Speculator to rank, and the generator scores each *unresolved* dependency, which resolves its own change content. So the scope is the snapshot plus one resolution record per unresolved dependency's changes. Bounded by the snapshot's own contents, and not a graph walk.

**Extension replay — two hops.** Replaying `Analyze(batch-D, [A, B, C])` needs the changed files of all four. The decision record carries the in-flight set, and each batch in it carries `Contains`, which names request IDs. A request carries several change URIs, so a request ID alone does not give a resolution key: the `ChangesForBatch` record, keyed `(queue, request ID)`, supplies the URI list, and the URIs then give the resolution keys. Decision → `Contains` → `ChangesForBatch` → resolution. Fixed depth, and it does not recurse: the analyzer never looks at a dependency of a dependency.

**Whole-workflow replay — the window plus the warm-up set.** Running forward **re-derives the dependency graph** by calling the analyzer, so the recorded graph is not an input. It consumes arrivals and change content: requests that arrived during the window, plus requests already in flight when it opens, since a run starting cold at 09:00 would otherwise miss the 08:45 batches still competing for budget.

Those warm-up requests are **restored with their recorded progress**, not re-injected as new. Re-injecting would rebuild work production had already done, which needs build outcomes for compositions the recording cannot supply and distorts the very land-time numbers the mode exists to produce. The warm-up set is one query at an instant, so its size is queue depth — the honest bound, and not always small, since a wedged queue makes it large for any window inside it.

One thing cannot be pre-loaded: external outcomes for compositions the simulation invents. That is the build-model problem named in [Replay modes](#replay-modes), and no scoping rule solves it.

**Preflight, not discovery mid-run.** Resolve and verify the scope before starting: walk the selected set, assert every referenced record is present, fail immediately with a list of what is missing. This is complete for extension and controller replay, where the whole input set is known up front. For whole-workflow replay it verifies the pre-loadable part only — arrivals, change content, warm-up state — and the build model covers what the run invents as it goes. Either way it converts the failure from *forty minutes in, looking like a candidate bug* into *before anything ran, with a name for what is absent*.

## Rejected

| Rejected | Why |
|---|---|
| Recording `(call arguments) → result` at each seam | The natural decorator implementation, and correct for pure replay-and-compare. Fails the moment a candidate constructs calls production never made, because the key *is* the constructed argument. |
| Storing references in the corpus instead of values, for immutable sources | Saves less than it appears, since identical keys already collapse repeated reads, and costs a live production dependency at replay plus silent breakage if the store gains an update path. Distinct from the hook *payload*, which may carry a reference — see [the payload question](#the-payload-question). |
| Requiring each extension to call a `Record()` method | Works until an author who does not know the convention ships an uninstrumented extension, surfacing later as an unexplained replay failure rather than a build-time signal. |
| Selecting a replay's records by time window alone | A dependency outside the window has no entry and the run fails mid-replay. A window still picks the seed for whole-workflow replay; completing it with the warm-up set and preflighting is what the failure mode needs. |
| A transitive closure over the dependency graph | Unnecessary: controller replay is bounded by one snapshot's contents, extension replay is a fixed two hops, and whole-workflow replay re-derives the graph. An unbounded walk would buy nothing any mode uses. |
| Capturing only the orchestrator | Misses everything Runway does, which here includes the merge itself. |
| Capturing message-queue internals | The queue is infrastructure a replay substitutes, not a signal source. |
| A synchronous capture write from the controller | Adds a database write to every request's hot path and couples request success to capture success. |
| Change-data-capture off the storage write stream | Needs no application change, and is decisively wrong: it sees only writes, and resolution capture is about reads. It would also record the reduced `[]string` rather than the analyzer's `[]Conflict` — the exact loss this design prevents. |
| Capturing at every granularity available | Two stored copies of one fact can disagree after a refactor with nothing to say which is authoritative. Store what the rule says is irreducible; derive the rest. |
| A schema per capture surface | Multiplies versioning and migration burden once the four surfaces are shown to share one row shape. |
