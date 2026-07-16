# Queue-Scoped Speculation

This RFC revisits the *execution model* of [speculation](speculation.md): how and when speculation decisions are computed, what is persisted, and how the seams compose. It keeps the conceptual model intact — paths as Base/Head futures, scores as live predictions, limits as signal-driven policies, a single writer for bets and the book — and proposes replacing the batch-keyed `speculate` stage and its materialized, persisted speculation tree with a **queue-scoped speculation pass** over a **bet book**: candidates are derived on demand and never stored; only the bets actually placed (paths dispatched to build) have records; selection and prioritization merge into one queue-wide admission decision.

An adversarial design review against the current implementation shaped this document; the rules it forced — the attempt counter, the executor-owned execution record, the book invariant, the write-order discipline — are design, not commentary. If adopted, this RFC supersedes the execution-model and persistence sections of speculation.md; the problem statement and the Base/Head path model there remain authoritative. One deliberate narrowing: speculation.md's **optimistic merge finalization is dropped here** — the pass advances a batch to merge only on actually-merged reality, and the optimistic hand-off is deferred to its own design (see the deferral note under merge finalization).

## Problem: what the materialized tree costs

The current design materializes a batch's full speculation tree up front (the exhaustive enumerator emits one path per subset of the active dependency list — 2^N paths for N dependencies), persists it as a row, and re-walks all of it on every event: reconcile every path's status, rescore every path, select over every path. Three distinct costs hide in that shape:

1. **Materialization.** Every path is created and persisted even though the selector will only ever act on a handful. Each +1 on the dependency limit doubles the tree, the row, and every subsequent pass. The dependency limit exists largely to protect this materialization — the architecture caps how far the limit policy can ever scale, and the limit policy cannot see that ceiling.
2. **Per-event full passes — which are already half queue-wide.** Every build signal or landing dependency triggers a respeculate per affected batch, and each respeculate is O(tree). One dependency landing fans out to every dependent batch via explicit fanout publishes, each doing a full-tree pass. And each of those passes then publishes a prioritize round that loads *every* speculating batch's tree queue-wide — so a single terminal event already costs K per-batch passes *plus* K queue-wide tree sweeps. The system already pays queue-scoped costs on every event; it just pays them K times over without getting a queue-scoped vantage in return.
3. **Contract coupling.** The seams' interchange currency *is* the materialized tree: the enumerator returns it, the scorer takes it, the selector takes it. Even an implementation that wanted to be lazy cannot be — the interfaces force the full structure through every boundary.

Four structural facts about the problem go unexploited by that shape:

- **The tree is derivable, not data.** The candidate space over N ordered dependencies is fully determined by the dependency list; a path is a full truth-assignment (which dependencies are bet to land, the rest bet to be ruled out). Candidates carry no information that is not recomputable in microseconds from live state.
- **Events only shrink the space.** When a dependency resolves, the live future space collapses to the space over the remaining unresolved dependencies: a landing kills every path that excluded it, a failure kills every path that included it. Reconciliation is a filter, not a recompute — yet today it is implemented as stored-row sweeps.
- **Ordered generation needs no enumeration.** For any score that composes monotonically from per-dependency beliefs (the product form is the canonical case), the best path is computable directly and the k-best paths can be generated lazily, best-first, without touching the full space. The ladder selector already *walks* paths in exactly this order — the design wants lazy generation; the enumerator just materializes the whole rung space underneath it.
- **The valid space is smaller than 2^N.** Subsets are the right space only when the dependencies are mutually independent. When the dependency DAG has internal edges, paths that bet on a batch while betting against one of *its* prerequisites are incoherent (the survivor rebases and its content changes, invalidating any build that bet on the old content). The honest space is the down-closed subsets of the DAG — for a chain, N+1 prefixes, not 2^N.

## Vocabulary

Terms from speculation.md (batch, dependency DAG, path, Base/Head, score) keep their meanings. New terms:

| Term | Meaning |
|---|---|
| **Speculation pass** | One queue-scoped evaluation: snapshot → beliefs → generate → admit → enact. Pure function of ground truth. |
| **Dirty signal** | A queue-keyed message meaning "something changed in this queue; run a pass". Carries no decision content. |
| **Snapshot** | The pass-local, in-memory view the controller assembles each pass — live batches, the book, the bets it lists, execution records, builds — and hands to the Speculator. Never persisted; rebuilt from ground truth every pass. |
| **Path** | speculation.md's path, carried forward as the unit of speculation: Head plus the ordered Base bet to land, everything unresolved outside the Base bet to be ruled out — a full truth-assignment over the dependencies. Its content hash is the path ID. |
| **Virtual candidate** | A path that exists only by derivation during a pass. Never persisted. |
| **Bet** | The persisted record of a path dispatched to build, keyed by the path ID; carries status and attempt count. Written only by the pass. |
| **Attempt** | The execution epoch of a bet. Identity is the path; the attempt distinguishes successive builds of it. |
| **Execution record** | The executor-owned record linking (path ID, attempt) to the CI build it triggered — the existing SpeculationPathBuild mapping carried forward, with the attempt added. The pass reads it; the executor writes it. |
| **Book** | The single per-queue *persisted* record listing active bets — the index for "bets of this queue / this head" and the membership linearization point. An ingredient of the snapshot, not the snapshot: it tells the controller which bets to read. |
| **Speculator** | The controller-facing decision seam: queue snapshot in, plan out. Composed by default from Belief, Generator, and Admitter. |
| **Belief** | The evidence model: per-batch pass probability plus resolution state, derived from live batch/build state. |
| **Generator** | The per-head lazy source of candidate paths, yielded best-first on demand. |
| **Admitter** | The queue-wide policy that keeps, cancels, and admits bets under the build budget. |
| **Budget** | The queue's concurrent-build bound — the prioritization limit of speculation.md, applied at plan time. |
| **Depth bound** | The per-connected-set cap on unresolved dependencies a head may be planned over — the dependency limit's successor, consulted inside the Speculator. |
| **Plan** | The Speculator's output — everything enact will write and publish: status updates, paths to dispatch, bets to cancel, batch verdicts. |
| **Verdict** | A rule-bound batch outcome in the plan — finalize, fail, or conclude — that no implementation may withhold. |
| **Valuation** | The probability a path matches the future, composed from per-dependency beliefs — priced only by the Belief view, on demand. |
| **Refutation** | A resolved fact contradicting a path. Refuted bets must cancel; refuted paths are never admitted and never resurrect. |
| **Coverage** | Path equivalence modulo resolved dependencies: a live, unrefuted bet covers the collapsed path formed by dropping its landed base members. |
| **Resurrection** | Re-dispatching a still-viable path whose bet is terminal — same path ID, attempt incremented. |

## The speculation pass

Any event that can change speculation reality — a build result, a batch scored into the queue, a merge completing, a cancel — publishes a dirty signal keyed by QueueID. A self-arming delayed tick provides the liveness backstop. The pass consumes dirty signals and evaluates the whole queue in one frame:

```
 buildsignal ─┐
 new batch ───┤                      ┌──────────── speculation pass (per queue) ─────────────┐
 mergesignal ─┼──dirty(queue)──▶     │ 1. snapshot   live batches (by-queue query), dep DAG, │
 cancel ──────┤  (partition key      │               book, bets, execution records           │
 DLQ recon ───┘   = QueueID)         │ 2. beliefs    per-batch p + resolution state          │
                                     │ 3. generate   per-head best-first candidates (lazy)   │
                                     │ 4. admit      keep / cancel / dispatch under budget   │
                                     │ 5. enact      bet CASes, book CAS, build & cancel     │
                                     │               dispatches, heal republishes, merge     │
                                     │               finalization, batch state CASes,        │
                                     │               re-arm tick                             │
                                     └───────────────────────────────────────────────────────┘
```

Three properties are load-bearing, and each maps onto machinery the repo already has:

**The pass is a pure function of ground truth.** It reads batches, builds, the book, bets, and execution records, and computes the plan from scratch every time. No pass depends on a previous pass having run or having observed any particular event. Dirty signals are therefore safe to lose, duplicate, and reorder: at-least-once delivery plus idempotent recompute converges. This is the posture the pipeline already takes everywhere else, applied at queue granularity.

**One pass in flight per queue — normally.** The consumer framework already processes deliveries serially per partition key and concurrently across partition keys; QueueID as the partition key gives per-queue serialization and cross-queue parallelism with existing mechanics. But serialization is *best-effort*: a pass that outlives the message visibility timeout gets redelivered concurrently, so the design must survive two concurrent passes — and does, via CAS guards and the convergence rules below, with `VisibilityTimeoutMs` configured above the worst-case pass duration to keep the case rare.

**The pass is the single writer of bets and the book.** Bet records and the book are written only by the pass. The one deliberate boundary: the **execution record is executor-owned** — the build executor mints the CI build reference, so it writes the (path, attempt) → build linkage, exactly as the build controller owns the path→build mapping today. Splitting that record out is what keeps the bet record single-writer; folding the build ref into the bet would put the pass and the executor on the same row. Every other stage — buildsignal, score, mergesignal, cancel, the DLQ reconcilers — updates its own entity (build, batch, request) and publishes a dirty signal.

### What the pass absorbs

Today's speculate controller is already more than select-and-dispatch, and everything it does beyond that must be explicitly re-homed, or it is homeless. The pass owns all of it:

- **Merge finalization — strict, not optimistic.** A batch is mergeable when it has a passed bet whose path matches *actually-merged* reality: every base member has landed (its merge observed complete via mergesignal) and every excluded dependency is terminally ruled out (Failed or Cancelled — never Cancelling, because a cancelling batch can still land). The pass computes this from the same snapshot it plans with and publishes to merge — so a chain advances one merge signal at a time, each link moving at the pass that observes its predecessor landed. Deliberately narrower than speculation.md's optimistic finalization — see the deferral note below.
- **Merging supervision.** The pass re-arms the merge hand-off for a batch sitting in Merging (a lost trigger heals at the next pass or tick); merge results themselves flow back through mergesignal, which writes the terminal batch state exactly as today. Under strict finalization the supervision duty shrinks to that: a batch enters Merging only when its path is composed entirely of terminal facts, so the refuted-while-Merging failure mode — a parked batch whose optimistic assumptions collapse — cannot arise here and moves to the deferred optimistic-merge design. Supervision is stated explicitly because leaving it unassigned strands Merging batches forever.
- **No-viable-path failure.** A head whose every path is refuted has no future; the pass fails it.
- **Cancel supervision.** The cancel stage CASes the batch to Cancelling and signals dirty; the pass cancels the batch's bets through the normal choreography, and once every bet is terminal it writes the terminal Cancelling → Cancelled batch transition and publishes the conclusion. Batch-terminal writes stay batch-entity writes — the single-writer claim is about bets and the book, and the pass performing batch CASes is the same role speculate plays today.
- **Dependent fanout disappears.** Today an advancing batch publishes respeculate messages to each dependent, and each of those triggers a queue-wide prioritize round. In this design an advance is one dirty signal; the next pass sees the new reality for every head at once. The fanout code path — and its partial-failure modes — is deleted, not moved.

**Deliberately not absorbed: optimistic merge finalization.** speculation.md pipelines merges down a chain by publishing a passed path for merge as soon as its base is itself published for merge, with the merge stage re-verifying and parking on the not-yet-confirmed members. This RFC removes that coupling: the pass hands a batch to merge only when its predecessors have actually merged, one merge signal at a time. The cost is real and accepted — chained hand-offs serialize on merge round-trips instead of pipelining, roughly one merge latency per chain link, which is small against multi-minute builds. The reason is separation of concerns: optimistic hand-off entangles the speculation pass with merge-executor semantics — confirmed-versus-possible readiness, park-and-poll at the merge stage, unwinding a refuted optimistic bet at the merge boundary — and those concerns deserve their own design, with their own failure analysis, as an additive extension on top of this one. It is recorded in open questions so the deferral is a decision, not an omission.

### Triggers, the tick, and coalescing honesty

**Every producer of dirty signals must mint an occurrence-unique message ID**, because the queue's publish dedup compares against retained rows *including already-consumed ones* — a reused ID silently swallows the wakeup. The producers and their ID material: buildsignal on terminal build results (build ID + terminal status), score on batch entry (batch ID + state version), mergesignal on merge results (batch ID + result), cancel on cancel acceptance (batch ID + state version), batch-level DLQ reconcilers after driving a batch terminal (batch ID + reconciled state), the merge stage on an anomalous hand-off (batch ID + observed state), the speculate DLQ's own re-arm (dead-lettered message ID), and the tick (queue ID + timestamp). This enumeration is normative: a producer omitted here is a class of state change the pass only discovers at the next tick.

**The tick is self-arming, not a scheduler.** Nothing in the codebase runs on a schedule, and nothing new needs to: the pass re-arms its own wakeup by publishing a delayed dirty signal (the queue's existing delayed-publish capability, the delayed-publish pattern the merge stage already uses) whenever the queue has live batches. A queue with nothing in flight stops ticking; the score stage's dirty signal bootstraps it back. A crash before re-arming is covered by redelivery of the in-flight dirty message. The tick heals lost signals and bounds the staleness of anything that changes without a producer — notably **dynamic limit values: limits are pull-only policies with no change feed, so a moved limit takes effect at the next event or tick**, and this RFC accepts that latency rather than inventing a change feed.

**Events do not coalesce, and the design does not pretend otherwise.** Unique message IDs mean every event is delivered, and the consumer framework hands controllers one delivery at a time with no drain hook — so a burst of ten build signals is ten passes. The design accepts this: passes serialize per queue, and a pass that finds nothing to change enacts nothing but its tick re-arm, so redundant passes cost reads and one delayed publish, not state writes. If pass frequency ever matters, a drain hook in the speculate consumer is a contained optimization that changes nothing semantically, because any suffix of passes computes the same result from the same ground truth.

### Pipeline changes

| Stage | Today | Queue-scoped |
|---|---|---|
| `speculate` | Batch-keyed; full tree reconcile/rescore/select per event; dependent fanout; merge finalization; cancel sweep; merging supervision | Rebuilt queue-keyed — the same `speculate` stage, now consuming dirty signals; one pass per event covers all heads and all absorbed duties; no fanout |
| `prioritize` | Separate queue-wide stage ranking selected paths under the build budget, one round per speculate pass | Deleted; admission happens inside the pass, which sees candidates and budget in one frame |
| `build` | Consumes BatchID, walks the tree for prioritized paths, triggers/cancels via runner, owns the path→build mapping | Consumes per-bet dispatch and cancel messages carrying (path ID, attempt); point-reads the bet and refuses to trigger unless it is dispatched at that attempt; owns the execution record keyed by (path ID, attempt) |
| `buildsignal` | Correlates result to path, publishes respeculate per batch | Correlates result to execution record, updates build state, publishes dirty(queue) |
| `score` | Publishes BatchID to speculate | Publishes dirty(queue) |
| `merge` | Re-verifies confirmed/possible readiness at hand-off, parks on possible members, refuted branch nudges speculate by batch ID | Re-verifies terminal reality as defense-in-depth — never parks, because strict finalization hands off only fully-terminal paths; any anomaly publishes dirty(queue) |
| `mergesignal` | Publishes conclude + respeculate fanout | Publishes conclude + dirty(queue) |
| `cancel` | CAS batch to Cancelling, publish to speculate | CAS batch to Cancelling, publish dirty(queue) |
| batch DLQs | Drive batch/request terminal, stop | Additionally publish dirty(queue), so bet cleanup follows the reconciled state without waiting for the tick |

The speculate topic's own DLQ follows the re-arm shape the prioritize stage's DLQ already implements: a dead-lettered dirty signal is answered by publishing a fresh dirty signal (new occurrence ID), because the remedy for a failed pass is another pass — there is no single entity to reconcile. A queue that persistently dead-letters its passes is an alerting condition, not a reconcilable one.

## Persistence: bets, executions, and the book

The persistence model inverts: **persist bets placed, not possibilities.** Three record kinds replace the per-batch tree row and the path→build mapping.

**The bet record** — created only when the pass admits a path for building, keyed by the path's content-derived ID. It carries the path (head batch ID plus the ordered base batch IDs), its status, the attempt counter, timestamps, and its own version for CAS. Written only by the pass. It does *not* carry the CI build reference — that is the execution record's job, and keeping it out of the bet record is what keeps the bet single-writer.

**The execution record** — today's path→build mapping (SpeculationPathBuild) kept in place and given the attempt — keyed by (path ID, attempt), linking each attempt to the CI build the executor triggered for it. The executor writes it once per attempt; the pass reads it to reconcile build outcomes into bet status, exactly as today's reconcile step does for paths. Its presence is also the dispatch dedup: the executor triggers only if no execution record exists for the (path ID, attempt) it was asked to run, and only if the bet's current status and attempt still call for it.

**The queue book** — one record per queue listing the active bet entries (path ID plus head batch ID) under a CAS version. It is deliberately the *index*: "all in-flight bets for queue Q" and "bets for head H" are one book read plus point-reads by ID — no scan, no secondary index, no join. Active bets are bounded by the budget (tens), so the book stays one small record. Its governing invariant: **the book lists every bet that could have a live execution.** A bet is pruned only after its execution is observed terminal (a failed bet's execution already is, so it prunes at the same enact; only cancelling waits for its build to drain) — pruning earlier would orphan a live CI build with nothing left to enumerate it — and a *passed* bet is retained until its head batch is terminal, because merge finalization and hand-off re-verification read it for as long as the batch is in Merging.

Live batches are *not* book state: the pass reads them through the existing by-queue-and-states batch query — the same read today's prioritize and cancel stages depend on. That query is a query-by-attribute, which the storage contract discourages for new interfaces; this RFC leans on it precisely because it already exists and adds no new contract surface. The book indexes what is genuinely new (bets); batches keep their existing index.

Why candidates are not stored — three reasons, in increasing order of importance:

1. **They carry no information.** The candidate space is fully derivable from live batches, the dependency DAG, and current resolutions. A stored candidate is a cache of a microsecond computation.
2. **Write amplification the store cannot make atomic.** Today one dependency landing means sweeping every dependent batch's tree row to cancel newly-invalid candidates and rescore survivors — wide multi-row updates with no transaction to keep them coherent. With virtual candidates that write class disappears: when a dependency resolves, invalid candidates simply stop being derived. The space collapses with zero writes.
3. **Single source of truth.** A stored candidate set can drift from the space implied by live state, and repairing that drift is reconciliation logic whose only job is defending a cache. Derivation cannot drift.

### Storage integration

All of it lands in the existing storage seam — the storage extension under the SubmitQueue domain (`submitqueue/extension/storage`) — following the same pattern every store does today: an interface per store with a generated mock, an implementation per backend, a getter on the aggregate storage interface, constructed in the wiring layer. The contracts stay the repo's key-value shape — get/put by key, per-record CAS, no scans — which is exactly what keeps a NoSQL backend a drop-in. The delta:

- **New: the bet store and the book store.** Get/put/CAS by their single keys — path ID and queue ID. Updates follow the repo's optimistic-locking rule: the pass computes old and new versions and the store performs a pure conditional write. A create that hits an already-existing bet is not an error to the pass — content-addressed identity makes it an adoption: the record is the same decision, already persisted.
- **Evolved in place: SpeculationPathBuild**, the path→build mapping, keeps its name and role and gains the attempt — keyed by (path ID, attempt) instead of path ID alone. Ownership is unchanged — written by the build stage, read by the pass — and the build entity's reverse link likewise keeps its field and gains the attempt.
- **Unchanged: the batch store**, including the by-queue-and-states listing the snapshot reads, and **the build store** in every other respect.
- **Retired: the speculation tree store**, and with it the persisted tree entities. The bet and book entities join the domain's entity package in their place; nothing replaces the tree row.

Who calls what — the write side is deliberately narrow:

| Store | Written by | Read by |
|---|---|---|
| Book | speculation pass, only | speculation pass (snapshot); merge stage (hand-off re-verification locates the batch's passed bet) |
| Bets | speculation pass, only | speculation pass (snapshot); build stage (executor guard before trigger); merge stage (re-verifies the passed bet's path) |
| Execution records | build stage, only | speculation pass (reconciles outcomes, heals republishes); build stage (dedup check before trigger) |
| Builds | build stage (create on trigger); buildsignal (result) — today's two-phase writers | speculation pass (snapshot); buildsignal (correlate result) |
| Batches | score, cancel, mergesignal, DLQ reconcilers, and the speculation pass (Speculating, Merging, no-viable Failed, cancel quiescence) — CAS-guarded multi-writer, as today | speculation pass (by-queue-and-states snapshot); merge stage (dependency outcomes); everything that reads batch state today |

The single-writer claim is scoped precisely by this table: bets and the book have exactly one writer (the pass), execution records exactly one (the build stage), while builds keep today's two-phase writers and batches keep today's CAS-guarded multi-writer set — those two were already multi-writer before this design and are unchanged by it.

Nothing else in this design touches storage: beliefs and candidates are derived and never persisted, the plan log and explain surface are observability rather than state, and the tick rides the queue's existing delayed publish. Bet-record cleanup is TTL-shaped by construction — pruned terminal bets are unreferenced once out of the book, so a record TTL, with the safety rule below as its floor, is the KV-native mechanism rather than a sweep.

**Garbage collection has a rule, not a vibe:** a bet record and its execution records are deletable only when the head batch is terminal *and* every execution of the bet is terminal. Content-addressed identity makes this load-bearing rather than housekeeping — deleting half a bet's history and then re-deriving the same path would resurrect against a record that lies about its past. Retention beyond the safety rule (TTL, per-queue caps) is operational tuning.

### Path identity: content-addressed, with the attempt counter as a correctness requirement

A path's ID is the hash of its content — queue, head batch ID, the ordered base batch IDs — and the bet wagered on a path is keyed by it. This is the property the crash story stands on:

- **Re-derivation is idempotent.** A pass that crashes mid-enact and re-runs recomputes the same decisions and the same IDs; re-creating a bet record is a no-op put, and an orphaned record is *adopted* by the next pass that derives the same path — the record's identity is the decision.
- **Changed composition is a new path automatically.** Batch IDs are minted per composition; a re-batched change yields new batch IDs, a different hash, and a fresh path. Stale bets' paths stop being derivable, and the next pass cancels them. (`Batch.Version` is deliberately excluded from the hash: it increments on every state write, and including it would churn path identity on every transition.)

The design review broke the naive form of this scheme, and the **attempt counter is the repair — it is required for correctness, not a policy nicety.** Three forces demand it:

1. **Legitimate re-desire.** A bet cancelled for budget reasons while still viable can become the right bet again; a failed build may warrant a retry under a flake policy. Same path, same hash — without attempts, a terminal record permanently retires the path, and in the worst case every path for a head accumulates a cancelled record and the head can never build again: a user-visible failure caused purely by scheduling.
2. **Torn cancels.** A pass that loses the book CAS to a concurrent redelivered pass may already have CASed a bet to cancelling — and cancelling is one-way. The winning plan's desired bet dies anyway, and convergence *requires* re-dispatching the same path. Resurrection is how crash interleavings converge, independent of any policy.
3. **Queue dedup.** The publish path dedups message IDs against retained rows including consumed ones, so a message keyed by the bare path ID could never be legitimately re-sent. Dispatch and cancel messages carry (path ID, attempt) in the payload, and every publish mints an occurrence-unique message ID; idempotency lives at the executor's keyed execution record and status guard, not in queue dedup.

So: identity is the path; the attempt is the execution epoch. Re-admission CASes the terminal record back to dispatched with the attempt incremented; executions, statuses, and terminal outcomes are per-attempt; a stale message for an earlier attempt is rejected by comparison against the record's current attempt. Refuted paths — a base member failed, or a resolution contradicts the path — are never re-admitted: refutation derives from terminal batch states, which do not un-happen.

The attempt lives on the record rather than in the hash deliberately: an epoch inside the hash would make identity depend on history, so a crashed or racing pass could not recompute the ID without reading persisted epoch state — and two racing passes reading different epochs would mint two records for one path. The record is where history belongs; the hash is where identity belongs.

One comparison rule keeps identity honest as the world resolves: **paths compare modulo resolved dependencies.** A bet recorded over base [B1, B2, B3] and a candidate freshly generated over [B2, B3] after B1 landed are the same future — the bet's B1 factor settled in its favor — but they hash differently, because the bet's path ID froze the dependency list at dispatch time. The Speculator therefore matches in-flight bets against desired paths on their projection over the still-unresolved dependencies: a live, unrefuted bet covers the path formed by dropping its landed base members. Without this rule the pass would see the collapsed candidate as unserved, dispatch a duplicate, and cancel the equivalent running bet — churn with no information gain. Identity stays frozen (that is what makes crash re-derivation work); equivalence is computed against the collapsed space each pass.

This revisits speculation.md's rejection of structure-derived IDs. That rejection targeted IDs that *encode* structure and invite parsing; a hash is opaque to every consumer. What content addressing buys — identity that survives independent re-derivation — is precisely what makes transactionless enactment idempotent, and no controller-assigned ID can provide it.

### Write choreography, without transactions

Every write is a single-record CAS or an idempotent keyed message; no step needs two records updated atomically. Four rules govern enactment, each earning its place from a specific failure mode:

**Rule 1 — write order by delta type.** Dispatch: put/CAS the bet, *then* CAS the book to add it, *then* publish. An orphan bet record (crash before the book CAS) is harmless by construction — publish follows a successful book CAS and heals are book-scoped, so no message ever names an unlisted bet — and it is adopted or ignored by later passes; a book must never list a nonexistent bet, hence bet-before-book. Cancel and settle: CAS the bet to cancelling, publish the cancel; only after the execution is observed terminal, CAS the bet to cancelled, *then* CAS the book to prune it. Prune-last is the book invariant in action: the book must keep listing any bet that could still have a live execution.

**Rule 2 — publish only after the book CAS succeeds.** A pass that loses the book CAS aborts its remaining enactments. This makes "a dispatch message for a bet the winning book never listed" impossible. A plan with writes but no membership change still bumps the book version, so the gate applies to every enacting pass; only a pass with nothing to enact skips the CAS. The surviving imperfection is the mirror image: a *stale* pass can win the book CAS and dispatch a bet the newest state would not have chosen — that bet is book-listed, so the next pass sees it and cancels it through the normal choreography. Cost: one wasted CI trigger; never a leak.

**Rule 3 — heal republishes, every pass.** The pass republishes the dispatch for any book-listed, still-wanted bet with no execution record at its current attempt, and republishes the cancel for any cancelling bet whose execution still runs. This closes every crash-between-CAS-and-publish window, and the tick guarantees a healing pass happens. (Republishes mint fresh occurrence-unique message IDs — the executor's keyed record, not queue dedup, provides the idempotency.)

**Rule 4 — the executor guards.** On a dispatch message, the executor point-reads the bet and triggers only if it is dispatched at the message's attempt and no execution record exists for that attempt; on a cancel, cancelling a terminal build is a no-op at the runner. Late or duplicate deliveries buy at most a wasted read.

What the book CAS actually linearizes — stated precisely, because the first draft over-claimed it: **the book CAS linearizes membership, not the pass.** A losing concurrent pass can land absorbing bet-record writes (a cancelling CAS) before its book CAS fails; those torn writes are never reversed — they converge because the path can be re-admitted at the next attempt. Per-queue partitioning makes concurrent passes the exception (visibility-timeout redelivery, lease expiry); the CAS discipline plus resurrection makes them harmless.

Two merge-safety rules complete the choreography, both inherited in spirit from today's terminal-path protections: **passed bets are untouchable** — the pass never cancels a passed bet (it holds no budget, so the Admitter has no reason to; it is a contract rule so the merge stage's read of a passed bet is stable under the single-writer model) — and **hand-off re-verifies from terminal states only** (a Cancelling base member never counts as ruled out). The known lossy race — the pass preemption-cancels a building bet from a stale snapshot while the build concurrently passes — is safe but wasteful: the cancel is moot at the runner, the bet settles cancelled, the passed result is discarded, and liveness recovers by re-admitting the path at the next attempt. An implementation may shrink the waste by re-reading the build row immediately before enacting a preemption cancel; it must *not* add an un-cancel transition (cancelling → passed), which would reintroduce exactly the two-writer ambiguity the design removes.

Two eventual-consistency notes the Admitter treats as invariants rather than anomalies: the snapshot may be stale (worst case the pass cancels something already finished — moot — or dispatches something just refuted — cancelled next pass), and side-effect messages may be arbitrarily delayed (the executor guard absorbs them).

## Lifecycles: the batch and its bets

The batch is the entity the rest of the pipeline sees; bets are speculation-internal machinery in its service. So the batch's journey comes first — annotated with which component writes each transition under this design — and the bet machine follows as the thing that runs inside the batch's Speculating window.

### The batch, and who moves it

```
         score stage        the pass:                 the pass:  strict
         (unchanged)        first plans the head      merge finalization
  created ────▶ scored ────▶ speculating ────────────▶ merging
                                  │                       │
                                  │ the pass:             │ mergesignal:
                                  │ no viable             │ runway's merge result
                                  │ path left             ▼
                                  ▼                 succeeded / failed
                                failed

  cancel stage: CAS to cancelling, from any active state
  the pass:     cancelling ──(once every bet is terminal)──▶ cancelled
```

Every transition is written by the component that owns the fact: the score stage scores, mergesignal records runway's verdict, and the pass writes everything speculation decides — Speculating, Merging, the no-viable-path Failed, and the cancel quiescence. Bets exist only in service of the Speculating window: the whole bet machine below runs while the batch is Speculating, and past the window only settling residue (a cancelling build draining) and the winning passed bet remain on the book — the latter retained until the batch is terminal so merge finalization and hand-off re-verification keep their input.

**Batch-level cancel integration.** When a head enters Cancelling, the pass cancels its bets (a cancelling head has no future to bet on) and stops generating for it; once every bet is terminal, the pass writes Cancelling → Cancelled on the batch and publishes its conclusion — today's cancel sweep, expressed as ordinary planning. A *dependency* entering Cancelling stops being bettable (dependency eligibility already excludes it), while already-dispatched bets follow today's leniency rules for cancelling base members. The known race — a cancelling batch whose merge still wins — resolves as today: terminal Succeeded prevails, and the next pass re-derives from whatever actually happened.

### The bet machine, inside the Speculating window

Two figures: first where a bet's transitions come from — one pass, its Speculator, and the executor loop around them — then the machine itself, with each arrow labeled by what drives it.

```
   dirty(queue) ◀── buildsignal · new batch · mergesignal · cancel · DLQ reconcilers
       │
       ▼
 ┌─────────────────────── one speculation pass (per queue) ───────────────────────┐
 │                                                                                │
 │  1 snapshot   live batches, book, bets, execution records, builds              │
 │       │                                                                        │
 │       ▼       ┌──────────── Speculator — extension seam ────────────┐          │
 │               │ 2 Belief (ext)     per-dep p(land) + resolutions    │          │
 │               │ 3 Generator (ext)  per head (within the injected    │          │
 │               │                    depth bound), best-first:        │          │
 │               │                      ┌┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┐       │          │
 │               │                      ┆ virtual candidates — ┆       │          │
 │               │                      ┆ derived, never stored┆       │          │
 │               │                      └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘       │          │
 │               │ 4 Admitter (ext)   candidates + active bets,        │          │
 │               │                    ranked under the injected budget │          │
 │               │                    ─▶ plan                          │          │
 │               └───────────────────────┬─────────────────────────────┘          │
 │                                       │  dispatch / cancel / keep bets,        │
 │                                       ▼  finalize / fail batches               │
 │  5 enact (controller — single writer of bets and the book):                    │
 │    bet CASes ▸ book CAS ▸ publish dispatch & cancel ▸ re-arm tick              │
 └─────┬─────────────────────────────────────────────────────▲────────────────────┘
       │ dispatch / cancel                                   │ dirty(queue)
       │ (path ID, attempt)                                  │
       ▼                                                     │
   build stage (executor):                             buildsignal:
   guard on bet status + attempt,       CI build       records the CI result
   write execution record,         ──▶  runs     ──▶   into the build entity,
   trigger or cancel the CI build                      then signals dirty
```

The extension boundary is the Speculator box. The Speculator is the seam the controller calls; Belief, Generator, and Admitter are the composed default's sub-seams, each independently swappable; the depth bound and the budget enter it as injected limit policies — extension surface as well. Steps 1 and 5 stay controller code on purpose: purity on the way in (the snapshot), single-writer discipline on the way out (enact).

```
 ┌┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┐
 ┆ virtual candidate — exists only inside step 3; re-derived every ┆
 ┆ pass; no record, no status, nothing to cancel                   ┆
 └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘
        │
        │  PLAN  Admitter admits under budget → bet record created
        │        (attempt 1), book add, dispatch published
        ▼
   dispatched ──FACT execution started──▶ building ──FACT build passed──▶ passed
        ▲  │                                 │   └───FACT build failed──▶ failed
        │  │  PLAN revoked                   │
        │  │  before start                   │  PLAN cancel: refuted (a fact contradicts
        │  ▼                                 ▼        the path) / preempted (policy)
        │ cancelling ──FACT execution terminal──▶ cancelled ── pruned from the book
        │                                             │
        └───────── PLAN re-admitted (attempt+1) ──────┘
                   from cancelled (viable again, torn-cancel recovery)
                   or failed (retry policy); refuted paths never resurrect

   passed feeds merge finalization once its path matches merged reality;
   the bet stays on the book until its head batch is terminal.
```

Every arrow is written by the pass — the single writer — but the arrows come in two families. **PLAN** arrows enact a decision out of the Speculator: admit, cancel, resurrect. **FACT** arrows reconcile an observation into bet status: the executor's execution record and the build's outcome, which reach the pass as a dirty signal and are written at the *next* pass — which is why `dispatched → building` waits for evidence the build actually started instead of being assumed at dispatch. Refutation deliberately sits in both families: its *trigger* is a fact (a resolution contradicts the path), but the cancel it forces is still enacted by the pass like any other plan output.

| Transition | Trigger (all written by the pass) |
|---|---|
| → dispatched | Admitter admits the path; record created, book updated, dispatch published |
| dispatched → building | Execution record + build state show the build started |
| building → passed / failed | Build result observed for the current attempt |
| dispatched / building → cancelling | Path refuted by a resolution, preempted (dominated on priority), or head batch cancelling |
| cancelling → cancelled | The attempt's execution observed terminal; then pruned from the book |
| cancelled / failed → dispatched (attempt+1) | Re-admission of a still-viable path (budget re-desire, flake retry, torn-cancel recovery); never for refuted paths |

Statuses and outcomes are per-attempt: "this bet passed" always means "its current attempt's build passed."

What disappeared relative to speculation.md's status machine, and why:

- **`candidate` is gone.** Candidates are not entities, so they have no lifecycle. The `candidate → cancelled` transition — writing tombstones for paths that never did anything — was bookkeeping for the stored-tree representation, and it is deleted wholesale. Cancellation now only ever applies to real dispatched work, the only thing with side effects to unwind.
- **`selected` and `prioritized` are gone.** Both encoded "desire waiting for supply" — a path chosen by the selector but parked pending the prioritizer. Desire that does not fit the budget is never materialized: it stays virtual, is re-derived next pass, and is admitted when capacity frees. Dispatched means admitted *and* sent — one state instead of a two-stage limbo.

## Extension seams

One controller-facing seam, three composable sub-seams beneath it. The controller mandates only snapshot → plan → enact; the decomposition below ships as the default implementation, not as controller orchestration — which is the deepest contract change in this RFC.

**Speculator** — the seam the pass calls. In: the queue snapshot (per Vocabulary) — the subject being decided over; richer signals arrive via factory injection, per the extension contract. Out: the plan — status updates reconciled from facts, paths to dispatch (minting new bets or resurrecting settled ones), bets to cancel, batches to finalize, fail, or conclude. Pure: same snapshot, same plan. The plan is split-natured, and the contract keeps the halves distinct: bet decisions (dispatch, cancel) are *policy* — the implementation's judgment; batch verdicts (finalize, fail, conclude) are *derivations* — rule-bound by merge readiness, no-viable-path, and cancel quiescence, which every implementation must emit whenever the rules fire. Swapping Speculators may change which bets run; it may never change whether a ready batch merges. The seam sits deliberately on the pure/impure boundary: deterministic policy inside, all I/O outside — which is what makes shadow mode and snapshot-replay property tests possible at all.

The default Speculator is a composed implementation assembled in the wiring layer from three sub-seams, each with its own design contract:

**Belief — the probability model** (the pathscorer's successor). *Represents:* what the queue currently believes about each dependency's fate. *Consumes:* per-dependency batch state from the snapshot — `Batch.Score` as the **prior**, set upstream by the score stage and never recomputed here — sharpened by live evidence: resolution state and dependency build outcomes, plus factory-injected signals such as historical pass rates. *Produces:* two things — the per-dependency belief, the current probability that dependency lands, with resolved facts saturating it (landed is certainty, failed or cancelled is zero); and path valuation, the probability a given path matches the future, composed from per-dependency beliefs as a point query (independent product is the default composition). This is where path probability lives in the new design: priors arrive from upstream, Belief turns priors plus evidence into path probabilities on demand, and nothing ever scores "the whole tree" because there is no tree to score. *Obligations:* calibrated probabilities in [0, 1], deterministic per snapshot; urgency, fairness, and wait time are deliberately excluded — they are ranking concerns, not evidence, and belong to the Admitter.

**Generator — the ordered source of futures** (the enumerator's successor). *Represents:* which paths are worth considering next, per head. *Consumes:* one head, its unresolved dependencies (already within the depth bound), and Belief. *Produces:* a pull-based stream of candidate paths in non-increasing valuation order, each with its valuation, no duplicates; paths range over the unresolved dependencies only, so a yielded candidate can never contradict a resolved fact by construction. *Obligations:* descending order however achieved — a factorized valuation admits lazy k-best generation; an implementation that cannot order lazily may materialize internally and stream from a sorted buffer, keeping the materialization private; deterministic per input; the space it ranges over (chain prefixes, down-closed subsets of the dependency DAG, the full subset lattice) is the implementation's choice. **It never withholds by price:** every path in its space is eventually yielded if pulled — the only exclusions are structural (incoherent paths, outside the space at any price) and exhaustion. Stopping knowledge (budget, floor, fairness) is the consumer's — a Generator is never told to stop, it stops being pulled, and an unpulled stream does no work; eligibility (the depth bound) is the driver's, applied before a stream exists. Both are cross-head or pre-generation decisions a per-head stream cannot make.

How generation differs from the enumeration it succeeds: the enumerator materialized the complete candidate set eagerly, unordered and deliberately score-blind, and handed the whole thing to a separate scoring pass; the generator yields candidates one at a time, ordered by current valuation, over the live collapsed space, and only as many as admission actually pulls. Enumeration answered "what futures exist"; generation answers "what future is worth considering next" — same purity and determinism, opposite materialization default, with ordering (and therefore Belief) now part of the contract the old seam was blind to by design.

Belief precedes generation as a data dependency, not a pipeline phase: a best-first stream is ordered *by* valuation, so the valuation must guide the search rather than grade its output. The cost split is deliberate: step 2 eagerly computes only the per-dependency beliefs — one number per live dependency, shared by every head — while per-path valuations are lazy point queries made during generation and admission, so the path space is never valued in bulk. And the two remain separate seams because they vary independently — the probability model evolves with evidence and data-science iteration, the space-and-ordering algorithm with structural insight — and because Belief serves consumers generation never sees: the pass re-values the active bets against it on every pass (that is how a bet scored B1·B2·B3 becomes B2·B3 after B1 lands), and the explain surface prices arbitrary paths with it.

**Admitter — the budget's arbiter** (selector and prioritizer, merged). *Represents:* how the queue spends its build budget right now. *Consumes:* every within-depth head's generator stream, the active bets with their statuses and attempts, and its injected budget limit (the prioritization limit's descendant, consulted by the Admitter itself per speculation.md's limits-live-with-their-seam rule). *Produces:* the admission — paths to dispatch (minting or resurrecting bets) and bets to cancel — by merging the per-head streams against the budget, pulling each stream only as deep as the budget and its own quality floor make relevant. A floor is a stopping rule, not a filter: because each stream descends, a policy like "admit nothing below 0.2" ends a head's stream at the first candidate beneath it, costing the candidates that clear the floor plus one — never the space. *Obligations:* kept bets, cancelling bets whose builds still hold CI, and new admissions together never exceed the budget; passed bets are never cancelled; refuted paths are never admitted; and it ranks on **priority** — valuation adjusted by wait and fairness — which is exactly and only where urgency enters the system, so long-waiting heads age into the budget while Belief's probabilities stay pure. Preemption is its call — see the hazard documented below.

### Inside the default Speculator: one pass, in order

The contracts above say what each seam owes; this is the order the composed default runs them in, expanding the five steps the pass diagrams show — reconcile lives inside diagram step 1, disposition inside step 4, and verdict computation belongs to the Speculator even though its writes land in step 5. Two rules keep the flow honest. First, **nothing is applied mid-flow**: the Speculator only computes — reconciliations, cancels, admissions, and verdicts accumulate into the plan, and every write happens once, in enact, under the write-order discipline. Second, **admission drives generation**: streams are constructed early but do no work until pulled, so "generate then admit" is a data dependency, not two phases.

1. **Snapshot + reconcile** *(diagram step 1)*. The controller reads; the driver reconciles each active bet against its execution record and build into an effective status — a building bet whose build passed is treated as passed from here on, and written as passed in enact. Derivation, no policy.
2. **Believe** *(step 2)*. Per-dependency probabilities and resolution states, from batch state plus the reconciled build evidence.
3. **Generate** *(step 3)*. One stream per within-depth head — the depth bound is applied by the driver here, before any stream exists. Construction is free; no valuation has happened yet.
4. **Admit** *(step 4)*. The Admitter's opening move is **disposition** of the active bets: refuted paths must cancel (rule-bound — a resolution contradicts them), unrefuted in-flight bets are kept (v1 stickiness), and the budget is charged for keeps and for cancelling bets whose builds still hold CI. Free slots fall out; then the merge pulls streams until budget, floor, or space ends it. Zero free slots under never-preempt stickiness means zero pulls.
5. **Verdicts, then plan out** *(written in step 5)*. Rule-bound derivations over the same view — merge readiness → finalize, no viable path → fail, cancel quiescence → Cancelled plus conclude — join the status updates and bet decisions in the plan, and enact writes it all: bet CASes, book CAS, publishes, heal republishes, re-armed tick.

**What binds a replacement Speculator, and what does not.** Everything in the outline above is the default's convention — the sub-seam decomposition, the execution order, lazy pull-based generation, disposition-then-pull, the stickiness default — and a from-scratch Speculator may discard all of it. What it may not discard is the seam contract the outline is built on: purity and determinism over the snapshot; the rule-bound plan content, status updates (reconciled facts) and batch verdicts computed exactly per the rules and never withheld; the injected limits honored — kept, still-cancelling, and admitted together never exceed the budget, no head planned beyond the depth bound; passed bets never cancelled; refuted bets cancelled rather than left running; refuted or incoherent paths never dispatched. These obligations are decomposition-independent, and deliberately so: they are the same list the property tests assert and shadow mode diffs against, because a replacement Speculator is judged by its plan, never by its insides. That is also the cutover story: because the pass is pure, a Speculator can run in **shadow mode** — consume dirty signals, compute plans, enact nothing, diff the would-be admissions against live decisions — an acceptance test on production traffic with zero blast radius.

**Preemption is the Admitter's call — with one hazard documented.** Whether an in-flight bet may be cancelled in favor of a better candidate, and by what margin, is implementation policy like everything else in this seam. What every implementation must reckon with: cancelling a running build throws away real CI work, and because a still-viable path can be re-admitted, an Admitter that chases every score wobble can cancel and re-dispatch the same bet in a loop, burning a build each cycle. Refutation cancels are always right — the bet can never merge; preemption cancels are the ones that need a reason. The default implementation is sticky — it never preempts a building bet, today's sticky prioritizer's policy — and anything more aggressive is expected to preempt only above an explicit margin.

Extension happens at two granularities, which is the point of the split:

- **Swap a sub-seam** for formula-level changes: a new probability model is a Belief impl; a smarter top-K or a DAG-aware path space is a Generator impl; a fairness or preemption policy is an Admitter impl. Each is a pure function over literal inputs, testable with no store and no queue.
- **Replace the Speculator whole** for structurally different approaches — joint optimization that does not factor into generate-then-admit, learned policies, simulation-based planning. The sub-seam decomposition is a convention the default ships with, not a contract the controller imposes.

### Extension APIs

The contracts above say what each seam owes; these are the API shapes that make those obligations structural. The method sets and shapes are **normative design decisions** — the stream, the point-query view, the plan-as-entire-write-set — while exact field lists are indicative and may evolve in review. Every seam lives under `submitqueue/extension/speculation/{seam}` and follows the repo's extension pattern: a `Config` carrying the queue name, a `Factory` building the seam for that queue, with limit policies and richer signals injected at construction — never passed per call.

```go
// speculator — the seam the controller calls. Pure: no I/O, no stores;
// same snapshot ⇒ same plan.
type Speculator interface {
	Plan(ctx context.Context, snap Snapshot) (Plan, error)
}

// Snapshot is the controller-assembled view — data only: live batches,
// the book, active bets, execution records, builds.
type Snapshot struct{ /* live batches, book, bets, executions, builds */ }

// Plan is everything enact will write and publish, and nothing else.
type Plan struct {
	StatusUpdates []BetStatusUpdate // (path ID, attempt) → observed status; rule-bound
	Dispatches []entity.SpeculationPath // enact derives create-vs-resurrect by content address
	Cancels    []string            // path IDs
	Verdicts   []BatchVerdict      // finalize / fail / conclude; rule-bound
}
```

```go
// belief — prices dependencies and paths.
type Belief interface {
	// Derive builds the pass-local view: per-dependency beliefs eagerly,
	// path valuation lazily. Batches carry the priors (Batch.Score)
	// and resolutions; builds carry the mid-flight evidence — a dependency
	// whose own build passed is likelier to land than its prior said,
	// before any resolution makes it certain.
	Derive(ctx context.Context, batches []entity.Batch, builds []entity.Build) (View, error)
}

type View interface {
	// Dependency: current probability the dependency lands, plus its
	// resolution (unresolved / landed / ruled out). Resolved facts saturate.
	Dependency(batchID string) (DependencyBelief, bool)
	// Valuation prices one path — a point query, never bulk.
	Valuation(p entity.SpeculationPath) float64
}
```

```go
// generator — the ordered source of futures. Returning a stream is the
// deliberate API decision that makes laziness contractual: the consumer
// pulls, and the producer never learns why pulling stopped.
type Generator interface {
	// Stream opens the head's candidate stream over its unresolved
	// dependencies, best-first by the view's valuation.
	Stream(ctx context.Context, head entity.Batch, unresolved []entity.Batch, view belief.View) (Stream, error)
}

type Stream interface {
	// Next yields the next-best path with its valuation;
	// ok = false means the head's space is exhausted.
	Next(ctx context.Context) (c Candidate, ok bool, err error)
}

type Candidate struct {
	Path       entity.SpeculationPath
	Valuation  float64
}
```

```go
// admitter — disposes the active bets, then fills free slots from the
// streams. Bets arrive with their effective (already reconciled) statuses.
// The budget (and any floor) is injected at the Factory and consulted
// inside Admit.
type Admitter interface {
	Admit(ctx context.Context, active []entity.SpeculationBet, streams []HeadStream, view belief.View) (Admission, error)
}

// Admission omits kept bets — leave-as-is, the selector's precedent. Cancels
// cover refutations (mandatory) and preemptions (policy, sticky by default).
type Admission struct {
	Cancels    []string            // path IDs
	Dispatches []entity.SpeculationPath // admissions, in no implied order
}
```

```go
// The surviving limit policies — one method each, signal-driven per
// speculation.md; injected into the Admitter and the Speculator's driver.
type Budget interface{ Current(ctx context.Context) (int, error) }
type DepthBound interface{ Current(ctx context.Context) (int, error) }
```

Each obligation is a shape: laziness is `Stream`; eager-per-dep/lazy-per-path is `Derive` returning a `View` whose `Valuation` is a method, not a field; single-writer purity is `Plan` as a value, not callbacks into stores; keeps-cost-nothing is `Admission` omitting them; the Generator's ignorance of budget, floor, and depth is their absence from its signature. Only `View.Valuation` produces a number — it *is* the path scorer, the pathscorer's successor; the Generator and the Admitter decide which paths are worth pricing, never what they are worth. The Generator owns the walk, Belief the numbers; pairing them is a wiring concern — a composition that cannot order flips monotonically forces a paired generator to its private-materialization fallback.

### The fate of the three limits

- **Prioritization limit → the budget**, injected into the Admitter, enforced at plan time. The build stage stops rationing and just executes. Across *disconnected* dependency sets the budget is also the only rationing there is: independent components have no structural relationship, so how many build concurrently is purely a resource question.
- **Selection limit → deleted.** Its job — stopping one head from swamping the queue with hedges — falls out of admission itself: a head's paths are mutually exclusive futures, so each additional hedge covers a strictly lower-probability future and loses the marginal comparison to another head's first path long before any knob would bite. What remains of per-batch fairness is starvation, and that is Admitter policy, not a limit: priority is valuation adjusted by wait, so a long-waiting head ages into the budget while its valuation stays untouched.
- **Dependency limit → a per-connected-set depth bound**, consulted inside the Speculator before it generates for a head. The meaning survives intact — the maximum unresolved dependencies a head may be planned over — only the mechanism changes: no eligibility state and no controller gate; a head past the bound simply is not generated for this pass, and every resolution shrinks its depth until it enters naturally. The bound is nearly free in value terms — deep paths self-penalize through belief composition (a product of many probabilities ranks itself into oblivion), so the region beyond the bound holds almost nothing the budget would fund — and what it buys is hard: a cap on pass cost that does not depend on the scoring being well-behaved. It remains a signal-driven policy per speculation.md; depth can shrink under CI pressure.

**Rescoring while queued resolves by construction.** speculation.md left open which layer should refresh a batch's standing as the world moves. The evidence half is automatic here: every pass recomputes scores from ground truth, so when a dependency merges, the paths that bet on it lose that factor to certainty — a path scored B1·B2·B3 becomes B2·B3 the pass after B1 lands — while sibling paths that bet against B1 are refuted in the same pass, and freshly generated candidates no longer mention B1 at all. Nothing rescores in place; the score is re-derived, so it is always current. The only half that is not derivable from state is urgency — how long a head has waited — and that is deliberately an Admitter ranking input, not a score input, keeping Belief a calibrated probability.

## Four passes, by example

Queue `q`, four batches: `B2` depends on `B1`, `B3` depends on `B1` and `B2` (a chain — `B3`'s dependency list is the transitive closure), and `B4` is independent — a disconnected root. Priors from the score stage: p(B1)=.90, p(B2)=.80. Budget 3, depth bound 2, admission floor .25. Notation: `[base]→head`, valuation = product of p over the base × product of (1−p) over the excluded unresolved deps — the probability that exactly this path is the realized future. (The head's own build risk is identical across all of its paths, so it cancels within a head; whether cross-head ranking folds it in is Admitter policy, ignored here.) The Generator ranges over down-closed sets of the DAG, so a path betting on `B2` while betting against `B1` is never emitted.

```
pass 1 — dirty: batches scored into an empty queue
──────────────────────────────────────────────────
snapshot   live {B1,B2,B3,B4} · book {} · no bets, no builds
beliefs    p(B1)=.90  p(B2)=.80          (priors only — no evidence yet)
generate   B1 ▸ []→B1 1.0        B4 ▸ []→B4 1.0
           B2 ▸ [B1]→B2 .90      B3 ▸ [B1,B2]→B3 .72   (B3 has 2 unresolved deps —
           (one pull per head; deeper candidates       exactly at the depth bound;
            stay unpulled, unyielded)                  at bound 1 it would wait)
admit      budget 3 ▸ ✔ []→B1 1.0  ✔ []→B4 1.0  ✔ [B1]→B2 .90 │ ✘ [B1,B2]→B3 .72 — no
           slot; stays virtual: no record, no status, nothing to clean up later
enact      create bets b1,b4,b2 (attempt 1) · book {b1,b4,b2} · 3 dispatches · re-arm tick
```

How the Generator found B3's best future — the whole trick in one table. Each dependency is a two-way pick with a price: bet *on* it (worth p) or bet *against* it (worth 1−p). A future is one pick per dependency; its valuation is the product of its picks:

```
                 pick for B1         pick for B2         valuation
                 on .90 / off .10    on .80 / off .20
 [B1,B2]→B3         .90         ×       .80         =    .72
 [B1]→B3            .90         ×       .20         =    .18
 []→B3              .10         ×       .20         =    .02
 [B2]→B3            .10         ×       .80         =    (never exists: B2 needs B1)
```

Three things to read off the table:

- **Best future = take the bigger pick in every column.** .90 beats .10, .80 beats .20 → `[B1,B2]`, found in one step, no enumeration. That is the entire "p ≥ .5" rule — it only asks which of p and 1−p is bigger. Nothing is tuned and nothing is excluded; it is argmax arithmetic, not policy.
- **Next-best = change the one pick that costs least.** Swapping B2's pick (.80 → .20) hurts less than swapping B1's (which drags B2 out too, since B2 needs B1), so `[B1]` at .18 comes second and `[]` at .02 last. Every swap can only shrink the product, so the stream descends automatically — the alternatives all exist, they just come later.
- **The table itself is the old design.** Exhaustive built, persisted, and scored every row up front; here only pulled rows are ever priced. Pass 1 pulled B3's stream once to seed the merge, the budget filled, and no other row was yielded. Mechanically nothing is resident: a stream holds a small frontier — the single swaps of rows already yielded — and each `Next()` expands and pops on demand, so work tracks pulls × dependencies, never the table. "The stream contains `[]→B3`" means it *ranges over* it, not that it holds it.

The same machinery under a deep budget — say 10 slots. Seeding still costs one pull per head (four); the four admissions trigger successor pulls (`[]→B2` .10, `[B1]→B3` .18; B1's and B4's streams are exhausted); and then the floor, not the budget, binds: both successors sit below .25, ordered streams mean everything deeper is worse, so the merge stops with 6 of the queue's 7 valid paths ever yielded, 4 admitted, and `[]→B3` never yielded at all. Drop the floor and the merge keeps going — .18, .10, .02 all admit, the whole 7-path space funds, 3 slots idle: every future of every head built in parallel, the old exhaustive-and-build-everything behavior recovered as a knob setting rather than an architecture, and still only 7 valuations rather than a materialized tree. The general shape: generation costs the admissions plus one below-cutoff candidate per stream still open — the receipts proving it was right to stop — never the space.

```
pass 2 — dirty: buildsignal, []→B1's build passed
─────────────────────────────────────────────────
snapshot   bets b1 building→(FACT: build passed), b4 building, b2 building
beliefs    p(B1)=.97 (its build passed — evidence sharpens the prior)   p(B2)=.80
verdicts   b1 passed + empty base ⇒ B1 finalized: CAS Merging, publish merge
           (strict rule satisfied vacuously — no predecessors to wait on)
generate   B3 ▸ [B1,B2]→B3 now .97×.80 = .78   (yesterday's reject, repriced)
admit      b1 passed frees its slot (passed bets hold no budget; the record stays
           on the book until B1 is terminal) ▸ ✔ [B1,B2]→B3 .78
enact      b1 → passed · B1 → Merging + merge publish · create b3 (attempt 1) ·
           book {b1,b4,b2,b3} · 1 dispatch · re-arm tick
```

```
pass 3 — dirty: mergesignal, B1 Succeeded  (B4 passed and finalized meanwhile)
──────────────────────────────────────────────────────────────────────────────
snapshot   B1 terminal · bets b2 building, b3 building, b4 passed (B4 in Merging)
beliefs    B1 = certainty (resolved fact, no longer a probability)   p(B2)=.80
collapse   b2 [B1]→B2 ≡ []→B2 over the unresolved space — its one assumption became
           fact, valuation .90→1.0; the fresh candidate []→B2 is COVERED by the
           running bet (paths compare modulo resolved deps) — no re-dispatch
           b3 [B1,B2]→B3 ≡ [B2]→B3, valuation .72→.80
prune      b1: B1 terminal ⇒ taken off the book (record retained for GC rule)
admit      one free slot (the elided B4 pass admitted nothing — its successors sat
           below the floor) · B3's next: []→B3 = 1−.80 = .20 < floor .25 ⇒ the
           stream ends at its first uncovered pull; nothing deeper was ever yielded
enact      book {b2,b3,b4} · no dispatches · re-arm tick
```

```
pass 4 — dirty: buildsignal, B2's build failed
──────────────────────────────────────────────
snapshot   bets b2 building→(FACT: build failed), b3 building, b4 passed
verdicts   B2's only live path failed, retry policy off ⇒ batch verdict: B2 Failed
beliefs    B2 = ruled out (resolved fact)
refute     b3's collapsed path [B2]→B3 bets on B2 ⇒ REFUTED — a fact contradicts
           it, not a policy choice ⇒ PLAN cancel: b3 → cancelling, cancel published
admit      budget 3, charged 1 — b3's cancelling build holds CI until observed
           terminal ⇒ two slots free ▸ ✔ []→B3 1.0 as a NEW bet b3′: different
           path, different hash, attempt 1 — not a resurrection
enact      b2 failed → off the book · b3 cancelling · create b3′ · 1 cancel ·
           1 dispatch · book {b3,b4,b3′} · re-arm tick
```

The coda: the next pass, after b3's cancel is observed terminal, takes b3 off the book — b3′ was already running, so the refutation cost one wasted build, never a stalled head. Resurrection played no part: `[]→B3` is a *different* path; resurrection re-runs the *same* path at attempt+1. Each pass above demonstrates its rules: pass 1 — laziness and desire-stays-virtual; pass 2 — evidence rescoring, strict finalization, and a rejected candidate repriced back in; pass 3 — the collapse, the equivalence rule preventing duplicate dispatch, book pruning, and the floor as stream truncation; pass 4 — refutation as fact-driven cancel, batch verdicts as rule-bound plan output, a cancelling build still charging the budget, and a replacement path minted as a new bet.


## Assumptions re-validated

Verdicts from that review, per assumption. One broke; its repair is now a design requirement.

**Per-queue pass serialization is available — holds, as best-effort.** Partition-keyed consumption gives serial-per-queue, concurrent-across-queues with existing machinery. It is not a mutual-exclusion guarantee: visibility-timeout redelivery can run two passes concurrently, so the choreography is written to survive that (CAS guards, absorbing torn writes, resurrection), and `VisibilityTimeoutMs` must exceed the worst-case pass duration to keep it rare.

**The pass can be the single writer of speculation state — held, after one repair.** Every other flow already routes through "update own entity, then signal": buildsignal updates builds, cancel and mergesignal CAS the batch, DLQ reconcilers drive batch state and never touched trees. The repair: the CI build reference is minted by the executor, so it cannot live on the pass-owned bet record — the executor-owned execution record (the path→build mapping's descendant) is a required piece of the design, not an implementation detail. Two paths were also forced explicit: the pass owns the Cancelling → Cancelled and no-viable-path Failed batch writes plus conclusion publishes, and batch DLQ reconcilers must publish dirty signals so cleanup does not wait for the tick.

**Content-addressed identity survives its edge cases — broke; repaired by the attempt counter.** The naive scheme permanently retires legitimately-recurring paths and cannot converge after a torn cancel; the repair is the attempt counter exactly as specified under Path identity, with per-occurrence message IDs independently forced by the queue's publish-dedup semantics.

**Crash choreography converges from every interleaving — holds under the four rules.** The residual imperfections are bounded waste, not leaks: a stale winning pass buys one CI trigger the next pass cancels; a torn cancel costs one attempt. The over-claim from the first draft is retracted: the book CAS linearizes membership only.

**Per-event, uncoalesced passes are affordable — holds at expected scale.** The honest baseline is the problem statement's K per-batch passes plus K queue-wide prioritize sweeps per terminal event; the queue-scoped pass replaces all of it with one round: a book read, tens of bet/execution/build point-reads, the live batches of the queue, and generation bounded by the budget, not 2^N — strictly less total work per event. What is given up is parallelism, so the ceiling is event-rate × pass-duration < 1 per queue: at tens of events per minute and passes of a few hundred milliseconds (live-batch counts in the low hundreds), utilization sits around ten percent and reaction latency stays sub-second against minutes-long builds. The depth bound is the lever if queue depth grows past that; the crossover sits at thousands of live batches or near-second passes under sustained bursts.

**Merge safety does not depend on planning — holds, and strict finalization strengthens it.** Passed bets are untouchable by the pass; finalization and hand-off both read terminal batch states only (Cancelling never counts as ruled out — a cancelling batch can still land); the winning bet stays book-listed until its batch is terminal so a Merging batch's re-verification keeps its input. With optimistic hand-off deferred, a path that reaches merge is composed entirely of terminal facts, which do not change — hand-off re-verification becomes defense-in-depth rather than a live gate. No wrong-merge interleaving was found; the cancel-vs-passing-build race is lossy but safe, and liveness recovers through re-admission.

**Batch-level cancel integrates as pure derivation — holds.** Head cancelling → bets cancelled through normal choreography, terminal transition written after quiescence; dependency cancelling → excluded from eligibility by the existing dependency-state rules, with today's leniency for already-dispatched bets. One adjacent, pre-existing race is explicitly out of scope: a cancel CAS that wins after the push has physically landed records Cancelled while the changes are on the branch — this exists today and is unchanged by this design.

**Dropping stored candidates breaks no external surface — holds.** No gateway API or proto exposes speculation paths or trees; the list API explicitly excludes tree structure; history is the request log. The tree's only readers are the internal stages this design replaces. The genuine (minor) regression is the loss of the tree as a passive audit trail of considered-and-rejected candidates; the mitigation is a structured per-pass plan log plus the explain surface — because the pass is pure, an on-demand dry run (snapshot → plan, no enactment) shows the ranked candidates, the admitted set, and why each active bet is kept or cancelled, always current in a way persisted scores never were.

## Design decisions

**Queue-scoped speculation pass instead of batch-keyed respeculation.** One pass sees every head, the active bets, and the budget in a single frame. *Why:* speculation's decisions are inherently queue-wide (rationing a shared budget) — the current design already concedes this by running a queue-wide prioritize round after every per-batch pass; the per-batch partitioning is why selection and prioritization had to be separate stages; and one pass deletes the dependent-fanout machinery outright. *Rejected:* batch-keyed passes with an incremental belief cache — preserves the two-stage split and the fanout, and adds cache coherence instead of removing state.

**Persist bets, not candidates.** Only dispatched paths have records; the candidate space is derived every pass. *Why:* candidates are derivable (storing them is caching), their per-event maintenance is exactly the wide multi-row write pattern a transactionless store cannot make atomic, and a derived space cannot drift from reality. *Rejected:* persisting the full tree (today's model) — the root of all three costs in the problem statement; persisting a top slice of candidates — the same maintenance burden with a smaller constant.

**Content-addressed path identity with a record-held attempt counter.** The path ID hashes the path; the attempt lives on the record and keys executions and messages. *Why:* idempotent re-derivation is the foundation of transactionless enactment — a re-run pass converges on the same IDs and records — and the attempt absorbs legitimate re-admission *and* torn-cancel recovery without weakening identity. *Rejected:* controller-assigned opaque IDs (speculation.md's choice) — they cannot survive independent re-derivation, so every crash between decide and persist mints ghost identities; attempt-in-the-hash — identity would depend on history, forcing re-derivation to read persisted epoch state and letting racing passes mint two records for one path.

**The bet/execution split follows write ownership.** The pass owns bets and the book; the executor owns execution records. *Why:* the executor mints the build reference, so a build ref on the bet record means two writers on one row — the exact race class the store cannot arbitrate; the split is today's path→build mapping pattern, kept because it is load-bearing. *Rejected:* folding the build linkage into the bet record — simpler on paper, two-writer in practice.

**The book is the bet index and the membership linearization point — and only that.** One per-queue record lists active bets; its CAS decides which pass's membership changes win. *Why:* the two queries the system needs (bets of a queue, bets of a head) become one point-read plus point-reads by ID with zero secondary indexes; and a precise claim ("membership") survives adversarial review where the broad one ("the pass") did not. Live batches stay on the existing by-queue-and-states query rather than being mirrored into the book — no second copy to keep coherent. The name is chosen for what the record *is* — a bookmaker's book, the mutable register of open positions: bets come onto it when placed and off it when settled, and the budget is its exposure limit. *Rejected:* queue-prefixed bet key ranges — range scans the storage contract avoids; a book-maintained live-batch set — duplicates an existing index and adds hint-delivery machinery for no new capability; an append-only ledger, one entry per pass — linearization merely re-spells as conditional-append at the next sequence, reading current membership needs a latest-pointer or reverse scan, growth is unbounded and needs grooming, and the membership history it buys is the plan log's job — the artifact should not be bent append-only to fit an accounting name.

**Single writer, signal-only stages.** Bet and book writes happen only in the pass; every other stage updates its own entity and publishes a dirty signal. *Why:* one writer keeps the lifecycle coherent without cross-record atomicity, and "update your entity, signal the speculator" is already the pipeline's idiom. *Rejected:* letting buildsignal write bet status directly — a second writer on bets reintroduces the races the design exists to remove.

**One Speculator seam, composed default.** The controller calls a single Speculator; Belief, Generator, and Admitter compose inside the default implementation, assembled in wiring. *Why:* the controller mandating enumerate → score → select is what forced the materialized tree through every boundary; mandating only snapshot → plan → enact leaves implementations free to be lazy, greedy, or learned, while the shipped decomposition preserves formula-level swappability and isolated testing. *Rejected:* three controller-sequenced seams (today's shape) — the sequencing is the coupling; a Speculator with no shipped decomposition — loses the cheap extension points that make experimentation viable; folding the Speculator into the controller — the seam lies exactly on the pure/impure boundary (deterministic policy inside, I/O outside), and erasing it would make the sub-seam sequencing controller-mandated again — the old coupling with new names — while forfeiting shadow mode and snapshot-replay property tests; the name "Planner" — generic vocabulary from outside the domain, when the seam is precisely the thing that speculates.

**Admission replaces selection + prioritization; preemption policy stays with the Admitter.** One decision, at plan time, under the budget. *Why:* the split existed because per-batch selection could not see the queue; the pass can, so desire and supply reconcile in one place and the parked states disappear. Resurrection makes churn cheap to *express* and every preemption burns a real build — so the default Admitter is sticky (never preempt a building bet) and the churn hazard is documented at the seam — but the policy itself belongs to implementations. *Rejected:* keeping a build-stage prioritizer — it would re-ration what the pass already rationed; legislating stickiness in the seam contract (an earlier draft did) — it polices cost, not correctness, is unenforceable at the interface, and policy freedom is this seam's whole point.

**The dependency limit becomes a per-connected-set depth bound.** No eligibility gate; heads past the bound wait without a dedicated state, and disconnected sets are rationed by the budget alone. *Why:* the gate protected a materialization that no longer happens; the surviving concerns are pass cost, which a depth cap bounds directly, and speculation depth as an operator guarantee — and the bound is nearly free because deep paths already self-penalize through belief composition. Depth only means something within a connected component, so scoping it per set is what keeps one deep chain from starving unrelated roots. *Rejected:* keeping the eligibility gate — a controller-owned wait state whose only remaining purpose the bound serves more simply; a queue-position horizon ("plan the first N heads") — it would starve independent components sitting behind a deep chain in arrival order; keeping a per-head width limit (the selection limit) — same-head hedges are mutually exclusive futures whose diminishing marginal value the queue-wide ranking already prices, and starvation is aging policy in the Admitter, not a count.

## What this design gives up

- **Reaction latency is pass-bound.** Per-batch respeculates parallelize across batches; queue passes serialize. The pass must stay cheap; the depth bound is the enforcement lever, and the event-rate × pass-duration ceiling is the number to watch.
- **Blast radius.** A Speculator bug mis-plans the whole queue at once; per-batch isolated faults to one batch. Purity is the counterweight: the pass is deterministic and replayable from a snapshot, so it is property-testable (never exceed the budget, never cancel a passed bet, never dispatch a refuted path, never prune a bet with a live execution) in a way today's store-coupled pass is not.
- **Persisted desire is gone.** Today the store shows what each batch wanted separately from what the queue afforded — an analytical surface the plan log and explain surface recover on demand, but no longer as store history.
- **A new stage shape.** A queue-keyed, self-ticking stage is the first of its kind in the pipeline; the consumer wiring is existing machinery, but the operational playbook (what a stuck queue looks like, what the speculate DLQ's re-arm means, alerting on persistent pass failure) is new.

## Open questions

- **Depth-bound policy.** The per-connected-set depth bound caps pass cost and speculation depth, but its value is a policy (fixed, budget-derived, load-shedding under CI pressure). Which signals it weighs, and whether it is per-queue config or a limit-style seam, is open.
- **Optimistic merge finalization.** Deferred by design — see the deferral note under merge finalization. Recovering the pipelined hand-off is an additive extension with its own failure analysis (confirmed-versus-possible readiness, parking at the merge stage, unwinding a lost optimistic bet at the merge boundary), kept out of the speculation pass.
- **Limit change propagation.** Limits are pull-only; a moved limit takes effect at the next event or tick. If tighter reaction is ever needed, a change feed publishing dirty signals is the extension point — deliberately not built now.
- **Explain surface placement.** An orchestrator-local debug RPC is the minimum; whether any of it should reach the gateway's status APIs (batch-level "why is this waiting") is a product question, not a design one.
- **Drain-hook coalescing.** Deliberately deferred; the design is correct with one pass per event and no-change passes are read-only. If pass frequency ever matters, a drain hook in the speculate consumer is the contained optimization.
- **Belief model scope.** Whether beliefs stay per-batch-independent (product composition) or grow correlation awareness (shared targets, shared authors, build-system incidents) — the seam admits either; the default formula is a starting point.
- **Adjacent, pre-existing:** the cancel-vs-completed-push race (a batch recorded Cancelled after its changes physically landed) exists today and is unchanged here; it deserves its own fix independent of this design.
