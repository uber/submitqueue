# Speculation

A land queue that verifies one change at a time is limited by its slowest build. Speculation removes that limit: it builds a batch early, against an assumption about how the conflicting batches ahead of it will resolve, so a valid build is usually ready by the time they do.

Work enters SubmitQueue as **batches** — changes verified and landed together. Two batches **conflict** when they touch the same code, which makes the earlier one a **dependency** of the later. A **path** is one set of assumptions about how a batch's dependencies resolve, and the batch it builds is the path's **head**.

On every queue update the **speculate controller** reruns from scratch: it reads the current state, applies the incoming signals, asks a pluggable **Speculator** which paths are worth building within the CI budget, and persists only those. Everything else is recomputed next time, never stored. A batch normally lands after its dependencies resolve and a matching build has passed; complete passed coverage of every unresolved outcome lets it bypass those dependencies.

## The speculation run

The speculate controller runs whenever the queue changes — after a new batch, a completed build, a land result, or a cancel. Each publishes a **dirty signal** carrying the changed batch ID, partitioned by the queue so a queue's runs happen one at a time. The dirty signal is an internal queue contract — payload in `submitqueue/core/messagequeue`, topic key in `submitqueue/core/topickey`.

```
 dirty(queue) — "trigger a run" — published after:
   - a new batch
   - a build completes (success/failure/cancellation)
   - a land result arrives (success/failure)
   - a cancel
   │
   │  carries the changed batch ID, partitioned by queue, so a
   │  queue's runs happen one at a time
   ▼
 speculate  (orchestrator/controller/speculate) — all controller code,
 │          except step 3 (the one call out to the extension)
 │
 │ 1 read   in-flight batches, finalized dependencies they still reference,
 │          and all materialized path sets — once; never re-read mid-run
 │ 2 apply  signals — refute paths whose assumption a resolved dependency broke,
 │          finalize verdicts (below)
 │ 3 ask    Speculator.Speculate(batches, path sets)  ◀── the only extension call
 │          → paths to build, paths to preempt
 │ 4 check  validate that output: drop actions it shouldn't propose
 │          (non-Speculating head, refuted, incoherent, terminal path)
 │ 5 write  record each head's decisions; send build / cancel / land messages
 │          (a head whose write loses is re-planned on the next run)
 │
 ├─▶ build / cancel (path ID, attempt) → build (orchestrator/controller/build):
 │      reserve → BuildRunner.Trigger(base, head) → record build ID, mark path building
 │      CI runs → buildsignal marks path passed/failed/cancelled ─▶ dirty(queue)
 │
 └─▶ land (batch) → Runway performs the land
        → landsignal marks the batch succeeded/failed ─▶ dirty(queue)
```

### State reconciliation

Every run recomputes the whole queue from that single read — the batch ID on the trigger only wakes it.

Each path-status transition is written by the controller that observes it — nothing is polled or derived. The speculate run records a path *pending* when it funds it, and sets the *cancelling* intent when it refutes a path (a resolved dependency broke its assumption — one assumed to succeed failed, or one assumed to fail succeeded) or when a batch is cancelled. The build controller flips *pending → building* when it actually starts CI, so a backed-up build queue leaves the path *pending*. Each run idempotently re-sends the build dispatch for a funded pending path, keyed by (path ID, attempt), until the build controller records it *building* or terminal. The buildsignal consumer records the terminal status — *passed*, *failed*, or *cancelled* — when the build reaches it. The path stores no build ID; the execution record holds it, keyed by (path ID, attempt).

Every write is a compare-and-swap: a writer that loses re-reads on a later run. No run depends on an earlier one, so duplicated or reordered dirty signals are harmless. Any later queue update also reconciles earlier committed state because every run recomputes the whole queue.

### Finalization

Verdicts are controller-owned facts: the Speculator can neither compute nor veto them.

- **Land.** Each path carries an assumption about every dependency — *succeeds* (built on top of) or *fails* (built without). Normally, once a path's build has passed and every dependency has finished the way the path assumed — one assumed *succeeds* has landed, one assumed *fails* has failed or been cancelled — the speculate controller moves the head to Landing and hands it to Runway. A dependency that is merely *landing* has not finished, because a land can fail, so a single matching path still waits for the answer. Complete passed coverage is the exception described in Bypass large diff: it lets a head land before those answers arrive. If the hand-off is lost, the next run re-sends it. The same run sets the head's remaining in-flight paths *cancelling*: once the head can land they cannot help, and they hold CI slots until they stop. The landsignal controller records Runway's terminal result: success marks the head Succeeded, while failure marks it Failed. The result publishes a single dirty signal — no per-dependent fan-out — and the next run refutes paths whose assumption disagrees with the result: *fails* assumptions after success, *succeeds* assumptions after failure. The hand-off is idempotent, so Runway reports success without another land when the change is already present. A chain ordinarily lands one at a time, but a fully covered head can bypass its unsettled predecessors.
- **Failure (no viable path).** A batch fails when every possible future has a failed build — no path can pass, so it can never land.
- **Cancel.** A cancelled batch is driven terminal: its in-flight paths are set *cancelling*, then the batch is marked Cancelled once they stop (see Cancellation).

### Conflict relaxation

Conflict analysis is conservative — it flags any *possible* conflict — so heads carry dependencies that rarely matter and over-serialize. Relaxation is the intended answer: drop the weakest dependencies so a head does not wait on them.

**Not implemented.** An earlier design expressed it per path, with a third assumption value — *ignored* — meaning "this path makes no claim about this dependency". Nothing ever produced one, and the value has been removed rather than left as vocabulary the system could not create.

When relaxation is built, it belongs in the **controller**, as a trim of the dependency list before the snapshot is handed over: the Speculator then sees a head whose dependencies are exactly the ones that count, and a path stays a total function over them — one assumption per dependency, each *succeeds* or *fails*, with no third state to reason about. That keeps the decision where the other correctness decisions live, since dropping a dependency is a judgement about what may land untested, not about which candidate is most promising. It also keeps every consumer honest by construction: a land gate, a refutation check, or a generator cannot forget to special-case a value that does not exist.

The open question that design has to answer is what a stored path means once the trim changes between runs — a path built against a trimmed list no longer lines up with a head whose list has grown back, and `isWellFormed` rejects it. The per-path marker made that case self-describing; a trim does not, so the trim has to be either stable for a head's lifetime or recorded alongside the path.

Example of the payoff either way: `H` conflicts with `B1` and weak `B2`. Relax `B2`, and `H` lands once `B1` lands and its build passes — even if `B2` later lands. Without it, `H` waits on both.

### Bypass large diff

If a batch's passed builds cover *every* way its dependencies could resolve, the outcome is the same either way — so it can land now, ahead of them. Classic case: a small change stuck behind a slow one is built both with and without it; both pass, and it lands immediately.

The controller checks coverage over only the dependencies that have not settled yet. Settled dependencies pin each surviving path to the outcome that actually happened; for every combination of the remaining dependencies, the path set must contain a passed, unbroken path with that combination of assumptions. If any combination is missing, unbuilt, failed, or contradicted by a settled dependency, the head waits normally. The check only observes paths the Speculator already funded — it does not fund the exponential path space itself or alter the queue's build budget.

Coverage makes the bypass sound because whichever way the dependencies later resolve, a passed build already validated the resulting set of changes. The build order and land order differ: a path assuming dependency `D` succeeds validates `D` then head `H`, while bypass lands `H` before `D`. SubmitQueue treats those orders as content-equivalent. Runway still performs the real land, so if the reordered changes conflict textually, the older dependency can fail after the newer head has bypassed it; this is an accepted cost of landing the fully covered head early rather than a licence to put unlandable content on the target.

### Cancellation

Cancellation is best-effort: a batch marked *cancelling* may still land if a land wins the race, so terminal states prevail. A cancel sets the intent; a later run drives it terminal.

Two kinds of cancel, split by owner:

- **Preemption (extension).** The Speculator may propose `Cancel` on an in-flight path whose head is Speculating to free budget for a better candidate — its only cancel power. The controller validates it (never a passed path) and enacts it.
- **Correctness cancels (controller).** Refutation — a resolved dependency breaks a path's assumption, so the controller cancels that path — and batch cancellation (below). The Speculator is not consulted.

Batch cancellation (user-initiated) marks the batch *cancelling*: a non-terminal intent that halts new work. The speculate run drives it terminal — it sets its in-flight paths *cancelling*, marks the batch *cancelled* once they stop, recomputes dependent paths against that outcome (those that assume it fails keep building; those that assume it succeeds are refuted), and concludes the contained requests.

Cancelling a path sends a cancel (path ID, attempt) to the build controller, which cancels the CI build; that build holds its slot until the cancel reaches terminal.

## Speculator Extension

The one extension. It decides *which paths to build and which running ones to cancel* — nothing else; the controller handles the rest (reconciling facts, cancelling ruled-out paths, verdicts, checking output). A swapped-in Speculator changes which paths run, never whether a batch lands or fails.

**The contract** is `Speculate(batches, pathSets) → []Speculation`:

- **In:**
  - `batches`: every in-flight batch plus finalized batches still referenced as dependencies by an in-flight batch; each carries its dependency list and state.
  - `pathSets`: every materialized path for those batches, whether pending, in flight, or terminal, including recently finished paths retained to prevent duplicate work.
- **Out:** a list of build/cancel actions whose heads are in `BatchStateSpeculating`; batches in every other state provide facts but are never action targets.
- Budget and clock are injected at construction. An impl may read extra data (also injected); the controller checks the output, so extra data never affects correctness.

### The default Speculator

The default Speculator is composed from two swappable interfaces — a **Generator** and an **Allocator** — so scoring and preemption policy can vary independently. They are composition points inside the default implementation, not controller-facing extensions: the controller depends only on the Speculator contract, and an alternate Speculator need not use or expose this split. The default opens the Generator's candidate stream over the batches, then hands that stream and the path sets to the Allocator.

- **Generator** — yields the queue's candidate paths as one iterator across heads in `BatchStateSpeculating`. *Contract:* every candidate has a Speculating head and is coherent; none repeats or contradicts a resolved fact. Ranking is implementation-defined — the Generator may compute it directly, call an injected scorer extension, or use other injected data — and the score it carries is meaningful only within the run. The Allocator consumes the iterator in the order the Generator yields it and does not interpret the score. *Default:* `bestfirst` ranks best-first by the probability that a path's assumptions all hold.
- **Allocator** — spends the build budget (the queue's cap on concurrent builds) over the iterator. *Contract:* it pulls in order until the budget fills and matches candidates to existing paths by ID, so a pending or building path keeps the slot it already holds rather than starting a second attempt, and a candidate whose path is already terminal in the path sets is skipped rather than rebuilt; pending dispatches are replayed by the controller as described above. Pending, building, and cancelling paths charge the budget (a cancelling build holds CI until terminal), while terminal ones charge none. Cancellation is best-effort, so the Allocator does not spend capacity it merely expects a cancel to release and risk exceeding the hard CI cap. *Default:* the sticky policy fills only free slots and leaves in-flight builds running; a preempting policy cancels in-flight paths below the funded set. Budget is the only rationing lever — there is no ranking-score floor. A build cancelled to make room still charges budget until its cancel reaches terminal and publishes dirty, so the next run funds the released slot — the queue converges over successive ticks rather than oversubscribing in a single pass.

### Extension APIs

Signatures live in code and are not copied here, so they cannot drift. This section says what each contract is for and where to read it.

**Entities** — [`submitqueue/entity/speculation.go`](../../../submitqueue/entity/speculation.go). A `SpeculationPath` is a head batch plus one `PathDependency` per dependency in queue order, each carrying a `DependencyAssumption`: *succeeds* or *fails*. A `SpeculationPathEntry` is the stored record of one chosen path, keyed by a hash of its content, plus its status and attempt number; it holds no build reference — the execution record has that, keyed by (path ID, attempt) — and no ranking score, which means nothing outside the run that produced it. A `SpeculationPathSet` is one head's chosen paths, live and recently finished, under a single version for compare-and-swap. Every logical path is self-describing, but a store may encode the common head and ordered dependency IDs once per set and keep each path's assumptions positionally — one bit per dependency.

**Speculator** — [`submitqueue/extension/speculation/speculator`](../../../submitqueue/extension/speculation/speculator/README.md). `Speculate` takes one queue snapshot (the batches and their path sets) and returns the build and cancel actions it proposes; a path it wants left alone has no entry. Actions must target Speculating heads. Verdicts stay controller-owned, so there is no land or fail action.

**Generator and Allocator** — [`generator`](../../../submitqueue/extension/speculation/generator/README.md) and [`allocator`](../../../submitqueue/extension/speculation/allocator/README.md), the two composition points inside the default Speculator. The Generator opens a pull-based stream of candidate paths over the batches; the Allocator spends the build budget over that stream, reconciling it against the path sets. Both abort on a cancelled context.
