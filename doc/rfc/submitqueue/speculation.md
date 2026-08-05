# Speculation

A merge queue that verifies one change at a time is limited by its slowest build. Speculation removes that limit: it builds a batch early, against an assumption about how the conflicting batches ahead of it will resolve, so a valid build is usually ready by the time they do.

Work enters SubmitQueue as **batches** — changes verified and merged together. Two batches **conflict** when they touch the same code, which makes the earlier one a **dependency** of the later. A **path** is one set of assumptions about how a batch's dependencies resolve, and the batch it builds is the path's **head**.

On every queue update the **speculate controller** reruns from scratch: it reads the current state, applies the incoming signals, asks a pluggable **Speculator** which paths are worth building within the CI budget, and persists only those. Everything else is recomputed next time, never stored. Merging stays strict: a batch merges only after its dependencies resolve and a matching build has passed.

## The speculation run

The speculate controller runs whenever the queue changes — after a new batch, a completed build, a merge result, or a cancel. Each publishes a **dirty signal** carrying the changed batch ID, partitioned by the queue so a queue's runs happen one at a time. The dirty signal is an internal queue contract — payload in `submitqueue/core/messagequeue`, topic key in `submitqueue/core/topickey`.

```
 dirty(queue) — "trigger a run" — published after:
   - a new batch
   - a build completes (success/failure/cancellation)
   - a merge result arrives (success/failure)
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
 │ 5 write  record each head's decisions; send build / cancel / merge messages
 │          (a head whose write loses is re-planned on the next run)
 │
 ├─▶ build / cancel (path ID, attempt) → build (orchestrator/controller/build):
 │      reserve → BuildRunner.Trigger(base, head) → record build ID, mark path building
 │      CI runs → buildsignal marks path passed/failed/cancelled ─▶ dirty(queue)
 │
 └─▶ merge (batch) → Runway performs the merge
        → mergesignal marks the batch succeeded/failed ─▶ dirty(queue)
```

### State reconciliation

Every run recomputes the whole queue from that single read — the batch ID on the trigger only wakes it.

Each path-status transition is written by the controller that observes it — nothing is polled or derived. The speculate run records a path *pending* when it funds it, and sets the *cancelling* intent when it refutes a path (a resolved dependency broke its assumption — one assumed to succeed failed, or one assumed to fail succeeded) or when a batch is cancelled. The build controller flips *pending → building* when it actually starts CI, so a backed-up build queue leaves the path *pending*. Each run idempotently re-sends the build dispatch for a funded pending path, keyed by (path ID, attempt), until the build controller records it *building* or terminal. The buildsignal consumer records the terminal status — *passed*, *failed*, or *cancelled* — when the build reaches it. The path stores no build ID; the execution record holds it, keyed by (path ID, attempt).

Every write is a compare-and-swap: a writer that loses re-reads on a later run. No run depends on an earlier one, so duplicated or reordered dirty signals are harmless. Any later queue update also reconciles earlier committed state because every run recomputes the whole queue.

### Finalization

Verdicts are controller-owned facts: the Speculator can neither compute nor veto them.

- **Merge (strict).** Each path carries an assumption about every dependency — *succeeds* (built on top of), *fails* (built without), or *ignored*. Once a path's build has passed and every dependency it assumes *succeeds* has merged, the speculate controller moves the head to Merging and hands it to Runway — it waits only on the dependencies it was built on top of, not the head's full dependency list. If that hand-off is lost, the next run re-sends it. The same run sets the head's remaining in-flight paths *cancelling*: once one path has passed the others cannot help, and they hold CI slots until they stop. The mergesignal controller records Runway's terminal result: success marks the head Succeeded, while failure marks it Failed. The result publishes a single dirty signal — no per-dependent fan-out — and the next run refutes paths whose assumption disagrees with the result: *fails* assumptions after success, *succeeds* assumptions after failure. The hand-off is idempotent, so Runway reports success without another merge when the change is already present. Down a chain, each head waits for the predecessors it assumes succeed, so a chain merges one at a time.
- **Failure (no viable path).** A batch fails when every possible future has a failed build — no path can pass, so it can never merge.
- **Cancel.** A cancelled batch is driven terminal: its in-flight paths are set *cancelling*, then the batch is marked Cancelled once they stop (see Cancellation).

### Conflict relaxation

Conflict analysis is conservative — it flags any *possible* conflict — so heads carry dependencies that rarely matter and over-serialize. Relaxation lets the Speculator **ignore** the weakest: the path marks that dependency *ignored*, and its outcome neither gates the merge nor refutes the path. Which to ignore is a per-run Speculator policy.

That marking lives on the path, so the path stays self-describing: finalization needs no external relaxed set (relaxing is what shrinks the space of outcomes a head's paths range over).

Example: `H` conflicts with `B1` and weak `B2`. Ignore `B2`, and `H` merges once `B1` merges and its build passes — even if `B2` later merges. Without it, `H` waits on both.

### Bypass large diff

If a batch's passed builds cover *every* way its dependencies could resolve, the outcome is the same either way — so it can merge now, ahead of them. Classic case: a small change stuck behind a slow one is built both with and without it; both pass, and it merges immediately.

The default Speculator covers the whole space only when doing so is cheap enough, and funds the extra candidates within the build budget. The controller merges early only when a passed path exists for every combination of the dependencies — it reads that straight off the path records. If any combination is missing or unbuilt, the head waits normally.

### Cancellation

Cancellation is best-effort: a batch marked *cancelling* may still merge if a merge wins the race, so terminal states prevail. A cancel sets the intent; a later run drives it terminal.

Two kinds of cancel, split by owner:

- **Preemption (extension).** The Speculator may propose `Cancel` on an in-flight path whose head is Speculating to free budget for a better candidate — its only cancel power. The controller validates it (never a passed path) and enacts it.
- **Correctness cancels (controller).** Refutation — a resolved dependency breaks a path's assumption, so the controller cancels that path — and batch cancellation (below). The Speculator is not consulted.

Batch cancellation (user-initiated) marks the batch *cancelling*: a non-terminal intent that halts new work. The speculate run drives it terminal — it sets its in-flight paths *cancelling*, marks the batch *cancelled* once they stop, recomputes dependent paths against that outcome (those that assume it fails keep building; those that assume it succeeds are refuted), and concludes the contained requests.

Cancelling a path sends a cancel (path ID, attempt) to the build controller, which cancels the CI build; that build holds its slot until the cancel reaches terminal.

## Speculator Extension

The one extension. It decides *which paths to build and which running ones to cancel* — nothing else; the controller handles the rest (reconciling facts, cancelling ruled-out paths, verdicts, checking output). A swapped-in Speculator changes which paths run, never whether a batch merges or fails.

**The contract** is `Speculate(batches, pathSets) → []Speculation`:

- **In:**
  - `batches`: every in-flight batch plus finalized batches still referenced as dependencies by an in-flight batch; each carries its dependency list and state.
  - `pathSets`: every materialized path for those batches, whether pending, in flight, or terminal, including recently finished paths retained to prevent duplicate work.
- **Out:** a list of build/cancel actions whose heads are in `BatchStateSpeculating`; batches in every other state provide facts but are never action targets.
- Budget and clock are injected at construction. An impl may read extra data (also injected); the controller checks the output, so extra data never affects correctness.

### The default Speculator

The default Speculator is composed from two swappable interfaces — a **Generator** and an **Allocator** — so scoring and preemption policy can vary independently. They are composition points inside the default implementation, not controller-facing extensions: the controller depends only on the Speculator contract, and an alternate Speculator need not use or expose this split. The default opens the Generator's candidate stream over the batches, then hands that stream and the path sets to the Allocator.

- **Generator** — yields the queue's candidate paths as one iterator across heads in `BatchStateSpeculating`. *Contract:* every candidate has a Speculating head and is coherent; none repeats or contradicts a resolved fact. Ranking is implementation-defined — the Generator may compute it directly, call an injected scorer extension, or use other injected data — and the score it carries is meaningful only within the run. The Allocator consumes the iterator in the order the Generator yields it and does not interpret the score. *Default:* `bestfirst` returns paths in order of the probability that all their assumptions hold, generating each one lazily from its head's most likely path — the design and the alternatives considered are in [speculation-generator.md](speculation-generator.md).
- **Allocator** — spends the build budget (the queue's cap on concurrent builds) over the iterator. *Contract:* it pulls in order until the budget fills and matches candidates to existing paths by ID, so a pending or building path keeps the slot it already holds rather than starting a second attempt, and a candidate whose path is already terminal in the path sets is skipped rather than rebuilt; pending dispatches are replayed by the controller as described above. Pending, building, and cancelling paths charge the budget (a cancelling build holds CI until terminal), while terminal ones charge none. Cancellation is best-effort, so the Allocator does not spend capacity it merely expects a cancel to release and risk exceeding the hard CI cap. *Default:* the sticky policy fills only free slots and leaves in-flight builds running; a preempting policy cancels in-flight paths below the funded set. Budget is the only rationing lever — there is no ranking-score floor. A build cancelled to make room still charges budget until its cancel reaches terminal and publishes dirty, so the next run funds the released slot — the queue converges over successive ticks rather than oversubscribing in a single pass.

### Extension APIs

Signatures live in code and are not copied here, so they cannot drift. This section says what each contract is for and where to read it.

**Entities** — [`submitqueue/entity/speculation.go`](../../../submitqueue/entity/speculation.go). A `SpeculationPath` is a head batch plus one `PathDependency` per dependency in queue order, each carrying a `DependencyAssumption`: *succeeds*, *fails*, or *ignored*. A `SpeculationPathEntry` is the stored record of one chosen path, keyed by a hash of its content, plus its status and attempt number; it holds no build reference — the execution record has that, keyed by (path ID, attempt) — and no ranking score, which means nothing outside the run that produced it. A `SpeculationPathSet` is one head's chosen paths, live and recently finished, under a single version for compare-and-swap. Every logical path is self-describing, but a store may encode the common head and ordered dependency IDs once per set and keep each path's assumptions positionally — two bits per dependency, or a base-3 code that stays a small integer.

**Speculator** — [`submitqueue/extension/speculation/speculator`](../../../submitqueue/extension/speculation/speculator/README.md). `Speculate` takes one queue snapshot (the batches and their path sets) and returns the build and cancel actions it proposes; a path it wants left alone has no entry. Actions must target Speculating heads. Verdicts stay controller-owned, so there is no merge or fail action.

**Generator and Allocator** — [`generator`](../../../submitqueue/extension/speculation/generator/README.md) and [`allocator`](../../../submitqueue/extension/speculation/allocator/README.md), the two composition points inside the default Speculator. The Generator opens a pull-based stream of candidate paths over the batches; the Allocator spends the build budget over that stream, reconciling it against the path sets. Both abort on a cancelled context. The default Generator's design — its scoring, lazy generation, and the alternatives considered — is in [speculation-generator.md](speculation-generator.md).
