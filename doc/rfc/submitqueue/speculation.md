# Speculation

A merge queue that verifies one change at a time is limited by its slowest build. Speculation removes that limit: it builds a batch early, against a guess at how the conflicting batches ahead of it will resolve, so a valid build is usually ready by the time they do.

Work enters SubmitQueue as **batches** — changes verified and merged together. Two batches **conflict** when they touch the same code, which makes the earlier one a **dependency** of the later. A **path** is one guess at how a batch's dependencies resolve, and the batch it builds is the path's **head**.

On every queue update the **speculate controller** reruns from scratch: it reads the current state, applies the incoming signals, asks a pluggable **Speculator** which paths are worth building within the CI budget, and persists only those. Everything else is recomputed next time, never stored. Merging stays strict: a batch lands only after its dependencies resolve and a matching build has passed.

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
 │ 2 apply  signals — refute paths whose bet a resolved dependency broke,
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

Each path-status transition is written by the controller that observes it — nothing is polled or derived. The speculate run records a path *pending* when it funds it, and sets the *cancelling* intent when it refutes a path (a resolved dependency broke its bet — an included dependency failed, or an excluded one landed) or when a batch is cancelled. The build controller flips *pending → building* when it actually starts CI, so a backed-up build queue leaves the path *pending*. Each run idempotently re-sends the build dispatch for a funded pending path, keyed by (path ID, attempt), until the build controller records it *building* or terminal. The buildsignal consumer records the terminal status — *passed*, *failed*, or *cancelled* — when the build reaches it. The path stores no build ID; the execution record holds it, keyed by (path ID, attempt).

Every write is a compare-and-swap: a writer that loses re-reads on a later run. No run depends on an earlier one, so duplicated or reordered dirty signals are harmless. Any later queue update also reconciles earlier committed state because every run recomputes the whole queue.

### Finalization

Verdicts are controller-owned facts: the Speculator can neither compute nor veto them.

- **Merge (strict).** Each path carries a bet on every dependency — *included* (built on top of), *excluded* (built without), or *dropped* (ignored). Once a path's build has passed and every *included* dependency has merged, the speculate controller moves the head to Merging and hands it to Runway — it waits only on the dependencies it was built on top of, not the head's full dependency list. If that hand-off is lost, the next run re-sends it. The mergesignal controller records Runway's terminal result: success marks the head Succeeded, while failure marks it Failed. The result publishes a single dirty signal — no per-dependent fan-out — and the next run refutes paths whose bet disagrees with the result: excluded bets after success, included bets after failure. The hand-off is idempotent, so Runway reports success without another merge when the change is already present. Down a chain, each head waits for its included predecessors, so a chain lands one at a time.
- **Failure (no viable path).** A batch fails when every possible future has a failed build — no path can pass, so it can never merge.
- **Cancel.** A cancelled batch is driven terminal: its in-flight paths are set *cancelling*, then the batch is marked Cancelled once they stop (see Cancellation).

### Conflict relaxation

Conflict analysis is conservative — it flags any *possible* conflict — so heads carry dependencies that rarely matter and over-serialize. Relaxation lets the Speculator **drop** the weakest: the path tags that dependency *dropped* and ignores it — it neither gates the merge nor refutes the path if it lands. Which to drop is a per-run Speculator policy.

The drop lives on the path, so the path stays self-describing: finalization needs no external relaxed set, and dropped dependencies don't count toward the depth bound (relaxing is what shrinks a head's depth).

Example: `H` conflicts with `B1` and weak `B2`. Drop `B2`, and `H` merges once `B1` lands and its build passes — even if `B2` later lands. Without it, `H` waits on both.

### Bypass large diff

If a batch's passed builds cover *every* way its dependencies could resolve, the outcome is the same either way — so it can merge now, ahead of them. Classic case: a small change stuck behind a slow one is built both with and without it; both pass, and it merges immediately.

The default Speculator covers the whole space only when doing so is cheap enough, and funds the extra candidates within the build budget. The controller merges early only when a passed path exists for every combination of the dependencies — it reads that straight off the path records. If any combination is missing or unbuilt, the head waits normally.

### Cancellation

Cancellation is best-effort: a batch marked *cancelling* may still land if a merge wins the race, so terminal states prevail. A cancel sets the intent; a later run drives it terminal.

Two kinds of cancel, split by owner:

- **Preemption (extension).** The Speculator may propose `Cancel` on an in-flight path whose head is Speculating to free budget for a better candidate — its only cancel power. The controller validates it (never a passed path) and enacts it.
- **Correctness cancels (controller).** Refutation — a resolved dependency breaks a path's bet, so the controller cancels that path — and batch cancellation (below). The Speculator is not consulted.

Batch cancellation (user-initiated) marks the batch *cancelling*: a non-terminal intent that halts new work. The speculate run drives it terminal — it sets its in-flight paths *cancelling*, marks the batch *cancelled* once they stop, recomputes dependent paths against that outcome (those that bet it out keep building; those that bet it in are refuted), and concludes the contained requests.

Cancelling a path sends a cancel (path ID, attempt) to the build controller, which cancels the CI build; that build holds its slot until the cancel reaches terminal.

## Speculator Extension

The one extension. It decides *which paths to build and which running ones to cancel* — nothing else; the controller handles the rest (reconciling facts, cancelling ruled-out paths, verdicts, checking output). A swapped-in Speculator changes which paths run, never whether a batch merges or fails.

**The contract** is `Speculate(batches, pathSets) → []Speculation`:

- **In:**
  - `batches`: every in-flight batch plus finalized batches still referenced as dependencies by an in-flight batch; each carries its dependency list and state.
  - `pathSets`: every materialized path for those batches, whether pending, in flight, or terminal, including recently finished paths retained to prevent duplicate work.
- **Out:** a list of build/cancel actions whose heads are in `BatchStateSpeculating`; batches in every other state provide facts but are never action targets.
- Budget, depth bound, and clock are injected at construction. An impl may read extra data (also injected); the controller checks the output, so extra data never affects correctness.

### The default Speculator

The default Speculator is composed from two swappable interfaces — a **Generator** and an **Allocator** — so scoring and preemption policy can vary independently. They are composition points inside the default implementation, not controller-facing extensions: the controller depends only on the Speculator contract, and an alternate Speculator need not use or expose this split. The default opens the Generator's candidate stream over the batches and their path sets, then hands that stream and the path sets to the Allocator.

- **Generator** — yields the queue's candidate paths as one iterator, best-first across heads in `BatchStateSpeculating`. *Contract:* every candidate has a Speculating head, is coherent, and is within the depth bound (the count of unresolved dependencies a head's paths range over); none repeats or contradicts a resolved fact; a head past the bound is skipped until its dependencies resolve. Ranking is implementation-owned: the Generator may compute it directly, call an injected scorer extension, or use other injected data. It carries the resulting ranking score only within the run and skips paths already terminal in the path sets.
- **Allocator** — spends the build budget (the queue's cap on concurrent builds) over the iterator. *Contract:* it pulls in order until the budget fills and matches candidates to existing paths by ID, so a pending or building path remains funded rather than starting a new attempt; pending dispatches are replayed by the controller as described above. Pending, building, and cancelling paths charge the budget (a cancelling build holds CI until terminal), while terminal ones charge none. Cancellation is best-effort, so the Allocator does not spend capacity it merely expects a cancel to release and risk exceeding the hard CI cap. *Default:* the sticky policy fills only free slots and leaves in-flight builds running; a preempting policy cancels in-flight paths below the funded set. Budget is the only rationing lever — there is no ranking-score floor. A build cancelled to make room still charges budget until its cancel reaches terminal and publishes dirty, so the next run funds the released slot — the queue converges over successive ticks rather than oversubscribing in a single pass.

### Extension APIs

```go
// SpeculationPathEntry is the stored record of one chosen path, keyed by a hash
// of its content. It holds no build reference (that is the execution record's)
// and no ranking score (a ranking score is meaningful only within a single run).
type SpeculationPathEntry struct {
	ID        string                // primary key; hash of the path's content (head batch ID + its bets)
	Path      SpeculationPath       // head batch ID + one bet per dependency, in queue order
	Status    SpeculationPathStatus // pending | building | passed | failed | cancelling | cancelled (current attempt)
	Attempt   int                   // build attempt number, >= 1; builds and messages key on (ID, Attempt)
	Version   int                   // for compare-and-swap writes
	CreatedAt int64                 // ms
	UpdatedAt int64                 // ms
}

// SpeculationPath identifies a head batch and its ordered dependency bets; every
// dependency appears exactly once, so the logical path is self-describing.
type SpeculationPath struct {
	HeadBatchID string          // ID of the batch being built
	Bets        []DependencyBet // one bet per dependency of HeadBatchID, in queue order
}

// DependencyBet is the path's bet on one dependency.
type DependencyBet struct {
	BatchID string            // dependency batch ID
	Bet     DependencyBetType // included | excluded | dropped
}

// DependencyBetType is how a path treats one dependency.
type DependencyBetType int

const (
	BetUnknown  DependencyBetType = 0 // zero-value sentinel; never valid
	BetIncluded DependencyBetType = 1 // bet it lands; head built on top of it, refuted if it fails
	BetExcluded DependencyBetType = 2 // bet it does not land; refuted if it lands
	BetDropped  DependencyBetType = 3 // dropped by relaxation; landing or failing never affects the path
)

// SpeculationPathSet is one head's chosen paths — live and recently finished —
// under a single version. Finished entries linger briefly so a re-run cannot
// collide with an old build; live heads are listed via the batch store's
// by-state query. Every logical path is self-describing, but a store may encode
// the common head and ordered dependency IDs once per set and store each path's
// bets positionally — two bits per dependency, or a base-3 code the depth bound
// keeps to a small integer.
type SpeculationPathSet struct {
	BatchID string                 // primary key; the head batch
	Paths   []SpeculationPathEntry // the head's chosen paths
	Version int                    // for compare-and-swap writes
}
```

```go
// Speculator selects build and preemption actions from one queue snapshot.
// batches contains every in-flight batch plus finalized batches still referenced
// by an in-flight batch. pathSets contains every materialized path for those
// batches, terminal or not. Returned actions must target Speculating heads;
// finalization remains controller-owned.
type Speculator interface {
	Speculate(ctx context.Context, batches []entity.Batch, pathSets []entity.SpeculationPathSet) ([]Speculation, error)
}

// Speculation is one proposed action on one path; a kept path has no entry.
type Speculation struct {
	Path   entity.SpeculationPath // the path acted on; its ID hashes head batch ID + its bets
	Action PathAction             // Build | Cancel
}

// The only two actions the Speculator may propose. No Merge or Fail —
// verdicts are the controller's.
type PathAction int

const (
	PathActionUnknown PathAction = 0 // zero-value sentinel; never valid, rejected by the controller's check step
	Build             PathAction = 1 // start (or resurrect) a build for the path
	Cancel            PathAction = 2 // preempt this in-flight path (refutation cancels are the controller's)
)
```

```go
// Generator — the queue's candidate stream. The consumer pulls candidates
// lazily; the producer computes only what is pulled.
type Generator interface {
	// Open starts the queue's candidate stream, best-first across Speculating heads.
	Open(ctx context.Context, batches []entity.Batch, pathSets []entity.SpeculationPathSet) (PathIterator, error)
}

type PathIterator interface {
	// Next yields the next-best candidate across Speculating heads; ok = false means the
	// queue's coherent, depth-capped space is exhausted. Candidates descend in
	// ranking score, never repeat, and never contradict a resolved fact.
	Next(ctx context.Context) (c CandidatePath, ok bool, err error)
}

// CandidatePath is a path plus its transient ranking score for this run.
type CandidatePath struct {
	Path         entity.SpeculationPath // head batch ID + one bet per dependency
	RankingScore float64                // transient ordering score; never stored because rankings go stale across runs
}
```

```go
// Allocator — spends the build budget over the iterator, matching in-flight
// paths to the funded set by ID. Budget and clock are injected at construction.
type Allocator interface {
	Allocate(ctx context.Context, pathSets []entity.SpeculationPathSet, iter PathIterator) ([]Speculation, error)
}
```
