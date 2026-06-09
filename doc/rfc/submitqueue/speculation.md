# Speculation

How SubmitQueue speculates: why it does, the path/tree model, where it sits in the orchestrator pipeline, and the seams it is built from. Speculation is two layers: **decision seams** — enumeration and scoring *describe* the tree of possible bets, selection and prioritization *act* on it — and **limit policies** that decide *how much* to allow at each step, scaling with the build system's resources. A path's score is not fixed at enumeration; it is recomputed as the batch's world changes, so choices always reflect the latest reality.

This document captures the concept and the design decisions.

## Problem: why speculate at all

SubmitQueue lands batches of changes onto a target branch. Batches that touch overlapping targets conflict, so they form a **dependency DAG**: if batch `B` conflicts with an earlier batch `A`, then `B` must land after `A`.

The naive policy serializes the DAG: build `A`, wait for it to pass, merge it, *then* build `B` on the new branch tip, and so on. Every batch waits for all of its predecessors to fully validate and merge before its own build can even start. With multi-minute builds and a deep queue, end-to-end latency grows with queue depth and throughput collapses.

**Speculation removes the wait by betting on the likely outcome.** Instead of waiting for predecessors to merge, the orchestrator *assumes* they will pass and builds a dependent batch now, on top of an assumed-good prefix of those predecessors. If the bet holds — the predecessors pass and merge — the dependent batch has already been validated against the exact tree it will land on, so it merges immediately. Builds for the whole chain run in parallel instead of in series.

The bet can be wrong. If a predecessor fails, every build stacked on top of it is invalid and the orchestrator must **re-speculate**: discard the broken assumption and fall back to a path that survives (for example, build the dependent batch without the failed predecessor in its base). Because each predecessor is an independent "will it pass?" bet, a batch has *many* possible speculation paths. Enumerating them, choosing among them, and rationing them against finite build capacity — that is what this design is about.

## Vocabulary

| Term | Meaning |
|---|---|
| **Batch** | A group of land requests that land together. The unit of speculation. |
| **Dependency DAG** | Conflict graph over batches. `B` depends on `A` ⇒ `B` lands after `A`. |
| **Active dependency** | A dependency batch that is still in flight (not yet in a terminal state). Landed/failed dependencies are no longer active. |
| **Speculation path** | One bet: an ordered **Base** of predecessor batches assumed to pass, plus a **Head** — the batch being verified. Built by applying Base then Head on the target branch and validating. |
| **Speculation tree** | The set of all candidate paths for one batch — its possible bets — each carrying a score and a status. |
| **Score** | A predicted-success number for a path; how good a bet it is. Computed by the scorer and **recomputed as the batch's state changes** — not fixed at enumeration. |
| **Status** | The observed lifecycle state of a path (candidate, building, passed, …). Written only by the controller. |
| **Action** | What the selector asks the controller to do for a path (build, cancel). |
| **Limit** | A "how much" bound produced by a signal-driven policy: the dependency limit, selection limit, and prioritization limit. Scales with build resources (and other signals). |

The Base/Head shape is the key modelling choice: it maps one-to-one onto the build stage, where **Base** becomes the assumed-good changes to apply first and **Head** becomes the changes under validation. A path validates only the targets changed by its **Head** on top of the assumed-good **Base** — the targets changed by the base batches are covered by *their own* paths, so no path re-validates its base.

## Where speculation sits in the pipeline

Speculation is one stage in the orchestrator's queue-driven pipeline (see the [Orchestrator Workflow](workflow.md) for the full picture). It is the hub of two cycles: the build feedback loop `speculate → build → buildsignal → speculate`, and the advance loop `merge → speculate`.

```
  score ──BatchID──▶┌───────────────── speculate ──────────────────┐──BatchID──▶ merge ──▶ conclude
                    │ 0. gate on dependency limit  1. enumerate tree │              │
                    │ 2. persist  3. reconcile status + rescore paths│◀──BatchID────┘
                    │ 4. select   5. enact + write status & score    │  (a merge advances
                    └───┬─────────────────────────────▲─────────────┘   the next batch)
            per path    │ BatchID                     │ build result
                        ▼                             │
                      build ───Build───▶ buildsignal
               (prioritize, then         (poll build status)
                trigger admitted builds)
```

Each speculative path becomes its own build; build results flow back through `buildsignal` into `speculate`, which re-evaluates against the new reality. The controller is a **thin driver**: it gates a batch on the dependency limit, asks the enumerator for the tree structure over the batch's active dependencies, persists it, reconciles each path's status from the latest builds and dependency states, **asks the scorer to recompute each path's score for that new state**, asks the selector which paths to build, then enacts those actions and writes the resulting statuses and scores back to the store. **Prioritization** — rationing the queue's selected builds against its build budget — happens downstream, queue-wide, at the build stage where all of the queue's paths converge. (The pipeline's `score` stage sets the per-*batch* `Batch.Score`; the path scorer inside `speculate` consumes those to score whole *paths*, and reruns as state changes.)

## Limits and decisions

Speculation splits into two layers.

**Decision seams** split into *describing* the tree and *acting* on it. **Enumeration** mechanically lists what futures are possible (the structure); **scoring** predicts how good each is right now (recomputed as state changes). Then a **selector** — the per-batch policy — chooses which of a batch's paths are worth building, and a **prioritizer** — the queue-wide policy — chooses which of all the queue's selected builds actually run right now. Enumeration is deliberately dumb; the intelligence lives in scoring, selection, and prioritization.

**Limit policies** answer *how much*: the **dependency limit** bounds how many in-flight predecessors a batch speculates over, the **selection limit** bounds how many paths a batch builds in parallel, and the **prioritization limit** bounds how many builds the queue runs at once. These are the resource-aware knobs. Their primary input is the build system's available capacity, but they are not restricted to it — a limit may also weigh historical pass rates, cost budgets, time of day, or an experiment toggle. There is no fixed constant and no prescribed static config: a limit is whatever its policy computes from its signals.

The two layers compose by **dependency injection**: a decision seam that is bounded by a limit is constructed with that limit policy and calls it itself. The selector holds the selection limit; the prioritizer holds the prioritization limit. The dependency limit is the exception — it gates whether a batch is eligible to enumerate at all, which is controller orchestration, so the controller holds and applies it (and the enumerator stays pure).

```
  dependency limit ─▶ controller gate ─▶ enumerator.Enumerate(batchID, activeDeps) ─▶ tree structure (status = candidate)
                                                                                         │
       ┌── controller reconciles status, then scorer.Score(tree, deps) each pass ◀───────┘
       ▼
  selector.Select(tree)  ──Build / Cancel per path (capped by its selection limit)──▶  controller enacts,
                                                                                        writes status + score,
                                                                                        dispatches builds
                                                                                             │
  prioritizer.Prioritize(queue's pending builds)  ──admitted subset (capped by its prioritization limit)──▶ builds run
```

### Enumeration

Enumeration is deliberately **dumb**, and purely **structural**: given a batch and its ordered list of active dependencies, it mechanically lists the candidate paths — the Base/Head splits — and nothing else. It does not score, set status, decide what to build, or decide eligibility. It is **pure and deterministic** — the same inputs always yield the same structure — so the controller can re-enumerate freely whenever the batch's active dependency *set* changes, without the enumerator holding state. Keeping enumeration tractable for a wide dependency list is its only real concern.

### Scoring

A path's score is a **prediction** — "how likely is this bet to pay off?" — and predictions must move as evidence arrives. The **scorer** computes each path's score from the current state: the per-batch success probabilities of the path's base batches (`Batch.Score` from the score stage), which of those dependencies have already landed or had their build pass (resolved assumptions raise confidence), and optionally other signals (how long the batch has waited, historical pass rates). Because it is a prediction over live state, the scorer is **re-run on every respeculate**, right after the controller reconciles status — so when a dependency lands, its build passes, or a sibling path fails, the surviving paths' scores are recomputed against the new reality before anything is selected or prioritized. The controller drives *when* to rescore (it is part of reconciliation) and persists the result; the scorer owns the *formula*. The score is the common currency the selector and prioritizer both rank on, so keeping it current is what makes both act on the latest reality.

### Selection

Selection is the per-batch **policy** — the part that decides which of a batch's paths are worth building. Given the tree (with each path's controller-stamped status), the selector returns an **action** per path: which to build and which to drop. Strategies span a spectrum: only the single optimistic path (cheapest — bet on the happy case), every candidate (maximum parallelism), or a top-K subset in between. Because it is re-run on every build signal, a strategy can start narrow — build the optimistic path first — and widen later, committing more paths only once earlier bets resolve.

How *many* paths a batch may build in parallel is not the selector's judgement call but its **selection limit**, a signal-driven policy the selector is constructed with and calls itself. This keeps "which" (the selector's ranking) separate from "how much" (the limit), and lets the limit scale with build resources without touching selector logic. The selector reads the tree and emits actions only; it never reads storage, builds, or scores directly, and it never writes status. It does **not** decide merging — see [Path state](#path-state-status-vs-action).

### Prioritization

Selection is per batch and cannot see across batches, so it cannot ration a shared build budget: if every batch in a queue independently selected generously, their combined demand could swamp CI. **Prioritization** is the queue-wide policy that closes that gap. It sees every selected build across all of the queue's in-flight batches, ranks them (by each build's score, plus any fairness or tie-break policy), and admits only the top few that fit the queue's **prioritization limit** — the queue's concurrent-build budget, another signal-driven, resource-scaled policy the prioritizer is constructed with and calls itself.

Prioritization lives at the **build stage**, where all of the queue's selected paths converge and the build budget is known — not in `speculate`, which is partitioned per batch. The likely implementation is lightweight: each build request carries its path score as a **priority**, and priority-ordered consumption under a concurrency cap makes "top-N by score" emerge naturally; an explicit admit-top-N component is the fallback if preemption or fairness needs more than ordering. Because it is the one place that sees the whole queue, the prioritizer is the queue-wide **enforcer**: selection expresses *desire* per batch, prioritization reconciles that desire against *supply*.

## Path state: status vs action

Each path carries two distinct things with two distinct owners:

- **Status** — the *observed* lifecycle state of a path. Written **only by the controller**, into the speculation tree store, and read by the selector as input.
- **Action** — what the selector wants done next. The **selector's only output**: recomputed on every run, never persisted.

The selector reads status and emits actions; the controller enacts an action, which produces the next status it writes. The selector never writes status; the controller never asks the selector to persist anything.

The controller persists the **entire** tree — every enumerated path together with its current status and its latest score — not just the actionable ones. So each `Select` run reads the up-to-date status *and* freshly recomputed score of *all* paths (including ones already `selected` or `building`) and can return `Cancel` for a path it earlier asked to build. Score, like status, is controller-persisted dynamic state; the difference is only in who computes the value — the controller derives status directly from builds and dependency states, and calls the scorer for the score.

```
  Status (controller-written, persisted)

    candidate ──▶ selected ──▶ building ──▶ passed
        │            │            │      └─▶ failed
        │            └────────────┴────────▶ cancelled
        └─────────────────────────────────▶ cancelled
```

| Transition | Trigger (all written by the controller) |
|---|---|
| → `candidate` | enumerator produced the path; controller persists it |
| `candidate → selected` | selector returned `Build`; controller **sent** the path to the build controller (no `BuildID` yet) |
| `selected → building` | a **build signal** confirms the build is running; controller records `BuildID` |
| `building → passed` / `failed` | build result arrives via `buildsignal` |
| `selected → cancelled` | selector returned `Cancel` before any build started, or the build never started |
| `building → cancelled` | the build was cancelled |
| `candidate → cancelled` | the path's base broke before it was ever sent |

Actions the selector can emit: `Build` (send this path to the build controller) or `Cancel` (drop it; cancel any build in flight). The selector leaves a path as-is by simply omitting it from its decisions. Note there is no merge/finalize action: **merging is the controller's job, not the selector's.** A path becomes mergeable when its build `passed` *and* its base matches what actually landed — that is deterministic, not a policy choice, so the controller finalizes it on its own (the existing `tryFinalize` → `merge` reconciliation). The selector only decides where to spend build resources; the prioritizer decides which of those actually run.

Why `selected` is distinct from `building`: the selector only *sends* a path to the build controller. The build is then subject to prioritization and resources and may not start immediately. So `Build` moves the path to `selected`; speculate does not assert `building` itself — it learns a build actually started only from a build signal, and only then records `building` and the `BuildID`. Between the two, the path is sent but unconfirmed, and the selector treats `selected` as "already sent — don't re-send, but still cancellable." "Base invalid" is not a status — it is one of the *triggers* that sends a path to `cancelled`.

## Dependency limit

A batch can sit deep in a chain of dependencies. The **dependency limit** bounds how many **active** (in-flight, non-terminal) dependencies a batch may speculate over. It is an *eligibility gate*, not a trimming step: a batch becomes eligible to speculate only when its count of active dependencies is at or below the limit; otherwise it waits. Nothing is dropped — as dependencies land, they leave the active set, the count shrinks, and the batch becomes eligible.

The limit applies even to the fully-stacked happy path: on a very long chain, applying every predecessor serially is itself slow, so the limit caps how much of the chain is speculated at once rather than always speculating it in full.

### Gating, by example

Consider a chain `q1 ← q2 ← q3 ← q4`, ordered by arrival, where each batch depends (transitively) on all earlier ones, with a dependency limit of 1:

```
   batch   active deps        eligible at limit=1?     path speculated
   q1      —                  yes (0 active)           []   → q1
   q2      q1                 yes (1 active)           [q1] → q2
   q3      q1, q2             no  (2 active) → waits    —
   q4      q1, q2, q3         no  (3 active) → waits    —
```

When `q1` lands, it leaves the active set. `q3`'s active dependencies shrink to `{q2}` — one active — so `q3` becomes eligible and speculates `[q2] → q3` (with `q1` now part of the real branch tip, not the speculative base). The gate is re-evaluated on every respeculate, so a landing predecessor naturally admits the batches waiting behind it.

### Owned by the controller, decided by a policy

Applying the gate is the **controller's** job: it reconciles each dependency's state, counts the active ones, compares against the limit, and — when eligible — hands the active dependencies to the enumerator in order (the DAG order is reconstructed here; the enumerator receives a ready-ordered list). This keeps the connected-set walk and state reconciliation out of the dumb enumerator.

The limit's *value* is decided by a signal-driven policy, not a fixed constant. It is meant to scale up and down with build resources (and other signals), so a period of CI pressure can shrink how deep the queue speculates. Because the value is dynamic, a change to the limit — not only a landing dependency or a DAG change — can newly admit a waiting batch, so it is one of the triggers the controller re-evaluates on.

## Paths and trees, by example

Consider queue `q` with batches `q1`, `q2`, `q3`, where both `q2` and `q3` depend on `q1` (and not on each other):

```
   Dependency DAG               Speculation tree for q2 (depends on q1)

        q1                        Base    Head   Score*
       /   \                      [q1]    q2     0.27   ← bet: q1 passes, build q2 on it
     q2    q3                     []      q2     0.90   ← fallback: build q2 alone

                              *Scores are illustrative and dynamic; the exact
                               formula is a scorer concern.
```

`q1`, having no predecessors, has a single path `[]→q1`. Each dependent batch has two: build on the assumed-good predecessor, or build alone.

### A bet, and its recovery

The selector runs an optimistic top-1 policy: it returns `Build` for `[q1]→q2` (betting `q1` passes). The controller sends that path to the build controller, so it moves to `selected`; once a build signal confirms the build started, the controller marks it `building`:

```
   q2 tree (q1 still building)               q1 FAILS → q2 tree (after controller reconciles)

   Base   Head  Status      Score           Base   Head  Status      Score
   [q1]   q2    building     0.27            [q1]   q2    cancelled   0.27   ← base broke
   []     q2    candidate    0.90            []     q2    candidate   0.90   ← selector now returns Build
```

- **Bet holds** — `q1` passes and merges: the build of `[q1]→q2` ran against exactly the tree `q2` will land on, so `q2` is mergeable and the controller finalizes it (publishes to `merge`) — no selector action involved. `q1` and `q2` were validated in parallel — the latency win. The now-redundant `[]→q2` fallback is dropped: the selector returns `Cancel` for it and the controller cancels any build still in flight.
- **Bet fails** — `q1` fails: the `[q1]→q2` path's base is broken, so the controller stamps it `cancelled`. Re-running the selector over the updated tree returns `Build` for the surviving `[]→q2` candidate; `q2` still lands, just without the head start.

Re-speculation needs no special undo path: the controller refreshes statuses, and the selector simply re-runs over the updated tree.

## Interfaces

The seams are vendor-agnostic extensions, each in its own package; the exact Go signatures live in the source. All are per-queue: the system hands a `Factory` the queue identity, and the factory builds the seam for that queue — so the queue is bound at construction and never re-passed to a method.

**Decision seams:**

- **Enumerator** (`extension/speculation/enumerator`) — given a batch ID and its ordered active dependencies, returns the batch's speculation tree *structure*: the candidate paths, each a Base/Head split. Pure and deterministic; sets no score and no status.
- **Scorer** (`extension/speculation/scorer`) — given the speculation tree and the current dependency batches, returns each path's predicted-success score. Called by the controller on every respeculate (during reconciliation) so scores track the live state — dependencies landing, dependency builds passing, siblings failing. Owns the score formula; combines the base batches' `Batch.Score` and their resolved/unresolved state (and optionally other signals).
- **Selector** (`extension/speculation/selector`) — given a speculation tree (with each path's controller-stamped status and freshly recomputed score), returns a per-path action (`Build` or `Cancel`) for the paths it chooses to act on. It reads status and score and emits actions only; paths it leaves alone are omitted. It is constructed with its **selection limit** and calls it to cap how many paths it builds in parallel.
- **Prioritizer** (`extension/speculation/prioritizer`) — given the queue's pending build candidates, returns the subset admitted to run, ranked by score plus any fairness policy. It is constructed with its **prioritization limit** and applies it itself. Operates queue-wide, across all of the queue's in-flight batches.

**Limit policies** — each a signal-driven "how much" seam returning a bound from build-resource and other signals:

- **Dependency limit** — the max active dependencies a batch may speculate over. Consulted and applied by the controller as the eligibility gate.
- **Selection limit** — the max paths a batch may build in parallel. Injected into and called by the selector.
- **Prioritization limit** — the max concurrent builds for the queue. Injected into and called by the prioritizer.

The scorer, prioritizer, and the three limit policies are design-level here — the enumerator and selector exist in source (with the enumerator's scoring responsibility slated to move into the scorer); the rest are not yet implemented.

## Design decisions

**Two layers: decisions and limits.** Decision seams (enumerator, scorer, selector, prioritizer) — enumeration and scoring *describe* the tree, selection and prioritization *act* on it; limit policies (dependency, selection, prioritization) decide *how much*. *Why:* the "which" is qualitative policy that is stable, while the "how much" must scale with volatile build resources; separating them lets the resource-aware knobs move independently of the decision logic, and lets each be tested in isolation. *Rejected:* baking counts into each decision seam as constants — it hard-codes a policy that needs to breathe with CI capacity.

**Limits are signal-driven, and resources are the primary but not the only signal.** A limit is whatever its policy computes — from available capacity, and optionally historical pass rates, cost, time, or experiment flags. *Why:* speculation aggression should rise and fall with the build system, and the design should not foreclose other inputs. *Rejected:* a single fixed constant, or a static per-queue config value — neither can react to load.

**Limits are injected into the seam that uses them and called there — never a method parameter.** The selector holds its selection limit; the prioritizer holds its prioritization limit. *Why:* it follows the repo's extension-contract pattern (dependencies injected at the `Factory`), keeps the interfaces limit-free and stable, and keeps "which" and "how much" swappable independently. *Exception:* the dependency limit gates eligibility *before* enumeration and needs active-dependency reconciliation, which is controller orchestration — so the controller holds it, and the enumerator stays pure.

**Status is the controller's; Action is the selector's.** Status is observed reality, written only by the controller into the store; the selector reads it and returns only actions, which the controller enacts and turns into the next status. *Why:* one writer for persisted state keeps the lifecycle coherent and the selector a pure function of its input, exercisable against a literal tree with no store, builds, or scorer. *Rejected:* letting the selector mutate status — it couples policy to storage and gives status two writers.

**The dependency limit is an eligibility gate, not a base trim.** A batch waits until its active-dependency count fits the limit; landed dependencies leave the active set and admit it. *Why:* a batch's base must include every still-in-flight predecessor it conflicts with — you cannot skip one — so the honest control is *when* to speculate, not *which* ancestors to drop. *Rejected:* trimming the oldest ancestors and speculating on a nearest-N subset — it would build a base that omits a live conflicting predecessor.

**Prioritization is cross-batch, at the build stage, and is the queue-wide enforcer.** Selection is per batch and blind to other batches; only the build stage sees all of the queue's selected paths and the build budget. *Why:* rationing a shared budget requires a single vantage point; putting it in per-batch `speculate` would let batches collectively over-commit. *Rejected:* making the selector resource-aware across batches — it cannot see them.

**Scoring is separate from enumeration, and dynamic.** Enumeration produces stable *structure*; the scorer produces a *prediction* that is recomputed on every respeculate. *Why:* a path's odds change as dependencies land, dependency builds pass, and siblings fail — a score frozen at enumeration would misrank the selector's and prioritizer's choices; separating them lets the score refresh in place each pass without re-structuring, and lets the scorer see live state (build outcomes, wait time) that the pure structural enumerator should not. *Rejected:* folding scoring into enumeration (its earlier form) — it froze the prediction and would force a full re-enumerate plus status-merge just to refresh a number.

**Scoring takes dependency `Batch` values; enumeration needs only ordered IDs.** The scorer combines the base batches' `Batch.Score` and their current state, so it takes the dependency `Batch` values. Enumeration only needs the ordered dependency identities to build the Base/Head structure. In both, the head is passed as an ID: its own score is constant across all of its paths, and passing an ID avoids handing a seam the head's full dependency list, which would tempt it to bypass the controller's gate.

**A path is a Base/Head split, not a flat list.** Base is the assumed-good prefix; Head is the single batch under verification. *Why:* it maps one-to-one to the build stage (apply Base, validate Head), lets a build backend cache a shared base prefix, and lets failure be attributed to base vs head. *Rejected:* a flat ordered list — the consumer would have to re-derive which portion is assumed-good.

## Open questions

Named here for context; not settled by this design:

- **Rescoring while queued.** The scorer already recomputes path scores as dependencies land, fail, or have their builds pass. An open extension: should it also decay or boost a batch's standing the longer it waits without a build — feeding wait time in as another scorer signal, or rescoring the per-batch `Batch.Score` upstream? Open which layer owns it.
- **Cross-queue capacity.** Prioritization here is queue-wide. If build capacity is ever shared *across* queues, a further arbitration layer is needed — with its own questions of fairness and starvation across queues, and score comparability between them.
- **Concrete implementations.** The limit policies (single fixed value, resource-tracking, adaptive), enumerators (single-path, exhaustive, top-K by structure), scorers (independent-product of base scores, discounted-by-depth, evidence-weighted), selectors (top-K, optimistic-first, shadow A/B), and prioritizers (priority-ordered consumption vs explicit admit-top-N, with or without preemption).
- **Signal sourcing.** Which signals each limit policy weighs, and how they are surfaced (a capacity feed, historical metrics, config) to the wiring layer that constructs the policies.
