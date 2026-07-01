# Speculation

How SubmitQueue speculates: why it does, the path/tree model, where it sits in the orchestrator pipeline, and the two pluggable seams it is built from — **speculation-tree enumeration** and **path selection**.

This document captures the concept and the design decisions.

## Problem: why speculate at all

SubmitQueue lands batches of changes onto a target branch. Batches that touch overlapping targets conflict, so they form a **dependency DAG**: if batch `B` conflicts with an earlier batch `A`, then `B` must land after `A`.

The naive policy serializes the DAG: build `A`, wait for it to pass, merge it, *then* build `B` on the new branch tip, and so on. Every batch waits for all of its predecessors to fully validate and merge before its own build can even start. With multi-minute builds and a deep queue, end-to-end latency grows with queue depth and throughput collapses.

**Speculation removes the wait by betting on the likely outcome.** Instead of waiting for predecessors to merge, the orchestrator *assumes* they will pass and builds a dependent batch now, on top of an assumed-good prefix of those predecessors. If the bet holds — the predecessors pass and merge — the dependent batch has already been validated against the exact tree it will land on, so it merges immediately. Builds for the whole chain run in parallel instead of in series.

The bet can be wrong. If a predecessor fails, every build stacked on top of it is invalid and the orchestrator must **re-speculate**: discard the broken assumption and fall back to a path that survives (for example, build the dependent batch without the failed predecessor in its base). Because each predecessor is an independent "will it pass?" bet, a batch has *many* possible speculation paths. Enumerating and choosing among them — under finite build capacity — is what this design is about.

## Vocabulary

| Term | Meaning |
|---|---|
| **Batch** | A group of land requests that land together. The unit of speculation. |
| **Dependency DAG** | Conflict graph over batches. `B` depends on `A` ⇒ `B` lands after `A`. |
| **Connected set** | The batches reachable through dependency/dependent edges from a given batch. |
| **Speculation path** | One bet: an ordered **Base** of predecessor batches assumed to pass, plus a **Head** — the batch being verified. Built by applying Base then Head on the target branch and validating. |
| **Speculation tree** | The set of all candidate paths for one batch — its possible bets — each carrying a score and a status. |
| **Speculation depth** | How far back along the dependency chain speculation reaches. The controller trims the dependency list to this bound before enumeration. |
| **Score** | A predicted-success number for a path; how good a bet it is. Set by the enumerator. |
| **Status** | The observed lifecycle state of a path (candidate, building, passed, …). Written only by the controller. |
| **Action** | What the selector asks the controller to do for a path (build, cancel). The selector's only output. |

The Base/Head shape is the key modelling choice: it maps one-to-one onto the build stage, where **Base** becomes the assumed-good changes to apply first and **Head** becomes the changes under validation. A path validates only the targets changed by its **Head** on top of the assumed-good **Base** — the targets changed by the base batches are covered by *their own* paths, so no path re-validates its base.

## Where speculation sits in the pipeline

Speculation is one stage in the orchestrator's queue-driven pipeline (see the [Orchestrator Workflow](workflow.md) for the full picture). It is the hub of two cycles: the build feedback loop `speculate → build → buildsignal → speculate`, and the advance loop `merge → speculate`.

```
  score ──BatchID──▶┌──────────────── speculate ─────────────────┐──BatchID──▶ merge ──▶ conclude
                    │  1. enumerate tree   2. persist tree        │              │
                    │  3. select actions   4. enact + write status│◀──BatchID────┘
                    └───┬────────────────────────────▲────────────┘  (a merge advances
            per path    │ BatchID                    │ build result   the next batch)
                        ▼                            │
                      build ───Build───▶ buildsignal
                  (trigger build)       (poll build status)
```

Each speculative path becomes its own build; build results flow back through `buildsignal` into `speculate`, which re-evaluates against the new reality. The controller is a **thin driver**: it trims the dependency set by speculation depth, asks the enumerator for the tree, persists it, reconciles each path's status from the latest builds and dependency states, asks the selector for actions, then enacts those actions (dispatching builds, cancelling, advancing to merge) and writes the resulting statuses back to the store.

## The two seams

Speculation splits into two concerns with a clean handoff, driven in order by the controller: an **enumerator** mechanically lists *what futures are possible*; the controller **persists** that tree and keeps each path's **status** current; then a **selector** — the policy — reads that status and decides *what to do with each path right now*. Enumeration is deliberately dumb; the intelligence lives in selection.

```
  enumerator.Enumerate(batchID, deps)  ──paths + scores──▶  controller persists (status = candidate)
                                                                    │
       ┌──── controller stamps status from builds & deps  ◀─────────┘
       ▼
  selector.Select(tree)  ──decisions: Build / Cancel per path──▶  controller enacts,
                                                                          writes new status to store
```

### Enumeration

Enumeration is deliberately **dumb**. Given a batch and a list of dependency batches, it mechanically lists the candidate paths and attaches a score to each. It is **pure and deterministic** — the same inputs always yield the same tree — so the controller can re-enumerate freely whenever the DAG changes, without the enumerator holding state. It does **not** decide what to build, it does **not** set status, and it does **not** decide how deep to speculate: it builds paths from exactly the dependency list it is handed (see [Speculation depth](#speculation-depth)). Keeping enumeration tractable for a very wide dependency list is its only real concern.

Scores ride in on the inputs: each dependency is passed as a full `Batch`, which already carries the per-batch success probability (`Batch.Score`) computed by the score stage. The enumerator combines the scores of a path's base batches into the path's score; the combination formula is the enumerator's concern. No separate scoring backend or injected probability source is needed.

### Selection

Selection is the **policy** — the part that decides how aggressively to spend build resources. Given the tree (with each path's controller-stamped status), the selector returns an **action** per path: which to build and which to drop. Strategies span a spectrum: only the single optimistic path (cheapest — bet on the happy case), every candidate (maximum parallelism, maximum build cost), or a top-K / budget-bounded subset in between. Because it is re-run on every build signal, a strategy can start narrow — build the optimistic path first — and widen later, committing more paths only once earlier bets resolve. It does **not** decide merging — see [Path state](#path-state-status-vs-action).

The **tree is its complete input**: the controller folds every external fact (a build that passed, a dependency that failed) into each path's status *before* `Select` runs, so the selector never reads storage, builds, or scores directly. This keeps it a pure, deterministic policy, testable against a literal tree. Policy knobs (a top-K cap, a build budget, an experiment toggle) live in the implementation's construction, not the call — and so does capacity-awareness: the `Select` contract sees only the tree, but a concrete selector that wants to throttle to CI capacity takes those signals as dependencies injected at its `Factory`. Arbitrating build spend *across* batches stays the build stage's job (see [Deferred](#deferred)).

## Path state: status vs action

Each path carries two distinct things with two distinct owners:

- **Status** — the *observed* lifecycle state of a path. Written **only by the controller**, into the speculation tree store, and read by the selector as input.
- **Action** — what the selector wants done next. The **selector's only output**: recomputed on every run, never persisted.

The selector reads status and emits actions; the controller enacts an action, which produces the next status it writes. The selector never writes status; the controller never asks the selector to persist anything.

The controller persists the **entire** tree — every enumerated path together with its current status — not just the actionable ones. So each `Select` run reads the up-to-date status of *all* paths (including ones already `selected` or `building`) and can return `Cancel` for a path it earlier asked to build.

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

Actions the selector can emit: `Build` (send this path to the build controller) or `Cancel` (drop it; cancel any build in flight). The selector leaves a path as-is by simply omitting it from its decisions. Note there is no merge/finalize action: **merging is the controller's job, not the selector's.** A path becomes mergeable when its build `passed` *and* its base matches what actually landed — that is deterministic, not a policy choice, so the controller finalizes it on its own (the existing `tryFinalize` → `merge` reconciliation). The selector only decides where to spend build resources.

Why `selected` is distinct from `building`: the selector only *sends* a path to the build controller, which triggers a build **subject to resources** and may not start everything at once. So `Build` moves the path to `selected`; speculate does not assert `building` itself — it learns a build actually started only from a build signal, and only then records `building` and the `BuildID`. Between the two, the path is sent but unconfirmed, and the selector treats `selected` as "already sent — don't re-send, but still cancellable." "Base invalid" is not a status — it is one of the *triggers* that sends a path to `cancelled`.

## Speculation depth

A batch can sit deep in a **connected set** of dependencies. Speculating across the entire set is rarely worth it: the deeper a path's base, the less likely the whole prefix holds, and the more build resources each path burns. **Speculation depth** bounds how far back along the dependency chain speculation reaches. The bound applies even to the fully-stacked happy path: on a very long chain, applying every predecessor serially is itself slow, so depth can cap how much of the happy path is speculated rather than always speculating it in full.

Depth is the **controller's** responsibility, not the enumerator's. The controller walks the connected set and trims the dependency list to those within the depth bound *before* handing it to the enumerator. The enumerator then enumerates over exactly that limited set, unaware any trimming happened — which is why enumeration can stay dumb. Depth is owned in one place, but it is **not a single fixed constant**: it is configured per queue (resolved from `queueconfig`) and may be adaptive — varying with CI pressure or other signals. How an adaptive value is computed is left open (a dedicated mechanism/extension, or possibly folded into enumeration); see [Deferred](#deferred).

## Paths and trees, by example

Consider queue `q` with batches `q1`, `q2`, `q3`, where both `q2` and `q3` depend on `q1` (and not on each other):

```
   Dependency DAG               Speculation tree for q2 (depends on q1)

        q1                        Base    Head   Score*
       /   \                      [q1]    q2     0.27   ← bet: q1 passes, build q2 on it
     q2    q3                     []      q2     0.90   ← fallback: build q2 alone

                              *Scores are illustrative; the exact formula is an
                               enumerator concern.
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

The two seams are vendor-agnostic extensions, each in its own package; the exact Go signatures live in the source.

**Enumerator** (`extension/speculation/enumerator`) — given a batch ID and its ordered dependency batches, returns the batch's speculation tree: the candidate paths, each a Base/Head split with a predicted success score (derived from the dependency batches' own scores). Pure and deterministic; sets no status.

**Selector** (`extension/speculation/selector`) — given a speculation tree (with each path's controller-stamped status), returns a per-path decision — the action (`Build` or `Cancel`) for the paths it chooses to act on. It reads status and emits actions only; it never writes status, and paths it leaves alone are simply omitted.

## Design decisions

**Two seams, not one strategy.** A dumb enumerator lists and scores the possibilities; a separate selector — the policy — chooses what to do with each. *Rejected:* a single interface that both enumerates and picks. It conflates "what is possible" with "what we do this instant," forces the enumeration to re-run every time build reality shifts, and buries the one part worth tuning (the selection policy) inside the mechanical part. Splitting lets enumeration be pure and cacheable while the policy re-runs cheaply on every build signal.

**Status is the controller's; Action is the selector's.** Status is observed reality, written only by the controller into the store; the selector reads it and returns only actions, which the controller enacts and turns into the next status. *Why:* one writer for persisted state keeps the lifecycle coherent and the selector a pure function of its input; the selector can be exercised against a literal tree with no store, no builds, no scorer. *Rejected:* letting the selector mutate status in place — it couples policy to storage, gives status two writers, and makes the policy non-deterministic and hard to test.

**Enumeration takes dependency `Batch` values, not IDs.** *Why:* each `Batch` already carries `Score` (its success probability from the score stage), so the enumerator scores paths from its inputs alone — no injected probability source and no `scorer` import. Tests just set `.Score` on literal batches. The head is passed as an ID, not a `Batch`: its score is constant across all of its own paths (so it can't change ranking within the tree), and passing it as an ID avoids handing the enumerator the head's full, untrimmed `Dependencies` field, which would tempt it to bypass the controller's depth trimming.

**Speculation depth lives in the controller, not the enumerator.** The controller walks the connected set, trims the dependency list to `speculationDepth`, and hands the trimmed batches to `Enumerate`; enumerators enumerate whatever they are given. *Why:* depth is one knob that every enumerator must honor — putting it in the controller keeps enumerators dumb and stops each from re-implementing (and drifting on) the limit, and keeps the connected-set walk out of a component that should only see a flat dependency list. *Rejected:* a depth parameter on `Enumerate` — it spreads the policy across every implementation and couples the enumerator to graph traversal it should not know about.

**A path is a Base/Head split, not a flat list.** Base is the assumed-good prefix; Head is the single batch under verification. *Why:* it maps one-to-one to the build stage (apply Base, validate Head), lets a build backend cache or short-circuit a shared base prefix across stacked speculations, and lets failure be attributed to base vs head. *Rejected:* a flat ordered list of batch IDs — the consumer would have to re-derive which portion is assumed-good versus under-test, discarding the one structural fact the orchestrator already knows.

## Deferred

Named here for context; not part of this design:

- Concrete enumerators (single-path, exhaustive power set, probability-ranked top-K) and the per-path score-combination formula.
- Concrete selectors (top-K, budget cap, shadow A/B for safe rollout).
- **Construction & per-queue config.** Per the repo's extension contract ([extension-contract.md](extension-contract.md)), the system hands a `Factory` only the queue identity (`Config{QueueName}`); all behavioral config — selector top-K, build budget, and the like — is injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. The seam is the per-extension `Factory.For(Config) (Enumerator, error)` / `(Selector, error)`, matching `conflict.Analyzer`. Speculation `Depth` is controller-applied (the controller trims `deps` before calling `Enumerate`) and is likewise resolved from `queueconfig`, not threaded through the factory. **Deferred:** the concrete factories and impls, and how each per-queue knob is surfaced from `queueconfig` to the wiring/controller.
- **Cross-batch build scheduling (admission control).** The selector decides build spend *per batch*; nothing yet arbitrates across batches when their combined demand exceeds build capacity. A third concern — a global scheduler — should rank all selected paths system-wide by score and admit only the top few that fit available capacity. This belongs at the **build stage** (where all paths converge and capacity is known), not in `speculate` (which is partitioned per batch and cannot see across batches). The likely shape is the path score riding on the build message as a **priority** plus a concurrency cap, so "top-X by score" emerges from priority-ordered consumption (requires queue priority support); an explicit scheduler component is the fallback if preemption or cross-queue fairness is needed. Open semantics to settle later: starvation/fairness across queues, score comparability across batches, the capacity signal source, and whether to preempt running builds.
- Wiring into the `speculate` controller and example server: the connected-set walk and `speculationDepth` trimming, the enumerate → persist → reconcile-status → select → enact loop, the path↔build link via `BuildID`, and the controller-side finalize (a `passed` path whose base landed → publish to `merge`).
