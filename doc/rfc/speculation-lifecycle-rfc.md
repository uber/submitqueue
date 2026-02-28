# RFC: Speculation Lifecycle and Reconciliation

## Metadata

| Field | Value                            |
|-------|----------------------------------|
| **Author** | Preetam Dwived<preetam@uber.com> |
| **Status** | Draft                            |
| **Created** | 2026-02-27                       |
| **Updated** | 2026-02-27                       |

## Summary

End-to-end lifecycle management for speculative builds in SubmitQueue. A unified reconciliation model reacts to three signal types — batch scoring, build status changes, and merge outcomes — to maintain, prune, and re-speculate the speculation tree for each batch. All signals funnel through a single speculate topic for per-queue serialization, enabling correct DAG-wide state transitions without distributed locks.

## Background

### Motivation

SubmitQueue schedules speculative builds for batches based on their position in a dependency graph. Today, the speculate controller generates a static tree of speculation paths at batch creation time and hands them off to the build manager. But speculation is not a one-shot operation — it must react to signals throughout a batch's lifecycle:

- A build completes (pass or fail) for one of the speculation paths
- A predecessor batch merges successfully, collapsing a dimension in the tree
- A predecessor batch fails to merge, invalidating paths that assumed its success

Without lifecycle management, the system cannot:
1. Cancel builds for impossible futures (wasting CI resources)
2. Land batches out of order when all their futures are confirmed
3. Recover from upstream failures by re-speculating cancelled paths
4. Prune and rebuild trees as the DAG evolves

### Current State

The Strategy interface and path generation algorithms (TopK, Exhaustive, Shadow) are implemented. The speculate controller generates a `SpeculationTree` at batch creation time and publishes to the build topic. There is no mechanism to update the tree after creation.

```
Current flow (one-shot):

  batch created ──→ batch scorer ──→ speculate controller ──→ generate tree ──→ build
```

### Desired State

A reconciliation loop where the Speculation Controller continuously maintains speculation trees in response to signals from the Build Signal Controller and Merge Controller, in addition to initial batch scoring:

```
Desired flow (continuous):

  batch scored ──────────┐
  build status changed ──┼──→ speculate topic ──→ reconciler ──→ tree updates
  merge completed ───────┘                                    ──→ build actions
                                                              ──→ merge actions
                                                              ──→ finalize
```

## Requirements

### Functional Requirements

1. **Unified Reconciliation** — All three signal types (batch scored, build status changed, merge completed) flow through the same reconciliation path
2. **Tree Persistence** — Speculation trees are stored and updated, not regenerated from scratch each time
3. **Action State Machine** — Each speculation path has an action (schedule, cancel) that evolves based on signals
4. **Pruning** — Cancel paths that represent impossible futures (predecessor build failed)
5. **Collapsing** — Remove confirmed dimensions from paths (predecessor merged successfully)
6. **Re-Speculation** — Bring back cancelled paths when the assumption that caused cancellation is invalidated
7. **Out-of-Order Landing** — A batch may land before its predecessors if all its speculation paths have passing builds
8. **Connected Set Awareness** — Reconciliation operates on the set of batches reachable from the signal source via dependencies and dependents

### Non-Functional Requirements

1. **Per-Queue Serialization** — All reconciliation for a given queue is serialized to avoid concurrent tree mutations
2. **Idempotency** — Processing the same signal twice produces the same result
3. **Observability** — Metrics for tree operations (prune, collapse, re-speculate) and path action transitions
4. **Auditability** — Cancelled paths are retained (not deleted) for debugging and re-speculation

### Non-Goals

1. **Cross-queue speculation** — Batches in different queues are independent; no cross-queue coordination
2. **Real-time scoring updates** — Scores are computed at generation time and optionally refreshed; no continuous scoring stream
3. **Manual intervention UI** — No admin interface for manually manipulating speculation trees (future work)

## Design Overview

### Pipeline Architecture

The orchestrator pipeline is a cycle of controllers connected by queue topics. The Speculation Controller is the central reconciliation hub — it receives signals from upstream (new batches), downstream (build results), and lateral (merge outcomes) controllers.

```
  ┌──────────────┐     ┌───────────────┐     ┌─────────────────────┐
  │    Batch     │     │  BatchScorer  │     │    Speculation      │
  │  Controller  │────→│  Controller   │────→│    Controller       │
  │              │     │              │     │    (Reconciler)     │
  │ Creates      │     │ Scores batch, │     │                     │
  │ batches      │     │ queues for    │     │ Generates/updates   │
  └──────────────┘     │ speculation   │     │ speculation trees   │
                       └───────────────┘     └──┬──────────┬───────┘
                                                │          │
                                    ┌───────────┘          └──────────┐
                                    ▼                                 ▼
                             ┌──────────────┐              ┌──────────────┐
                             │    Build     │              │    Merge     │
                             │  Controller  │              │  Controller  │
                             │              │              │              │
                             │ Schedules/   │              │ Merges batch │
                             │ cancels CI   │              │ into target  │
                             │ builds       │              │ branch       │
                             └──────┬───────┘              └──────┬───────┘
                                    │                             │
                                    ▼                             │
                             ┌──────────────┐                     │
                             │ Build Signal │                     │
                             │  Controller  │                     │
                             │              │                     │
                             │ Monitors     │                     │
                             │ build status,│                     │
                             │ extends      │                     │
                             │ timeouts     │                     │
                             └──────┬───────┘                     │
                                    │                             │
                                    │  build status changed       │  merge outcome
                                    │                             │
                                    └──────────┐    ┌─────────────┘
                                               ▼    ▼
                                    ┌─────────────────────┐
                                    │    Speculation      │
                                    │    Controller       │    ◄── re-speculation
                                    │    (Reconciler)     │        loop
                                    └──────────┬──────────┘
                                               │
                                               ▼
                                    ┌─────────────────────┐
                                    │  Request Finalizer  │
                                    │                     │
                                    │ Updates request     │
                                    │ state for batches   │
                                    │ that landed/failed  │
                                    └─────────────────────┘
```

### Controller Responsibilities

| Controller | Consumes From | Produces To | Responsibility |
|------------|--------------|-------------|----------------|
| **Batch Controller** | request topic | batch-scored topic | Creates batches from incoming requests |
| **BatchScorer Controller** | batch-scored topic | speculate topic | Scores batch (build probability), queues for speculation |
| **Speculation Controller** | speculate topic | build topic, merge topic | Generates/reconciles speculation trees; decides build actions and merge readiness |
| **Build Controller** | build topic | build-signal topic | Schedules or cancels CI builds; publishes BuildID for monitoring |
| **Build Signal Controller** | build-signal topic | speculate topic | Polls build status, extends visibility timeout while running; publishes back to speculate when status changes |
| **Merge Controller** | merge topic | speculate topic, finalize topic | Executes the merge; publishes outcome to speculate (for re-speculation) and finalize (to update request state) |
| **Request Finalizer** | finalize topic | — | Updates request state (landed, failed) for all requests contained in the batch |

### Signal Flow

The Speculation Controller is the reconciliation hub. It receives three types of signals, all arriving on the **speculate topic** with `batch.Queue` as the partition key:

```
                    ┌───────────────────────────────────────────────┐
                    │             Speculate Topic                   │
                    │        (partition key = batch.Queue)          │
                    │                                               │
  batch_scored ────→│                                               │
                    │                                               │────→ Speculation
  build_status ────→│   Per-queue ordered consumption guarantees    │      Controller
   _changed         │   serialization without distributed locks    │
                    │                                               │
  merge_completed──→│                                               │
                    └───────────────────────────────────────────────┘
```

This single-topic design guarantees that all signals for batches in the same queue are processed by the same consumer partition, in order — achieving per-queue serialization without distributed locks.

### Signal Types

Three events trigger reconciliation:

| Signal | Source | Payload | When |
|--------|--------|---------|------|
| `batch_scored` | BatchScorer Controller | Batch ID, Queue, Dependencies, Score | A new batch is scored and ready for speculation |
| `build_status_changed` | Build Signal Controller | Build ID, Batch ID, Path, Status (passed/failed) | A speculative build's status changes |
| `merge_completed` | Merge Controller | Batch ID, Outcome (success/failure) | A batch merge attempt completes |

Each signal carries enough context for the reconciler to load the affected connected set and recompute desired state.

### Signal Message Envelope

All three signal types share a common envelope published to the speculate topic:

```
SpeculateSignal:
  Type:       "batch_scored" | "build_status_changed" | "merge_completed"
  Queue:      string          (used as partition key)
  BatchID:    string          (the batch that triggered the signal)
  Score:      float32         (only for batch_scored)
  BuildID:    string          (only for build_status_changed)
  BuildPath:  SpeculationPath (only for build_status_changed)
  Status:     string          (build status or merge outcome)
  Timestamp:  int64           (millis)
```

### Connected Set

When a signal arrives for batch B, the reconciler must update not just B's tree but the trees of all batches affected by B's state change. The **connected set** is the set of batches reachable from B by traversing both dependency and dependent edges.

```
Example DAG:

  B1 ◄─── B2 ◄─── B4
   ▲
   └───── B3

Signal: B1 merge_completed (success)
Connected set: {B1, B2, B3, B4}

After B1 merges:

  B2 ◄─── B4        (B1 removed from DAG)
  B3                 (disconnected — independent now)

Signal: B2 build_status_changed
Connected set: {B2, B4}   (B3 is no longer connected)
```

As batches merge and leave the DAG, the connected set can fragment into independent subgraphs. Each subgraph is reconciled independently.

### Speculation Tree Lifecycle

A speculation tree goes through a series of transformations as signals arrive. The tree is a complete set of possible futures — paths are never deleted, only have their action updated.

#### Initial State (batch_scored)

When batch B2 arrives with its score and dependency on B1, the strategy generates all relevant paths:

```
B2's Speculation Tree:
  ┌─────────────────────────────────────────────────────┐
  │ Path              │ Action    │ Score │ Description  │
  ├───────────────────┼───────────┼───────┼──────────────┤
  │ [B2]              │ schedule  │ 0.9   │ B2 alone     │
  │ [B1, B2]          │ schedule  │ 0.3   │ B2 after B1  │
  └─────────────────────────────────────────────────────┘

Both paths are scheduled — the build manager creates builds for each.
```

#### Prune (build fails)

When B1's build on a path fails, any path in B2's tree that depends on B1's success on that path becomes impossible:

```
Signal: B1's build for path [B1] failed

B1's Tree:
  ┌──────────────────────────────────────────────────────┐
  │ Path              │ Action    │ Score │ Description   │
  ├───────────────────┼───────────┼───────┼───────────────┤
  │ [B1]              │ cancel    │ 0.9   │ build failed  │
  └──────────────────────────────────────────────────────┘

B2's Tree (no change — B1's failure on [B1] doesn't affect B2):
  ┌──────────────────────────────────────────────────────┐
  │ Path              │ Action    │ Score │ Description   │
  ├───────────────────┼───────────┼───────┼───────────────┤
  │ [B2]              │ schedule  │ 0.9   │ still valid   │
  │ [B1, B2]          │ schedule  │ 0.3   │ still valid   │
  └──────────────────────────────────────────────────────┘

Note: B2's path [B1, B2] is still valid because it speculates on a
future where B1 succeeds on the [B1, B2] path, not the [B1] path.
```

#### Collapse (predecessor merges)

When B1 merges successfully, the B1 dimension is confirmed. Paths that included B1 are now the "real" future, and paths that excluded B1 are impossible:

```
Signal: B1 merge_completed (success)

B2's Tree after collapse:
  ┌──────────────────────────────────────────────────────┐
  │ Path              │ Action    │ Score │ Description   │
  ├───────────────────┼───────────┼───────┼───────────────┤
  │ [B2]              │ cancel    │ 0.9   │ B1 merged,    │
  │                   │           │       │ head moved    │
  │ [B1, B2]          │ schedule  │ 0.3   │ this is now   │
  │                   │           │       │ the real path │
  └──────────────────────────────────────────────────────┘

The path [B2] assumed B1 was NOT in the merge base — but B1 did merge,
so [B2] alone is no longer a valid future. [B1, B2] correctly predicted
B1's presence and remains active.
```

#### Re-Speculate (upstream failure reversal)

When B1 was expected to merge but fails, paths that were cancelled under the assumption of B1's success need to come back:

```
Signal: B1 merge_completed (failure)

B2's Tree before:
  ┌──────────────────────────────────────────────────────┐
  │ Path              │ Action    │ Score │               │
  ├───────────────────┼───────────┼───────┼───────────────┤
  │ [B2]              │ cancel    │ 0.9   │ was pruned    │
  │ [B1, B2]          │ schedule  │ 0.3   │ active        │
  └──────────────────────────────────────────────────────┘

B2's Tree after re-speculation:
  ┌──────────────────────────────────────────────────────┐
  │ Path              │ Action    │ Score │               │
  ├───────────────────┼───────────┼───────┼───────────────┤
  │ [B2]              │ schedule  │ 0.9   │ restored!     │
  │ [B1, B2]          │ schedule  │ 0.3   │ still active  │
  └──────────────────────────────────────────────────────┘

B1's merge failure means B1 is back to uncertain. Both futures are
possible again, so [B2] is re-scheduled.
```

### Reconciliation Algorithm

The reconciler is a pure function: given the current trees and batch states, it computes the desired action for every path. This is not a set of imperative rules — it is a declarative computation that can be re-run at any time.

```
Reconcile(signal, connected_set, trees, batch_states):

  1. Load the connected set of batches reachable from signal.BatchID
  2. For each batch B in the connected set:
     a. Load B's current SpeculationTree
     b. For each path P in the tree:
        - Compute desired action based on batch states of all
          batches in P.Base
        - If desired action differs from current → record transition
     c. Persist updated tree
  3. For each action transition:
     - schedule → cancel:  Publish to build topic — cancel the build
     - cancel → schedule:  Publish to build topic — schedule a new build
  4. Check if any batch is ready to land (all paths have passing builds)
     - If yes: publish to merge topic
```

#### Computing Desired Action

For each path in a batch's tree, the desired action depends on the states of the batches in the path's base:

```
ComputeDesiredAction(path, batch_states):

  For each dependency D in path.Base:
    state = batch_states[D]

    If state is "merged":
      continue                    // Confirmed — this is reality

    If state is "failed" or "cancelled":
      return cancel               // Impossible future

    // Otherwise: D is still in-flight (created, speculating, building)
    // This path remains valid speculation
    continue

  return schedule                  // All deps are confirmed or in-flight
```

The critical property: **this function has no memory**. It does not know or care whether a path was previously cancelled or scheduled. It computes desired state purely from current inputs. This is what makes re-speculation automatic — when a batch's state reverts from "expected to merge" back to "uncertain," the function naturally returns `schedule` for paths it previously returned `cancel` for.

### Out-of-Order Landing

A batch can land before its predecessors under a specific condition: **all of its speculation paths have passing builds**.

```
Example:

  B1 ◄─── B2

B2's paths:
  [B2]       → build passed     (B2 alone on head of main)
  [B1, B2]   → build passed     (B2 after B1 merges)

Since both futures have passing builds, B2 will succeed regardless of
whether B1 merges first or not. B2 can be published to the
merge topic immediately.

Conversely, if only [B1, B2] passed, B2 must wait — it can only
succeed in the future where B1 merges first.
```

```
CanLand(batch, tree, build_results):

  For each path P in tree where P.Action == schedule:
    If no passing build exists for P:
      return false

  return true        // All active paths have passing builds
```

### Scoring and Re-Scoring

Scores are a **batch-level property** — how likely is a batch's build to pass, based on its code changes (files touched, lines changed, dependency count, etc.). Path scores are derived by combining individual batch scores along the path.

Re-scoring is optional and configurable per signal type:

| Signal | Re-Score? | Rationale |
|--------|-----------|-----------|
| `batch_scored` | No | Batch already scored by BatchScorer Controller upstream |
| `build_status_changed` | No | Same paths, only actions change; batch hasn't changed |
| `merge_completed` (success) | Yes | DAG structure changed; remaining paths have different compositions |
| `merge_completed` (failure) | No | Re-speculation of cancelled paths; original scores still valid |

The reconciler accepts a configuration option to control re-scoring behavior. When re-scoring is enabled, the reconciler calls the scorer for affected batches and updates path scores before persisting.

### Persistence

#### Speculation Tree Storage

Each batch's speculation tree is stored as a single row keyed by batch ID. The tree contains the full set of paths with their current actions and scores.

```
SpeculationTree Store:
  Key:    BatchID (string)
  Value:  SpeculationTree (serialized)

Operations:
  Get(batchID) → SpeculationTree
  Put(batchID, tree) → error
  GetMulti(batchIDs) → map[batchID]SpeculationTree
  Delete(batchID) → error
```

Trees are created when a `batch_scored` signal is processed and deleted when the batch reaches a terminal state (landed, failed, cancelled).

#### Batch State

The reconciler reads batch states from the existing storage layer. No new storage is needed for batch state — the `Batch` entity already tracks state and version.

### Downstream Consumers

The reconciler produces outputs to two downstream controllers:

#### Build Controller (Build Topic)

When a path's action transitions, the reconciler publishes to the build topic:

```
BuildAction:
  BatchID:    string
  Path:       SpeculationPath     (base + head)
  Score:      float32
  Action:     "schedule" | "cancel"
```

The Build Controller is responsible for:
- `schedule`: Creating a CI build for this speculation path
- `cancel`: Cancelling an in-progress build for this path (if any)

After scheduling, the Build Controller publishes the BuildID to the Build Signal Controller for monitoring. The Build Signal Controller polls build status, extends the queue message visibility timeout while the build is still running, and publishes back to the speculate topic when the status changes (passed, failed, cancelled).

#### Merge Controller (Merge Topic)

When a batch satisfies the landing condition (all active paths have passing builds), the reconciler publishes:

```
ReadyToMerge:
  BatchID:    string
  Queue:      string
```

The Merge Controller handles the actual merge operation. Once complete, it publishes the outcome to two places:
- **Speculate topic** — so the reconciler can update trees (collapse on success, re-speculate on failure)
- **Finalize topic** — so the Request Finalizer can update the state of all requests contained in the batch

### Build Signal Controller — Monitoring Loop

The Build Signal Controller bridges the gap between the CI system and the reconciliation loop. It consumes BuildIDs from the build-signal topic and monitors their status:

```
Build Signal Controller loop:

  1. Consume BuildID from build-signal topic
  2. Poll CI system for build status
  3. If still running:
     - Extend message visibility timeout (prevent redelivery)
     - Continue polling
  4. If status changed (passed, failed, cancelled):
     - Publish build_status_changed signal to speculate topic
     - Ack the message
```

This design decouples the Speculation Controller from CI system polling. The Build Signal Controller handles the operational concern of monitoring long-running builds (which may take minutes to hours), while the Speculation Controller only processes discrete state-change events.

### Action State Machine

Each speculation path's action follows this state machine:

```
                 ┌──────────────────────────────┐
                 │                              │
                 ▼                              │
  ┌──────────────────┐    dep failed       ┌────┴─────────────┐
  │                  │ ──────────────────→  │                  │
  │    schedule      │                     │     cancel        │
  │                  │ ◄──────────────────  │                  │
  └────────┬─────────┘    dep state        └──────────────────┘
           │              reverts
           │              (re-speculation)
           │
           │  all paths pass
           ▼
  ┌──────────────────┐
  │                  │
  │  ready to land   │
  │  (batch-level)   │
  │                  │
  └──────────────────┘
```

Transitions:

| From | To | Trigger |
|------|----|---------|
| `schedule` | `cancel` | Dependency build failed or dependency cancelled |
| `cancel` | `schedule` | Dependency state reverted (merge failed, re-entered queue) |
| `schedule` | ready to land | All scheduled paths have passing builds (batch-level decision) |

Note: "ready to land" is not a path-level action — it is a batch-level outcome derived from the aggregate state of all paths.

### Observability

#### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `reconcile.signal.received` | Counter | Signals received, tagged by type |
| `reconcile.duration` | Timer | Time spent in reconciliation |
| `reconcile.connected_set.size` | Histogram | Number of batches in connected set |
| `reconcile.action.transition` | Counter | Path action changes, tagged by from/to |
| `reconcile.paths.scheduled` | Gauge | Currently scheduled paths |
| `reconcile.paths.cancelled` | Gauge | Currently cancelled paths |
| `reconcile.rescore` | Counter | Re-scoring events |
| `reconcile.ready_to_land` | Counter | Batches published to merge topic |

#### Logging

Each reconciliation pass logs:
- Signal type and batch ID
- Connected set composition
- Action transitions (from → to) for each affected path
- Whether re-scoring was triggered
- Ready-to-land decisions

### End-to-End Example

A complete walkthrough with three batches:

```
Initial state: Queue Q has three batches

  B1 ◄─── B2 ◄─── B3
  (B2 depends on B1, B3 depends on B2)
```

**Step 1: B1 scored**

```
Signal: batch_scored(B1, score: 0.8)
Connected set: {B1}

B1 Tree:
  [B1]  → schedule, score: 0.8

Action: Publish to build topic — schedule build for [B1]
```

**Step 2: B2 scored**

```
Signal: batch_scored(B2, score: 0.9)
Connected set: {B1, B2}

B2 Tree (generated by strategy):
  [B2]      → schedule, score: 0.9
  [B1, B2]  → schedule, score: 0.3

Action: Publish to build topic — schedule builds for [B2] and [B1, B2]
```

**Step 3: B3 scored**

```
Signal: batch_scored(B3, score: 0.95)
Connected set: {B1, B2, B3}

B3 Tree (generated by strategy):
  [B3]              → schedule, score: 0.95
  [B2, B3]          → schedule, score: 0.5
  [B1, B2, B3]      → schedule, score: 0.2
  [B1, B3]          → schedule, score: 0.4

Action: Publish to build topic — schedule builds for all four paths
```

**Step 4: B1's build passes**

```
Signal: build_status_changed(B1, path=[B1], status=passed)
Connected set: {B1, B2, B3}

Reconcile: No action changes (B1 is still in-flight, not merged yet)
Check: Can B1 land? [B1] passed → yes!
Action: Publish B1 to merge topic
```

**Step 5: B2's build for [B2] passes**

```
Signal: build_status_changed(B2, path=[B2], status=passed)
Connected set: {B1, B2, B3}

Reconcile: No action changes
Check: Can B2 land? [B2] passed, [B1, B2] still building → no
```

**Step 6: B2's build for [B1, B2] passes**

```
Signal: build_status_changed(B2, path=[B1,B2], status=passed)
Connected set: {B1, B2, B3}

Reconcile: No action changes
Check: Can B2 land? [B2] passed, [B1, B2] passed → yes!
Action: Publish B2 to merge topic

Note: B2 can land before B1 because both futures have passing builds.
```

**Step 7: B1 merges successfully**

```
Signal: merge_completed(B1, outcome=success)
Connected set: {B2, B3}  (B1 leaves the DAG)

Reconcile B2's tree:
  [B2]      → cancel  (B1 merged, so B2-alone is impossible now)
  [B1, B2]  → schedule (confirmed — this is reality)

Reconcile B3's tree:
  [B3]              → cancel  (B1 merged, B3-alone impossible)
  [B2, B3]          → cancel  (B1 merged, must include B1)
  [B1, B2, B3]      → schedule (matches reality so far)
  [B1, B3]          → schedule (B1 confirmed, B2 may not merge)

Action: Cancel builds for [B2], [B3], [B2, B3]
Publish to finalize topic for B1's requests
Re-score: Yes (DAG structure changed)
```

**Step 8: B1 fails to merge (alternative to Step 7)**

```
Signal: merge_completed(B1, outcome=failure)
Connected set: {B1, B2, B3}  (B1 stays in DAG)

Reconcile: B1's state reverts to uncertain

B2's tree:
  [B2]      → schedule (B1 didn't merge, B2-alone is possible again)
  [B1, B2]  → schedule (B1 still in-flight)

B3's tree:
  [B3]              → schedule
  [B2, B3]          → schedule
  [B1, B2, B3]      → schedule
  [B1, B3]          → schedule

If any paths were previously cancelled, they are re-speculated
(action flips from cancel back to schedule).

Action: Re-schedule builds for any previously cancelled paths
```

## Trade-offs

### Single Speculate Topic vs. Per-Signal Topics

**Chosen: Single speculate topic**

Funneling all signals into one topic guarantees per-queue serialization. The alternative — separate topics for each signal type — requires distributed locking because signals for the same queue can arrive on different consumer partitions simultaneously.

Trade-off: Slightly higher latency (all signals queue behind each other) in exchange for correctness without locks.

### Declarative Reconciliation vs. Imperative Event Handling

**Chosen: Declarative reconciliation**

The reconciler computes desired state from current inputs rather than applying incremental transitions. This means re-speculation is free — cancelled paths come back automatically when conditions change. The alternative — imperative handlers that track "why" each path was cancelled and "undo" specific actions — is more complex and error-prone.

Trade-off: Each reconciliation pass reads the full connected set (slightly more I/O) in exchange for simpler, stateless logic with no edge cases around state recovery.

### Cancelled Paths Retained vs. Deleted

**Chosen: Retained with `cancel` action**

Cancelled paths stay in the tree rather than being removed. This enables re-speculation without regeneration and provides an audit trail. The alternative — deleting cancelled paths and regenerating them when needed — requires the strategy to be called again and may produce different paths if the algorithm or scores have changed.

Trade-off: Slightly larger tree storage in exchange for deterministic re-speculation and auditability.

### Batch-Level Scores vs. Path-Level Scores

**Chosen: Batch-level scores, derived into path scores**

Scores represent how likely a batch's build is to pass, based on code change characteristics. Path scores are computed by combining batch scores along the path. Re-scoring happens at the batch level when the DAG structure changes, not at the path level on every signal.

Trade-off: Scores may be slightly stale after DAG changes but remain meaningful predictions. Avoids unnecessary scorer calls on signals that don't change batch characteristics.

## Alternatives Considered

### 1. Optimistic Locking per Batch Row

Each batch's speculation tree stored with a version number; concurrent updates detected and retried.

**Rejected because:** Reconciliation often updates multiple batches' trees from a single signal (the connected set). Per-row optimistic locking doesn't provide cross-batch consistency — two concurrent reconcilers could each update a subset of the connected set, leaving the DAG in an inconsistent intermediate state.

### 2. Distributed Lock per Queue

Acquire a lock (database row lock or external lock service) per queue before reconciling.

**Rejected because:** Adds operational complexity and a failure mode (lock holder crashes). The single-topic approach achieves the same serialization guarantee using the existing queue infrastructure.

### 3. Separate Reconciliation Paths per Signal Type

Different controllers handle each signal type independently, each with its own reconciliation logic.

**Rejected because:** Duplicates the core reconciliation logic across three controllers. Makes it harder to reason about interactions between signal types and increases the risk of inconsistencies.

## Appendix

### References

- [SubmitQueue SQL Queue RFC](sql-queue-rfc.md) — Queue infrastructure used for signal routing
- Kubernetes Controller Pattern — Inspiration for declarative reconciliation loops
- Speculative Execution in Processors — Analogous concept: execute multiple futures, commit the one that materializes
