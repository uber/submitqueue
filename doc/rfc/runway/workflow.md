# Runway Workflow

Runway is the landing service: it owns VCS operations — mergeability checking and landing — on behalf of SubmitQueue. The orchestrator subscribes to two inbound topics (`merge-conflict-checker`, `merger`) and publishes results to two outbound topics (`merge-conflict-checker-signal`, `merger-signal`). It is a consumer-only service with no gateway; work arrives via topic queues and results leave via topic queues.

## Merge-conflict check and merge

The two queues operate at different granularities:

- **merge-conflict-check** is request-level. A merge request carries an ordered sequence of steps (changes + merge strategy). Runway performs a read-only trial merge and publishes per-step mergeability results back.

- **merge** is batch-level. A merge request carries the same payload but Runway commits the result and reports the revisions it produced (per-step output IDs). The merge strategy on each step determines the VCS operation: `REBASE`, `SQUASH_REBASE`, and `MERGE` transform changes onto the target branch; `PROMOTE` fast-forwards the target ref to an already-existing commit as-is (e.g. pushing a landed SHA from `main` to `verified/main`).

These are independent input-output flows. A merge-conflict check can run without a merge ever running, and a merge does not depend on a prior check.

## Branch serialization

The partition key `repo/target` on both inbound topics serializes all VCS operations for a given branch. The message queue delivers messages with the same partition key to the same consumer in order, so at most one merge-conflict check or merge operation is in flight for any given branch at any time.

The outbound topics partition by SubmitQueue queue name, matching SubmitQueue's fan-out model where state updates for the same queue are serialized.

## Workflow

```
                 ┌─────────────────────────────────────────────────────┐
                 │              submitqueue orchestrator                │
                 └──────────┬───────────────────────────┬──────────────┘
                            │                           │
                  MergeRequest (dry run)      MergeRequest (commit)
                            │                           │
                            ▼                           ▼
              [merge-conflict-checker]              [merger]
                            │                           │
               merge-conflict-check ctrl          merge ctrl
                       (read-only)            (apply + commit)
                            │                           │
                       MergeResult                 MergeResult
                            │                           │
                            ▼                           ▼
           [merge-conflict-checker-signal]       [merger-signal]
                            │                           │
                            ▼                           ▼
                 ┌──────────┬───────────────────────────┬──────────────┐
                 │  merge-conflict-check-       merge-signal ctrl       │
                 │  signal ctrl              (update batch state,       │
                 │  (update request            fan out to conclude)     │
                 │   mergeability)                                      │
                 │              submitqueue orchestrator                │
                 └─────────────────────────────────────────────────────┘
```

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **merge-conflict-check** | MergeRequest | MergeResult -> merge-conflict-checker-signal | Dry-run merge: check mergeability of ordered steps against the target branch (read-only) |
| **merge** | MergeRequest | MergeResult -> merger-signal | Apply, commit, and report per-step output IDs |

The merge-conflict-check controller always publishes a result — even when all steps are mergeable — so SubmitQueue receives a definitive answer. On infrastructure error it nacks for retry.

The merge controller publishes a conflict result (and acks) when the merge detects a conflict; SubmitQueue handles rebatching. On infrastructure error it nacks for retry. On success it publishes per-step outcomes (output IDs of the revisions produced) so SubmitQueue can update its request state.

## Idempotency

Runway has no persistent state — no request store, no job store, no database. Idempotency is achieved through the VCS contract: merge detects already-pushed changes (revisions reachable from HEAD) and treats them as already-landed. Merge-conflict check is read-only and naturally idempotent.

## Ownership by service

### Orchestrator

The orchestrator is the only service. It subscribes to two inbound topics (`merge-conflict-checker`, `merger`), performs VCS operations through a pluggable extension, and publishes results to two outbound topics (`merge-conflict-checker-signal`, `merger-signal`). It owns no persistent data.

### Shared: the messaging queue

Runway communicates with SubmitQueue only through the messaging queue. The inbound topics are owned by runway; the outbound topics are owned by SubmitQueue.
