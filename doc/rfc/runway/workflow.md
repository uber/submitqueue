# Runway Workflow

Runway is the landing service: it owns VCS operations — mergeability checking and landing — on behalf of SubmitQueue. The orchestrator subscribes to two inbound topics (`runway-check`, `runway-land`) and publishes results to two outbound topics (`sq-check-result`, `sq-land-result`). It is a consumer-only service with no gateway; work arrives via topic queues and results leave via topic queues.

## Check and land

The two queues operate at different granularities:

- **check** is request-level. A check message carries a request's changes and merge strategy. Runway performs a read-only trial merge and publishes per-change mergeability results back.

- **land** is batch-level. A job message carries the resolved content for each request in the batch (runway has no access to SubmitQueue's request store, so the message is self-contained). Runway pre-validates, lands, and publishes a result with per-item outcomes back.

These are independent input-output flows. Check can run without land ever running, and land does not depend on a prior check.

## Branch serialization

The partition key `repo/target` on both inbound topics serializes all VCS operations for a given branch. The message queue delivers messages with the same partition key to the same consumer in order, so at most one check or land operation is in flight for any given branch at any time.

The outbound topics partition by SubmitQueue queue name, matching SubmitQueue's fan-out model where state updates for the same queue are serialized.

## Workflow

```
                 ┌─────────────────────────────────────────────┐
                 │            submitqueue orchestrator          │
                 └───────┬───────────────────────┬─────────────┘
                         │                       │
                   Check (per request)      Job (per batch)
                         │                       │
                         ▼                       ▼
                 [runway-check]          [runway-land]
                         │                       │
                    check ctrl              land ctrl
                   (read-only)        (pre-validate + push)
                         │                       │
                   CheckResult               Result
                         │                       │
                         ▼                       ▼
                [sq-check-result]       [sq-land-result]
                         │                       │
                         ▼                       ▼
                 ┌───────┬───────────────────────┬─────────────┐
                 │  check-result ctrl       land-result ctrl    │
                 │  (update request       (update batch state,  │
                 │   mergeability)       fan out to conclude)   │
                 │            submitqueue orchestrator          │
                 └─────────────────────────────────────────────┘
```

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **check** | Check | CheckResult -> sq-check-result | Check mergeability of a request's changes against the target branch (read-only) |
| **land** | Job | Result -> sq-land-result | Pre-validate, land, and finalize a batch's changes on the target branch |

The check controller always publishes a result — even when all changes are mergeable — so SubmitQueue receives a definitive answer. On infrastructure error it nacks for retry.

The land controller publishes a conflict result (and acks) when pre-validation detects a conflict; SubmitQueue handles rebatching. On infrastructure error it nacks for retry. On success it publishes per-item outcomes (commit SHAs, whether new commits were produced) so SubmitQueue can update its request state.

## Idempotency

Runway has no persistent state — no request store, no job store, no database. Idempotency is achieved through the VCS contract: land detects already-pushed changes (commit SHAs reachable from HEAD) and treats them as already-landed; closing an already-closed PR is a no-op. Check is read-only and naturally idempotent.

## Ownership by service

### Orchestrator

The orchestrator is the only service. It subscribes to two inbound topics (`runway-check`, `runway-land`), performs VCS operations through a pluggable extension, and publishes results to two outbound topics (`sq-check-result`, `sq-land-result`). It owns no persistent data.

### Shared: the messaging queue

Runway communicates with SubmitQueue only through the messaging queue. The inbound topics are owned by runway; the outbound topics are owned by SubmitQueue.
