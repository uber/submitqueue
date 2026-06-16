# Runway Workflow

Runway is the landing service: it owns VCS operations — mergeability checking and landing (resolve, apply, push, finalize) — on behalf of SubmitQueue. Unlike SQ and Stovepipe, it is a consumer-only domain with no gateway and no proto API; work arrives via topic queues, results leave via topic queues. The orchestrator subscribes to two inbound topics (runway-check, runway-land) and publishes to two outbound topics (sq-check-result, sq-land-result). There are no internal hops between stages and no cycles — each controller consumes one inbound message and produces one outbound result.

## Check and land

The two queues operate at different granularities:

- **check** is request-level. SubmitQueue publishes a check message to determine whether a request's changes can merge cleanly against the target branch. Runway performs a read-only trial merge and publishes per-change mergeability results back.

- **land** is batch-level. SubmitQueue publishes a job message to land a batch. The job carries the resolved content for each request in the batch (runway has no access to SubmitQueue's request store, so the message must be self-contained). Runway pre-validates, lands, finalizes, and publishes a result with per-item outcomes back.

These are independent input→output flows. Check can run without land ever running (SubmitQueue may skip external mergeability checks for some queues), and land does not depend on a prior check having passed through runway.

## Branch serialization

The partition key `repo/target` on both inbound topics serializes all VCS operations for a given branch. The message queue delivers messages with the same partition key to the same consumer in order — at most one check or land operation is in flight for any given branch at any time.

This replaces the LandQueue extension from the synchronous design — no Redis, no MySQL queue table, no distributed lock. The tradeoff: serialization granularity is fixed at the partition key level. If pipelining becomes necessary (overlapping check N+1 with land N for the same branch), it can be implemented within the controllers without changing the queue topology.

The outbound topics partition by SubmitQueue queue name (not repo/branch), matching SubmitQueue's existing fan-out model where state updates for the same queue must be serialized.

## Workflow

```
                 ┌─────────────────────────────────────────────┐
                 │            submitqueue orchestrator          │
                 │                                             │
                 │       │                       │             │
                 └───────┼───────────────────────┼─────────────┘
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
                 ┌───────┼───────────────────────┼─────────────┐
                 │  check-result ctrl       land-result ctrl   │
                 │  (update request       (update batch state, │
                 │   mergeability)       fan out to conclude)  │
                 │                                             │
                 │            submitqueue orchestrator          │
                 └─────────────────────────────────────────────┘
```

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **check** | Check | CheckResult → sq-check-result | Check mergeability of a request's changes against the target branch (read-only) |
| **land** | Job | Result → sq-land-result | Pre-validate, land, and finalize a batch's changes on the target branch |

The check controller always publishes a result — even when all changes are mergeable — so SubmitQueue receives a definitive answer. On infrastructure error it nacks for retry.

The land controller publishes a conflict result (and acks) when pre-validation detects a conflict; SubmitQueue handles rebatching. On infrastructure error it nacks for retry. On success it publishes per-item outcomes (commit SHAs, whether new commits were produced) so SubmitQueue can update its request state. Conflicts are expected outcomes, infrastructure errors are not — this matches the pattern established by SubmitQueue's merge controller.

## Idempotency

Runway has no persistent state — no request store, no job store, no database. Idempotency is achieved through the VCS contract: land detects already-pushed changes (commit SHAs reachable from HEAD) and treats them as already-landed; closing an already-closed PR is a no-op. Check is read-only and naturally idempotent. The message queue provides at-least-once delivery; idempotent consumers handle duplicates.

## DLQ reconciliation

Runway does not implement DLQ reconciliation controllers. Unlike SubmitQueue and Stovepipe — where a stuck message leaves a request or commit in a non-terminal state forever — runway is stateless. A check or land message that exhausts retries and lands in the DLQ simply means SubmitQueue never receives a result for that request or batch. SubmitQueue's own timeout and retry logic handles the missing response (re-publishing the check or re-batching the requests). Adding DLQ controllers to runway would duplicate that recovery logic without adding safety, since there is no persistent state to reconcile.

If runway later gains persistent state (e.g., a job store for observability), DLQ controllers should be added following the same pattern as SubmitQueue and Stovepipe.

## Ownership by service

### Orchestrator

The orchestrator is the only service. It subscribes to two inbound topics (runway-check, runway-land), performs VCS operations through a pluggable extension, and publishes results to two outbound topics (sq-check-result, sq-land-result). It owns no persistent data.

### Shared: the messaging queue

Runway communicates with SubmitQueue only through the messaging queue, consistent with the ownership model established by SubmitQueue and Stovepipe. The inbound topics are owned by runway; the outbound topics are owned by SubmitQueue. This keeps the dependency direction clean: runway depends on the shared entity package for message payloads, but not on SubmitQueue's internal state management.
