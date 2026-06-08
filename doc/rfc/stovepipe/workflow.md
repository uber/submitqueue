# Stovepipe Workflow

Stovepipe is the post-merge trunk-validation service: it consumes a stream of commits pushed to the trunk (`main`), validates them in batches *after* they land, and records a per-commit health state that downstream systems gate on. It exists because SubmitQueue (SQ) can no longer afford to prove every change green *before* merge at the throughput the monorepo now sees — so SQ merges directly to a `main` that may be temporarily broken, and Stovepipe is the system that finds the breakages, names the offending commit, and drives recovery.

Like SQ, the orchestrator is a queue-driven pipeline of small, single-purpose controllers. Each controller consumes one topic, advances a commit or batch, and publishes to the next topic. Most hops carry only an ID — the controller fetches the entity from storage — while the entry point carries the full payload because there is no row to fetch yet. The pipeline has two cycles: `speculate → build → buildsignal → bisect → speculate` (the build / bisection loop that narrows a failure to a single offending commit) and `conclude → batch` (advance to the next range once a green is established). `conclude` is the only stage that assigns a commit its terminal status. `status` and `log` are two gateway-owned sinks the orchestrator publishes to and the gateway consumes: `status` carries commit-health transitions into the commit-status store, and `log` is an append-only event log of what happened to each commit — the direct analogue of SQ's request-log sink.

## Commit states

Every trunk commit Stovepipe tracks is in one of three states. Callers — deployment systems and developer tooling — read this state to decide whether a commit is safe to act on.

- **`unknown`** — the commit has landed on `main` but has not yet been validated. This is the default the moment a commit is ingested. Most commits between two validated points sit here, because validation is batched rather than per-commit.
- **`succeeded`** (green) — the relevant targets build and test successfully at this commit.
- **`failed`** (not green) — a target is broken at this commit.

"Green" is ultimately *subjective per target/project*: a commit can be green for one team's targets and broken for another's. Stovepipe starts with a binary repo-level state and evolves toward per-target/per-project granularity; the state machine and the caller contract are the same either way.

## Identity and tracking

Identity is established at the gateway on ingest, but Stovepipe does not mint a synthetic per-event request ID: the **commit SHA** (scoped by repository and branch) is the identity and the dedup key. Keying on the SHA is what makes ingestion idempotent — a commit announced by both a webhook and a poll backfill resolves to the same record and is processed once.

A validation attempt over a contiguous range of commits is a **Batch**, identified by a BatchID. The batch is the unit that carries a build and, through it, a pass/fail result.

Bisection needs no separate tracking machinery: when a batch's build fails, `bisect` splits the range into smaller sub-ranges, each of which is just another Batch driven through the same `speculate → build → buildsignal` loop. The state of the search lives in those ordinary batch results, and `bisect` — not `buildsignal` — owns the decision of when the search is over:

- A probe that builds **green** does *not* end the search and does *not* advance the trunk; it only proves its commits good and shrinks the suspect range, so the result returns to `bisect` for the next probe.
- A probe that builds **red** narrows the suspect range to its lower half.
- When the suspect range is a **single commit** — including the trivial case where the failing range was one commit to begin with, so there is nothing left to split — that commit is the offender. `bisect` routes it to `conclude`, which marks it `failed` and hands it to `remediate`.

The commits proven good along the way are marked `succeeded`, letting the green pointer advance to the last good one; the commits after the offender stay `unknown` until a fix lands and re-validation reaches them.

## Ingestion and completeness

Trunk push events arrive as **external webhook events**, modeled as messages on the queue: when SQ merges a commit to `main`, a webhook notifies Stovepipe, which records the commit (`unknown`) and hands it into the pipeline. Webhooks give low-latency ingestion in the common case.

Webhooks are a latency *optimization*, not a completeness *guarantee*. They can be delayed for hours, arrive out of order, or be dropped entirely — and a missed commit means a hole in trunk coverage that no one notices until something gates on an `unknown` that should have been validated. So ingestion does not depend on webhook reliability. A **fallback reconciliation poller** periodically diffs the last-ingested trunk SHA against the actual `main` HEAD and backfills any gap, publishing the missing commits into the same entry path. With the poller running on a fixed cadence, no landed commit is missed even if webhooks are fully down.

The two producers — webhook and poller — converge through the SHA-keyed idempotency described above, so nothing downstream assumes a commit is seen only once. Commits are processed in trunk order (committer-timestamp / topological), and a batch is a contiguous range of commits since the last known green. Because the green pointer and per-commit state are persisted, the system must be resilient to history rewrites — a previously validated commit that is no longer present on the branch — and converge rather than wedge when that happens.

## Workflow

```
   push events ─┐                                     ┌──────────────────────┐
   (webhook)    │   ┌─────────────────────────────┐   │ gateway: status      │
                ├──►│ gateway: webhook + poll     │┌─►│ Commit-status store  │ ◄─ GetStatus (RPC):
   main HEAD  ──┘   │ Ingest pushes; fallback     ││  └──────────────────────┘    deployment & dev
   (poll)           │ poll backfills missed SHAs  ││  ┌──────────────────────┐    tooling query it
                    └──────────────┬──────────────┘│  │ gateway: log         │
                                   │ PushEvent     ├─►│ Append-only event log│
                                   ▼               │  └──────────────────────┘
                    ┌─ orchestrator ──────────────┐│
                    │ start                       ├┘  orchestrator publishes status
                    │ Record Commit (unknown) by  │   + log events (any stage)
                    │ SHA; emit status + log      │
                    └──────────────┬──────────────┘
                                   │ SHA
                                   ▼
                    ┌─────────────────────────────┐
                    │ validate                    │
                    │ Resolve commit metadata     │
                    │ for ordering & batching     │
                    └──────────────┬──────────────┘
                                   │ SHA
        ┌─────────────────────────►▼
        │ BatchID  ┌─────────────────────────────┐
        │(advance) │ batch                       │
        │          │ Aggregate commits since green│
        │          └──────────────┬──────────────┘
        │                         │ BatchID
        │             ┌───────────►▼
        │             │ ┌──────────────────────────┐
        │        next │ │ speculate  (stub)        │
        │       probe │ │ Prepare (sub-)range build│
        │             │ └────────────┬─────────────┘
        │             │              │ BatchID
        │             │              ▼
        │             │ ┌──────────────────────────┐
        │             │ │ build                    │
        │             │ │ Build changed targets    │
        │             │ └────────────┬─────────────┘
        │             │       Build  │
        │             │              ▼
        │             │ ┌──────────────────────────┐
        │             │ │ buildsignal              │
        │             │ │ Record build result      │
        │             │ └───┬──────────────────┬───┘
        │             │fail/│                  │ full-range
        │             │probe│                  │ pass
        │             │     ▼                  │
        │             │ ┌──────────────────┐   │
        │             └─┤ bisect  (stub)   │   │
        │               │ Narrow to the    │   │
        │               │ offender         │   │
        │               └────────┬─────────┘   │
        │           isolated fail│              │
        │                        ▼              ▼
        │               ┌────────────────────────┐
        │      advance  │ conclude               │
        └───────────────┤ pass → succeeded,      │
                        │   advance next batch    │
                        │ fail → failed,          │
                        │   then remediate        │
                        └───────────┬────────────┘
                                    │ SHA (offender)
                                    ▼
                         ┌─────────────────────────┐
                         │ remediate               │┄┄► remediation
                         │ Invoke remediation      │   extension →
                         │ extension for the commit│   external fix /
                         └─────────────────────────┘   revert → SQ
```

Any orchestrator controller can also publish a `log` event (via a `PublishLog` helper) recording what it did; the gateway is the sole consumer that persists those events to the event log. The `status` and `log` sinks are drawn once at the top right to keep the pipeline readable, but they receive events from across the pipeline, not only from `start`.

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **gateway/webhook** | push event (RPC/HTTP) | start | Receive a trunk push event, publish to the start topic, hand off async |
| **gateway/poll** | (timer) | start | Fallback reconciler: diff last-ingested SHA vs `main` HEAD, backfill any gap |
| **gateway/GetStatus** | RPC | — | Read path: callers query a commit's status (optionally scoped to a target/project) |
| **start** | PushEvent | validate, status, log | Record the Commit as `unknown` keyed by SHA (dedup), emit Recorded status |
| **validate** | SHA | batch | Resolve the commit metadata (parent, committer time) that ordering and batching need |
| **batch** | SHA | speculate | Aggregate commits since the last known green into a validation Batch (commit range) |
| **speculate** (stub) | BatchID | build | Decide the validation strategy and prepare the build for the full range or the next bisection sub-range |
| **build** | BatchID | buildsignal | Build the batch's changed targets (target analysis happens here) |
| **buildsignal** | Build | conclude, bisect | Record the build result; a clean full-range build → conclude (green), any failure or bisection probe → bisect |
| **bisect** (stub) | BatchID | speculate, conclude | Narrow a failing range via sub-batch probes; when the failure is isolated to a single commit, conclude it `failed`, otherwise probe the next sub-range |
| **conclude** | BatchID | batch, remediate, status, log | Green: mark commits `succeeded` and advance the next batch. Failure: mark the offending commit `failed` and hand off to remediate |
| **remediate** | SHA | — (extension) | Invoke the remediation extension for the offending commit; an external fix/revert lands via SQ |
| **status** | StatusEvent | — | Gateway-owned sink: persist the authoritative commit-status store |
| **log** | LogEvent | — | Gateway-owned sink: persist the append-only event log (audit trail) |

Any controller may publish to `log` (the append-only event log) via a `PublishLog` helper, exactly as in SQ; the table lists it only on the stages that most clearly emit it. There is deliberately no changed-target stage and no scoring stage. Target analysis belongs to `build`, which already needs the changed-target set to know what to compile and test, so a separate stage would only pre-compute what `build` must derive anyway. And commits are validated in trunk order rather than reordered by priority, so there is nothing to score; bisection may eventually use a suspicion-weighted heuristic to place its probes (build the commits most likely to be the culprit first), but that is an optional input to `bisect`, not a stage of its own.

## Remediation handoff

When `bisect` isolates the offending commit, `conclude` marks it `failed` and publishes its SHA to the `remediate` topic — the same decoupled publish-then-consume hop the rest of the pipeline uses, not an inline call. The `remediate` controller consumes that topic and invokes a **remediation extension**: a vendor-agnostic, pluggable interface that is Stovepipe's integration boundary with whatever external system produces the fix. The extension hands the offending commit to that system, which generates a revert or fix and lands it through SQ like any other change.

Stovepipe's responsibility ends at invoking the extension. It does not author or land the fix, and it does not block waiting for one — there is no synchronous "wait for green" stage. The fix lands on `main` as an ordinary commit and re-enters Stovepipe through the normal ingest-and-validate path, where it is validated like anything else and the trunk returns to green. This keeps the pipeline non-blocking and the external remediation system fully decoupled behind the extension.

## DLQ reconciliation

Every *consumed* primary pipeline topic above is paired with a `{topic}_dlq` subscription consumed by a dedicated DLQ controller. The `status` and `log` topics are the exception: the orchestrator only publishes to them (the gateway is the sole consumer that persists commit status and the event log), so they have no orchestrator-side subscription and therefore no DLQ. The consumer framework moves a message to its DLQ once the primary controller returns a non-retryable error or exhausts retries on a retryable one.

The stovepipe-specific risk a DLQ must close: a validation that can never complete must not leave a commit stuck non-terminal. A commit wedged at `unknown` forever is not a neutral outcome — callers gate on status, and an unvalidatable commit that silently stays `unknown` blocks the trunk's green pointer from advancing past it. So the DLQ controllers do not re-attempt the failed work; they decode the payload to recover the affected commit SHA or `BatchID` and drive the entity to a **conservative, not-green terminal state**, so gating stays safe (fail closed, never falsely green) and the pipeline can move on. State writes use the same optimistic-locking CAS as the primary pipeline, so a late primary-pipeline update wins cleanly and a version mismatch is asked back for redelivery.

DLQ consumers are wired with `errs.AlwaysRetryableProcessor` and a very high `Retry.MaxAttempts`, with their own DLQ disabled — the same effectively-non-droppable posture SQ uses. The trade-off is identical: a genuinely unprocessable DLQ message (typically a malformed payload) must be removed by an operator. See `submitqueue/orchestrator/controller/dlq/README.md` for the shared design constraints (simplest possible implementation, reconcile-only, no recovery).

## Ownership by service

Each service owns its own data; the gateway and orchestrator never touch each other's, and the only thing they share is the messaging queue.

### Gateway

The gateway is the boundary of the system and the owner of the commit-status store and the event log. It ingests trunk push events — both from external webhooks and from the fallback poller — and hands them to the orchestrator over the queue. It serves the status query RPC that downstream systems call. And it owns the record of each commit's health and history: it is the only service that reads or writes the commit-status store and the log, writing them both directly as commits are ingested and by consuming the status and log events the orchestrator emits.

### Orchestrator

The orchestrator runs the pipeline that takes a landed commit from `unknown` to a terminal state. It owns the working state of that pipeline — in-flight commits, batches, builds, and bisection bookkeeping — and is the only service that writes it. It drives a batch through validation, re-entering speculation as build results arrive and as bisection narrows a failing range, advances to the next range once a green is established, and hands an isolated offending commit off through the remediation extension. It never persists commit status or log entries itself; it only emits status and log events for the gateway to record.

### Shared: the messaging queue

The two services communicate only through the messaging queue. It is pluggable infrastructure kept in its own database, separate from either service's application data: it carries external push events in, the internal pipeline topics between orchestrator stages, and the status and log events the orchestrator publishes for the gateway to consume.

## Status and log ownership invariant

The commit-status store and the event log have exactly one owner: the **gateway**. The orchestrator only emits status and log events onto the queue; it never persists them. The gateway is the sole consumer of those events and the only writer of both the commit-status store and the log.

This keeps all status and log writes in one service: the orchestrator stays a pure pipeline that emits events, and the gateway owns the records — the health state callers query and the history of what happened — end to end. It is the direct analogue of SQ's request-log ownership invariant.
