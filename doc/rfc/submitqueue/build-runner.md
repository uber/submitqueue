# Build Runner

Design notes for the `BuildRunner` extension that lets the orchestrator drive external Build Runners from the build stage of the workflow pipeline.

This document captures **design decisions and rationale only**.

## Problem

The build stage needs a vendor-agnostic abstraction for talking to a Build Runner — one that fits the existing extension family (storage, queue, conflict) and that does not have to change when the first real backend lands.

## Flow

`build` triggers the runner and hands the `buildID` to the `buildsignal` poll loop. The loop calls `Status` on its own partition per build until the build is terminal: terminal results wake the batch state machine via `speculate`; non-terminal results hold the delivery, so the same message redelivers after a delay. A webhook-capable backend can publish a status message into the same queue — the consumer cannot tell a push from a poll.

```
   ┌────────────────────────────────────────────────────┐
   │ build                                              │
   │   Trigger(base, head) → buildID                    │
   │   persist Build{Accepted}, enqueue buildID         │
   └──────────────────────────┬─────────────────────────┘
                              │ buildID
                              ▼
   ┌────────────────────────────────────────────────────┐
   │ buildsignal   (poll loop, one partition per build) │
   │   load Build, call Status(buildID)                 │
   └──────────────────────────┬─────────────────────────┘
                              │
           ┌──────────────────┴─────────────────┐
           ▼                                    ▼
   ┌───────────────┐          ┌──────────────────────────────────┐
   │ terminal      │          │ non-terminal                     │
   │   → speculate │          │   → hold(delay)                  │
   │   re-evaluate │          │   same message redelivers        │
   └───────────────┘          └──────────────────────────────────┘
```

## Interface

`BuildRunner` exposes three verbs, all keyed by a build identifier (`entity.BuildID`):

- **`Trigger`** — submit a build for a queue, given the ordered `base` and `head` change sets plus a free-form metadata map; returns the new build's ID. Runner-side work is asynchronous.
- **`Status`** — fetch the current `BuildStatus` and runner-defined metadata for a build; MAY round-trip to the runner.
- **`Cancel`** — request cancellation; returns once the request reaches the runner, not once the build stops.

See `submitqueue/extension/buildrunner/build_runner.go` for the exact Go signatures. The sections below record why the contract is shaped this way.

### Trigger: base + head

> **Revised by [extension-contract.md](extension-contract.md).** `Trigger` now takes identity at the batch granularity — `base []entity.Batch` (the dependency batches) and `head entity.Batch` (the batch under test) — and the runner resolves each batch's changes itself through an injected `changeset.Resolver`. The base/head split below still holds; only the boundary type changed from resolved `[]entity.Change` to batch identity. The "rejected: list-of-lists of changes" note is superseded by that RFC's "identity in, resolve internally" principle.

`Trigger` takes the base dependency batches, the head batch, and a free-form metadata map:

- **`base`** — the dependency batches (assumed-good prefix), ordered. The runner resolves their changes.
- **`head`** — the batch being verified. The runner resolves its changes.
- **`metadata`** — caller-supplied attributes (requester, ticket ID, trace ID, etc.) the provider MAY persist or echo back via `Status`. Schema is caller/provider-defined; the interface treats it as opaque. `nil` is equivalent to an empty map.

The provider resolves and applies `base` then `head` in order on top of the queue's target branch and validates the resulting tree. Validation is **implicit and holistic**: it is not a per-change action, it is what the provider does after applying everything.

Why split base and head:

- The orchestrator's internal model already distinguishes them — a speculation path has a head batch and a list of base batches assumed to pass.
- Lets a provider cache or short-circuit the base when it has validated the same prefix before — a hot path for stacked-batch speculation.
- Lets the provider attribute terminal failure to base vs head in `BuildMetadata` without the orchestrator having to round-trip the split itself.

Rejected: a single flat input with no base/head split. Provider would have to deduce base via prefix matching and could not distinguish "base broke" from "head broke" without out-of-band hints. Loses the orchestrator's clearest piece of structural information at the boundary for no gain.

### Async vs sync contract

| Verb | Must return promptly? | Notes |
|---|---|---|
| `Trigger` | yes | provider-side work is asynchronous |
| `Cancel` | yes | returns once the request reaches the provider, **not** once the build stops |
| `Status` | no | a provider round trip is typical |

This keeps the orchestrator's queue loops snappy while admitting that reading state is remote.

### Status delivery: polling, as queue traffic

The interface is pull-only — the only way to learn a build's current state is to call `Status`. But polling is not a tight loop. It runs as queue traffic, with each `buildID` as its own partition.

A status-loop consumer receives a `buildID` message, calls `Status`, and either:

- **terminal** → forwards downstream, or
- **non-terminal** → re-publishes to its own queue with a delay.

This makes polling behave like everything else in the orchestrator:

- **Independent partitions** — slow polls on one build don't block others.
- **Restart-safe** — pending polls live in the queue, not in memory.
- **Retry-native** — a `Status` call that errors out is `Nack`'d and redelivered with the queue's normal backoff, separate from polling cadence.
- **Tunable cadence** — the hold delay can vary by status (longer for `Accepted`, shorter for `Running`).

### Polling primitive: hold, not `Nack`

Postponing the next poll needs a "check back later" verb, and the consumer framework provides one: the controller records a hold on its delivery and returns success, and the framework postpones the message — it redelivers after the delay, and the redelivery is exempt from `retry_count` accounting (see [consumer-hold.md](../consumer-hold.md)). `Nack` remains the primitive for genuine `Status` failures, with its normal bounded-retry-then-DLQ behaviour. The two signals stay separate: `retry_count` means "consecutive failures," never "polls so far."

An earlier revision of this design reached the same separation with `Publisher.PublishAfter` — ack the delivery, publish a fresh copy of the same message with delayed visibility. Hold supersedes it: no publisher in the poll loop, no fresh message ids to mint around the queue's publish dedup, no new log row per tick, and the loop's continuation no longer depends on an enqueue succeeding (a failed postpone write lapses into a normal visibility-timeout redelivery).

**Why not `Nack` with `MaxAttempts = ∞`** (one message per build, just keep cycling)? The mechanism works. Three things break:

- **No DLQ escape valve.** A malformed `buildID`, or a build the provider has lost, fails `Status` every call. With unbounded retries the message spins forever; the operator gets no signal that something is permanently wrong. DLQ exists for exactly this case; opting out for the buildsignal subscription means opting out of every poison-message signal it offers.
- **Conflated metric.** `retry_count` is the obvious dashboard signal for "this consumer is having trouble." With infinite-retry polling, a `retry_count` of 500 might mean "build has been running 30 minutes" *or* "Status has errored 500 times" — operationally indistinguishable.
- **Visibility-timeout coupling.** If the consumer crashes mid-poll before its `Nack`, the queue's visibility timeout redelivers the message and bumps `retry_count`. One number ends up counting legitimate polls, real errors, *and* consumer crashes — three signals fused.

Hold keeps all three signals separate for free: a deliberate postpone resets the failure streak, so `retry_count` counts only consecutive genuine failures.

### Push, when a backend supports it

A webhook-capable backend publishes a status message into the same queue, keyed by the same `buildID`. The consumer cannot tell whether a message came from a poll or a push — both shapes are identical, both partition the same way.

This keeps the `BuildRunner` contract pull-only: push is implemented at the queue boundary, not on the interface. A backend without webhooks needs zero extra code; a backend with webhooks needs only a webhook receiver that publishes into the existing queue.

Rejected: a push method on `BuildRunner` (e.g. `Subscribe`). Forces every implementation to either expose a real push primitive or fake one, and gives the orchestrator two parallel update paths. Pushing into the queue collapses both paths.

Rejected: long-polling on `Status`. Not every backend supports efficient server-side blocking; making it part of the contract forces backends to fake it. Same latency, more interface complexity.

### Lifecycle

Implementations are long-lived singletons bound to provider config at construction. Every method is concurrent-safe; connection pools and caches live inside the manager; anything that must survive a restart belongs in persistent storage, not the manager.

### Transient failures

The manager's problem, not the caller's. Reconnect and retry-with-backoff internally; during the recovery window return plain errors rather than block indefinitely.

### Error classification

Methods return plain errors. The controller classifies errors as user vs infra and retryable vs not — the manager does not. It MAY mark errors as retryable when it knows a failure is transient. Domain sentinels (e.g. "build not found") land with the first implementation that needs them, not before.

## `BuildStatus`

A deliberately narrow set — `Unknown`, `Accepted`, `Running`, `Succeeded`, `Failed`, `Cancelled`. Every backend must collapse its native lifecycle into one of these.

`Accepted` covers any pre-execution state — submitted, queued for a worker, waiting on capacity. The orchestrator cannot act differently on "submitted" vs "queued", so collapsing them removes a distinction nobody consumes. `Running` is a separate first-class non-terminal state because "in a queue" and "burning compute right now" are operationally different: cancellation cost differs, and future speculation decisions can use the signal.

`Succeeded` is the common terminal-success name across Build Runners. `Failed` covers terminal failure of any cause — including runner-initiated terminations like timeouts, OOM, or worker crashes, which are real failures the orchestrator must see and react to. `Cancelled` is the terminal state for a build that was cancelled.

The set excludes `Blocked` — a wait on an upstream gate is the orchestrator's concept, owned by speculation. Folding it into build status would conflate two systems and invite a controller branch that should not exist.

`IsTerminal` is a predicate on the type: `Succeeded`, `Failed`, `Cancelled` are terminal. Living on the enum prevents callers from reimplementing the list and drifting.

## `BuildMetadata`

`BuildMetadata` is a free-form `map[string]string` from `Status`. Every backend exposes different metadata (URL, duration, SHA, runner ID, base-vs-head failure attribution); the orchestrator's job is to round-trip it to users, not interpret it.
