# Stovepipe `batch` stage

This document specifies the `batch` pipeline stage: the seam between Stovepipe's per-commit tracking and its per-range validation. It assumes the pipeline and entity model described in [workflow.md](workflow.md).

## Responsibility

`batch` consumes validated commits and aggregates them into a contiguous **Batch** — a validation attempt over a range of commits — then hands the `BatchID` to `speculate`. It is the first stage that owns persistent pipeline state, and the only stage that creates batches. It does not build, analyze targets, or assign commit status; those belong to later stages.

Per the workflow RFC's per-controller summary, the stage's contract is: input a commit SHA (from `validate`), output a `BatchID` (to `speculate`), aggregating "commits since the last known green into a validation Batch (commit range)".

## Why the stage is more than a passthrough

Unlike `start` and `validate` (pure forwarders today), `batch` has to decide *when* an accumulating range stops growing and becomes a unit of work, and it has to persist that range so the rest of the pipeline can fetch it by ID. Two forces drive its shape:

- It has two logical triggers in the pipeline graph: the **forward** trigger (`validate → batch`, carrying a commit) that grows a range, and the **advance** trigger (`conclude → batch`, carrying a `BatchID`) that opens the next range once a green is established.
- It reads and writes working state — the open batch per repository — that downstream stages depend on.

## Scope of this version

This version specifies the **forward path** only: `validate → batch → speculate`, with batches that close on a size or age threshold. The **advance path** (`conclude → batch`) and the **green pointer** (the last-known-good trunk SHA that anchors where the next batch begins) are deferred. They become meaningful only once `conclude` exists, and the forward path is correct without them: `batch` manages a single open batch per repository, creating a fresh one whenever none is open. The interfaces below leave explicit seams so the advance path slots in without reshaping them.

## Close policy

An open batch closes — publishing its `BatchID` to `speculate` — when **either** of two thresholds is reached, whichever comes first:

- **Size**: the number of commits in the batch reaches `N` (default 10).
- **Age**: the batch's age since it opened reaches `T` (default 30s).

Both thresholds are configurable. The size threshold bounds how much work a single build covers; the age threshold bounds how long a low-throughput repository waits for feedback when it never accumulates `N` commits.

### How each threshold fires

The size threshold is purely event-driven: each forwarded commit appends to the open batch, and the append that reaches `N` closes it inline.

The age threshold needs a time trigger, but the pipeline is otherwise message-driven. Rather than introduce a timer subsystem, `batch` schedules a delayed self-message when it opens a batch, using the queue's existing deferred-delivery support (`Publisher.PublishAfter`, backed by the `visible_after` column). The message carries the `BatchID` and becomes visible after `T`. When it is delivered, `batch` closes the batch if it is still open.

This keeps the age trigger inside the same at-least-once, idempotent message model as everything else, and costs nothing while a batch is filling.

### Closing is an idempotent CAS

Both triggers funnel through one close operation: a compare-and-set of the batch state from `open` to `closed`, guarded by the batch's version (the optimistic-locking pattern used across the system). Whichever trigger arrives first flips the state and publishes to `speculate`; the other observes a non-open batch and does nothing. Because appends are idempotent on the commit identity and the close is idempotent on state, redelivery of any message — a re-sent commit, a duplicate timeout — converges rather than double-acting.

## Message kinds on the `batch` topic

The controller dispatches on the deserialized payload type:

| Payload | Source | Action |
|---|---|---|
| Commit (ChangeEvent) | `validate` | Open or extend the repository's batch; close on size threshold |
| BatchID (timeout) | `batch` itself (delayed) | Close the batch if still open (age threshold) |
| BatchID (advance) | `conclude` | Open the next range — **deferred to a later version** |

## Entities

Two new Stovepipe entities, mirroring the shape of SubmitQueue's `Batch`/`BatchID`:

- **Batch** — the durable record: a unique ID, the repository-scoped partition key, the ordered member commits, a lifecycle state, a creation timestamp, and a version for optimistic locking. States are `open`, `closed`, and the terminal `succeeded`/`failed` that later stages assign. The member list stores the commit identities the pipeline already carries (the change URIs), in arrival order.
- **BatchID** — a thin identifier wrapper used as the queue-hop payload, so hops carry an ID and the consumer fetches the full record from storage, as every internal hop does.

## Storage

`batch` introduces Stovepipe's first storage extension, `batchstore`, following the extension contract: a vendor-agnostic interface with a MySQL implementation behind a factory, key/value-shaped, with version arithmetic owned by the caller and the store performing pure conditional writes.

The interface supports: fetch a batch by ID; create a batch (failing if the ID exists); append a member under a version guard; update state under a version guard; and look up the single open batch for a partition key. Sentinel errors distinguish not-found, already-exists, and version-mismatch so the controller can classify retryability.

Every operation is a single-key get/put or a guarded conditional write, and the open-batch lookup is a single-partition point read (there is at most one open batch per repository by construction). This keeps the contract satisfiable by a key-value or document backend, not just SQL.

Batch IDs are minted from the shared counter extension, formatted as `<partition-key>/batch/<counter>`, the same approach SubmitQueue uses for its batch IDs.

The green pointer is intentionally **not** part of this extension yet; it arrives with the advance path.

## Controller flow

```
   validate
      │ commit
      ▼
 ┌─ batch ─────────────────────────────────────────────┐
 │ on commit:                                           │
 │   open = GetOpenByPartition(repo)                    │
 │   if none: id = counter.Next(); Create(open id)      │
 │            PublishAfter(batch, BatchID, T)  ── age ──┐│
 │   AppendMember(id, ...)                              ││
 │   if size >= N:  close ──────────────────────────┐  ││
 │                                                   │  ││
 │ on timeout (delayed BatchID):                     │  ││
 │   if still open: close ───────────────────────────┤  ││
 │                                                   ▼  ││
 │ close = CAS state open→closed; publish BatchID ──────┼┼─► speculate
 └──────────────────────────────────────────────────────┘
```

The controller keeps the same shape as `start` and `validate`: it implements the consumer `Controller` contract, resolves its content through injected dependencies (the batch store and counter) rather than pre-resolved data, and publishes through the topic registry.

A version mismatch on append or close is returned as a retryable error, so the consumer framework redelivers and the operation reconverges on fresh state. Deserialization and validation failures are non-retryable and flow to the stage's dead-letter queue, where — per the workflow RFC's DLQ contract — a commit that can never be batched must be driven to a conservative not-green terminal state rather than left stuck.

## Wiring

The example orchestrator constructs the MySQL `batchstore` and the counter, registers the `batch` controller on the primary consumer under the `orchestrator-batch` consumer group, and registers a `speculate` topic so `batch` has a real forward target. As with `validate` before it had a consumer, `speculate` is published-to but not yet consumed. The size and age thresholds are surfaced as configuration with the defaults above.

## Testing

- Unit tests for the entities (serialization, terminal-state classification) and the controller (open a new batch, extend an existing one, close on size, close on age, timeout on an already-closed batch is inert, version mismatch surfaces a retryable error), using mocks for the store, counter, and publisher.
- An orchestrator integration test that stands up the orchestrator and queue via Docker Compose, injects validated commits, and asserts both close paths: at least `N` commits produce a `speculate` message and a `closed` batch row; fewer than `N` commits close after `T`. This replaces manual SQL injection for exercising the stage.

## Deferred work

- The advance path (`conclude → batch`) and green-pointer advancement, which anchor batches to "since last green" and bound the pipeline to one validating range at a time.
- History-rewrite convergence for the green pointer, per the workflow RFC's resilience requirement.
- Per-target/per-project batching granularity, which the workflow RFC describes as an evolution of the initial repo-level model.
