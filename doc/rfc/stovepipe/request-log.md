# Stovepipe Request Log

## Summary

Stovepipe retains an append-only request log for each validation request. Its internal `RequestLog` is the counterpart of SubmitQueue's `RequestLog`: both retain request status changes and explanatory lifecycle events, while Stovepipe persists records directly instead of sending them through a cross-service log topic and materializer. The public API presents these records as request history. The log records every durable `Request.State` transition plus three asynchronous milestones needed to explain those transitions and the public verdict:

- `build_triggered`;
- `build_finished`;
- `validation_fact_recorded`.

The model deliberately follows SubmitQueue's distinction between statuses describing where a request is and events describing important activity that does not move it. It remains a bounded request-lifecycle log rather than a generic event bus or an audit of every correlated operation.

`Request`, `Build`, and `ValidationFact` remain the operational sources of truth. The request log is a required audit/read model derived from successful durable operations; it does not replace their optimistic-locking state machines.

The separate [Stovepipe Request History API](request-history-api.md) defines public lookup, projection, materialization, ordering, and retention semantics.

Related documents are [SubmitQueue Gateway Request History APIs](../submitqueue/history-api.md), [Stovepipe Workflow](workflow.md), [Process](steps/process.md), [Build](steps/build.md), [Buildsignal](steps/buildsignal.md), and [Record](steps/record.md).

## Problem and Scope

Stovepipe currently retains only the latest mutable state of a validation request and build. A `ValidationFact` retains the immutable verdict for a commit, but none of these records explains how a request reached that verdict.

State changes alone are insufficient. A request remains processing while a build is triggered and finishes, and its terminal state becomes durable before record creates the fact that establishes green or broken. These milestones must remain visible without inventing request states for work occurring underneath the request's position.

An occurrence belongs in the request log only when it is either:

1. a durable `Request.State` transition; or
2. an asynchronous durable milestone required to explain the next request transition or public verdict.

The following remain with their owning entities, metrics, or structured logs:

- Queue latest-request and last-green bookmarks;
- source-control promotion;
- project analysis and project facts;
- hooks and downstream notifications;
- build-slot claims, waits, and releases;
- queue handoffs, delivery attempts, holds, nacks, and visibility timeouts;
- transient dependency errors and DLQ mechanics.

## Goals

1. Retain every request-state transition with its successful request version.
2. Retain the build and fact milestones needed to explain request progress and verdict creation.
3. Make writes idempotent under redelivery, restart, and uncertain failure.
4. Preserve enough source context to repair the request log after a source write succeeds.
5. Keep storage queue-scoped and compatible with key/value, SQL, document, remote, and in-memory backends.
6. Preserve persist-before-publish and controller-owned version arithmetic.

## Non-goals

1. Replacing the Request, Build, or ValidationFact stores with event-sourced projections.
2. Recording every durable operation correlated with a request.
3. Reconstructing transport behavior or unchanged build polls.
4. Providing global ordering or cross-request search.
5. Using the request log as the source of durable greenness; only `ValidationFact` establishes green or broken.
6. Publishing internal request-log records directly as cross-domain hook events.

## Existing SubmitQueue Pattern

SubmitQueue retains `RequestLog` separately from its mutable orchestrator `Request`. Each entry is either a customer-facing status or an event that happened while the request remained at a status. Status entries may carry the orchestrator request version for reconciliation; events carry no request version and never compete to become current status.

Its status vocabulary covers accepted, validating, batched, speculating, landing, landed, error, and cancellation. Its deliberately small event vocabulary covers building, built, waiting, and invalidated rather than every queue hop, retry, storage update, or side effect.

Producers use stable occurrence values in message IDs to deduplicate repeated publication of one logical occurrence. The gateway appends a flat `RequestLog` with type, status or event, timestamp, request version, error, and metadata. Consumer retries may still retain duplicate log rows because the stored record itself has no idempotency key.

Stovepipe adopts the status/event split, bounded vocabulary, queue scoping, and per-occurrence identity. It strengthens retained idempotency with a stable entry ID so retries converge on one stored occurrence.

Stovepipe writes the request log directly because every stage belongs to the same service and resolves the same queue-scoped storage aggregate. SubmitQueue's cross-service log topic is unnecessary unless log ownership later crosses a service boundary.

## Request Log Entity

The retained unit is `entity.RequestLog`:

```go
type RequestLog struct {
    // ID is the stable identity of one logical occurrence within the request. It is opaque, stable across redelivery, and never derived from time or randomness.
    ID string
    // Queue is the logical queue containing the request and scopes RequestID.
    Queue string
    // RequestID identifies the request whose log contains this record.
    RequestID string
    // TimestampMs is the durable occurrence time in Unix milliseconds.
    TimestampMs int64
    // State is the durable request state recorded by a state entry. It is unset on an event entry.
    State RequestState
    // Event identifies the occurrence recorded by an event entry. It is unset on a state entry.
    Event RequestEvent
    // RequestVersion is the successful request version recorded by a state entry. It is zero on an event entry.
    RequestVersion int32
    // OutcomeReason is the domain reason for a terminal request state and never names a transport mechanism.
    OutcomeReason RequestOutcomeReason
    // Metadata contains optional occurrence context. Nil and empty maps are equivalent.
    Metadata map[string]string
}
```

Enums are strings with unknown sentinels, and the entity has no storage or transport dependency. Constructors enforce exactly one of state and event and validate the required context for that kind. The populated field identifies the entry kind without a separate discriminator.

The core column shape follows SubmitQueue's `RequestLog`, except Stovepipe omits SubmitQueue's redundant `Type` column. Context that is meaningful only for one occurrence kind remains in the JSON metadata map instead of adding sparse columns.

Metadata is never used for occurrence identity, filtering, or control flow. Nil and empty maps are equivalent. The initial entity and storage contract treats the map as opaque JSON; writer and projection work may later define and enforce keys such as `superseded_by_request_id`, `build_id`, and `fact_degree`. Producers must not store credentials, raw dependency errors, stack traces, or unbounded payloads. The initial public history API does not expose the raw map.

Immutable Request context such as URI, build strategy, and base URI remains on `Request` and is resolved there rather than copied into log records or history responses. Build status and version remain on `Build`; the triggered and finished event kinds plus the terminal Request state describe the lifecycle without duplicating Build snapshots. Diagnostic error codes remain in structured logs until a concrete public vocabulary is required.

## Vocabularies

### State entries

| State | Required retained context |
|---|---|
| `accepted` | Request version 1 |
| `processing` | Request version |
| `superseded` | Superseding request ID metadata, outcome reason, and request version |
| `succeeded` | Outcome reason, winning build ID metadata, and request version |
| `failed` | Outcome reason, optional build ID metadata, and request version |
| `cancelled` | Outcome reason, build ID metadata, and request version |

### Event entries

| Event | Meaning | Required retained context |
|---|---|---|
| `build_triggered` | A runner accepted a build and its Build row became durable. | Build ID metadata and creation time |
| `build_finished` | The Build first reached a write-once terminal status. | Build ID metadata and status-change time |
| `validation_fact_recorded` | The immutable whole-repository fact became durable. | Degree metadata and fact creation time |

Build running and unchanged polls are not retained. Trigger and terminal result explain the request outcome without turning polling into an unbounded log. Project facts remain outside the initial vocabulary.

### Evolution

State and event strings never change meaning or get reused. A new event must satisfy the scope rule and update required-field validation, storage mapping, public projection, and compatibility tests.

Schema evolution is additive: nullable or defaulted columns and tolerant readers land before writers populate a new field or kind. Incompatible reinterpretation requires a new field or event rather than changing stored meaning; metadata-key semantics are defined when their writers and readers are introduced.

### Outcome reasons

Terminal entries retain domain reasons rather than transport mechanisms. Initial reasons include:

- `build_succeeded`;
- `build_failed`;
- `build_cancelled`;
- `processing_failed`;
- `build_polling_exhausted`;
- `validation_timeout`;
- `superseded_by_newer_head`.

## Stable IDs and Idempotency

`stovepipe/core/requestlog.Recorder` constructs opaque IDs from durable identities:

| Entry | Stable identity inputs |
|---|---|
| Request state transition | Request ID and request version |
| Build triggered | Request ID, event kind, and build ID |
| Build finished | Request ID, event kind, and build ID |
| Validation fact recorded | Request ID, event kind, and whole-repository fact identity |

The recorder calls `RequestLogStore.Create`. If the ID already exists, it loads the stored record and compares every semantic field. Identical content is idempotent success; conflicting content is an internal consistency error, and the stored record is never overwritten.

## Storage Contract

The queue-scoped store is append-only and key/value-shaped:

```go
type RequestLogStore interface {
    Create(ctx context.Context, log entity.RequestLog) error
    Get(ctx context.Context, requestID, logID string) (entity.RequestLog, error)
    List(ctx context.Context, requestID string) ([]entity.RequestLog, error)
}
```

`Create` returns `ErrAlreadyExists` for an existing ID. `List` returns every retained record for one request ordered by `(timestamp_ms ASC, log_id ASC)`. An empty result does not determine whether the request exists; the controller loads `Request` separately to distinguish an unknown request from an existing request with no retained records.

The contract exposes no update, delete, query-by-state, query-by-event, query-by-build, cross-request search, or cross-queue enumeration. A key/value backend can represent the request as a partition and entries as immutable child records. The bounded lifecycle vocabulary and reasonable per-request build limits keep this point read modest, matching SubmitQueue's `RequestLogStore.List` contract.

The initial MySQL table uses:

```sql
metadata JSON NOT NULL
PRIMARY KEY (queue, request_id, log_id)
KEY idx_request_log_order (queue, request_id, timestamp_ms, log_id)
```

Metadata is serialized as a JSON object and normalized to an empty map on write and read. The storage layer does not interpret its keys or values. The ordering index covers the request partition and stable order without introducing query-by-attribute capability.

## Write and Repair Protocol

Request-log durability is part of completing a pipeline transition. The source write succeeds first, the required log record is retained second, and a dependent handoff is published only after log creation or identical-existing reconciliation succeeds.

For a Request transition, the controller:

1. builds an immutable updated copy with transition context and `StateChangedAtMs`;
2. computes `newVersion = oldVersion + 1`;
3. calls `RequestStore.Update(updated, oldVersion, newVersion)`;
4. assigns the in-memory version only after the store succeeds;
5. asks the recorder to create the log record from durable Request data;
6. publishes the downstream handoff.

Request creation, Build changes, and fact creation use the same source-write, log-write, dependent-publish ordering. A request-log outage can leave a source update visible, but it cannot allow dependent processing to move past an unrecorded transition.

### Controller ownership

| Owner | Source operation and retained log | Retry repair |
|---|---|---|
| Ingest | Create accepted Request, then retain accepted. | An existing Request ensures accepted before process publication. |
| Process | CAS to superseded or processing, then retain that state. | An existing state is reconstructed from Request context before ack or build publication. |
| Build | Create Build after runner acceptance, then retain `build_triggered`. | An identical existing Build ensures the event before buildsignal publication. |
| Buildsignal | Persist terminal Build and retain `build_finished`; CAS the Request outcome and retain its terminal state. | Existing terminal Build and Request outcome each ensure their own entry before record publication. |
| Record | Create or verify the whole-repository fact, then retain `validation_fact_recorded`. | An identical fact owned by the Request ensures the event before bookmark or promotion work. |
| Reconciler | CAS an unrecoverable non-terminal Request to failed, then retain failed. | An existing terminal Request is repaired from its persisted outcome without relabeling it. |

Build running and unchanged polls create no entry. A failed runner trigger that creates no Build creates no event. Cancelled and superseded requests create no validation fact.

No controller infers an outcome reason from `RequestStateFailed` alone, invents a broken fact, or records DLQ as the domain reason.

### Repair trigger requirement

Every writer retains a retry trigger until the request-log write succeeds. A queue stage retries its delivery, or its DLQ reconciler repairs the log and republishes the original handoff. A log or publish failure after a source transition never forces a different outcome.

Stages without a suitable DLQ reconciler add one or use a subscription policy that cannot discard the only repair trigger. Ingest relies on caller retry of the same `(queue, URI)` to complete an accepted Request whose log or process publication failed.

This is rollout work, not deferred cleanup: a mandatory request log without a durable repair trigger could turn a store outage into silently stranded pipeline work.

## Rollout and Retention

Rollout therefore:

1. deploys source timestamp and provenance fields, request-log storage, recorder, and readers;
2. enables writers and verifies every repair path stage by stage;
3. enables the public API after every writer and repair path is active.

The service does not fabricate prior state transitions or emit a synthetic snapshot. Requests accepted before request-log writers are active are not exposed through the history API and are not backfilled.

Initial request-log retention matches Request and request-URI mapping retention. The API RFC owns the resulting selector guarantees.

## Failure Semantics

- Request-log store unavailability is retryable and prevents dependent publication.
- Invalid entry construction is non-retryable.
- `ErrAlreadyExists` followed by identical content is success.
- `ErrAlreadyExists` followed by conflicting content is a non-overwriting consistency error and operator alert.
- A log-write failure cannot roll back its source write; retry or reconciliation must converge the missing record.

Identifiers, outcome reasons, and reasonable per-request build counts have explicit limits. Raw dependency responses, credentials, tokens, stack traces, and unrestricted error messages are never retained.

## Verification and Observability

Contract tests cover required-field validation, stable IDs, idempotent create/reload, conflict detection, queue binding, deterministic ordering, equal-timestamp tie breaking, and empty histories.

Writer tests cover source success followed by log failure, redelivery with the log record absent or present, CAS loss, conflicting terminal writers, downstream publish failure, stable timestamps, and controller-owned version arithmetic. End-to-end tests cover successful, failed, cancelled, superseded, and fail-closed paths plus idempotent redelivery.

Tests reconstruct the latest Request state from state entries by request version and compare it with `RequestStore.Get`. They separately verify that only a durable fact produces green or broken.

The recorder reports create, identical-existing, conflict, validation failure, and storage failure counters tagged only by bounded state or event. IDs, URIs, and errors remain structured log fields rather than metric tags. Alerts cover sustained repair gaps and content conflicts.

## Alternatives Considered

### Record request state changes only

Rejected because build activity occurs while the request remains processing, and terminal Request outcome precedes the fact establishing green or broken.

### Record every correlated durable operation

Rejected because bookmarks, promotion, project facts, hooks, and operational bookkeeping have different owners and audiences. Correlation alone does not make an operation part of the request lifecycle log.

### Use a generic versioned envelope

Rejected for six request states and three explanatory events. Explicit fields are simpler to inspect, constrain, test, and expose. A future migration should be justified by actual vocabulary growth.

### Copy SubmitQueue's log topic

Rejected while one Stovepipe service owns every writer and the queue-scoped storage aggregate. A topic becomes useful if ownership later crosses a service boundary.
