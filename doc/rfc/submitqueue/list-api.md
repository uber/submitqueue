# Gateway List API

Design notes for a gateway `List` API that powers a queue-scoped UX for observing SubmitQueue requests over a time window.

This document captures design decisions and rationale only.

## Problem

Users need to inspect requests admitted to a queue during a time window and see the useful current state of each request. The existing `Status` API answers that question for one `sqid`; the UX needs the same gateway-owned view across a queue and admission-time range.

The gateway owns request admission data and request logs. The orchestrator may emit request-log events, but it does not persist or read the gateway's records. `List` must preserve that ownership boundary and must not read orchestrator-owned working tables.

## API Shape

`List` is a read-only gateway RPC, named tersely to match `Land`, `Cancel`, and `Status`.

The first version of the wire contract is:

```proto
// ListSort defines deterministic orderings supported by List.
enum ListSort {
  // Use the server default, currently ADMITTED_ASC.
  SORT_DEFAULT = 0;
  // Return requests in FIFO admission order.
  ADMITTED_ASC = 1;
  // Return most recently admitted requests first.
  ADMITTED_DESC = 2;
}

// ListRequest lists requests admitted to a queue during a time window.
message ListRequest {
  // Queue whose requests should be returned. Required.
  string queue = 1;
  // Inclusive lower bound of the admission-time window, in Unix milliseconds.
  int64 start_time_ms = 2;
  // Exclusive upper bound of the admission-time window, in Unix milliseconds.
  int64 end_time_ms = 3;
  // Optional current-status filter using the customer-facing Status vocabulary.
  // An empty list disables status filtering.
  repeated string statuses = 4;
  // Maximum number of summaries to return. Zero selects the server default.
  int32 page_size = 5;
  // Opaque cursor returned by an earlier List response. Empty selects the first page.
  string page_token = 6;
  // Deterministic page order. SORT_DEFAULT selects FIFO admission order.
  ListSort sort = 7;
}

// RequestSummary is the current gateway-owned view of one request in a List page.
message RequestSummary {
  // Globally unique request ID returned by Land.
  string sqid = 1;
  // Queue that admitted the request.
  string queue = 2;
  // Original change URIs submitted when the request was admitted.
  repeated string change_uris = 3;
  // Current customer-facing request status.
  string status = 4;
  // Last error associated with the current status. Empty when there is no error.
  string last_error = 5;
  // Free-form display or debugging metadata for the current status.
  map<string, string> metadata = 6;
  // Time the request entered SubmitQueue, in Unix milliseconds.
  int64 started_at_ms = 7;
  // Time the visible summary state last changed, in Unix milliseconds.
  int64 updated_at_ms = 8;
  // Time the request reached a terminal status, in Unix milliseconds. Zero while active.
  int64 completed_at_ms = 9;
}

// ListResponse is one page of request summaries.
message ListResponse {
  // Current request summaries matching the query, in the requested deterministic order.
  repeated RequestSummary requests = 1;
  // Opaque cursor for the next page. Empty when this is the final page.
  string next_page_token = 2;
}

// List returns a page of summaries for requests admitted to a queue during a time window.
rpc List(ListRequest) returns (ListResponse) {}
```

In the public API, `sqid` is the request identifier. Storage records and SQL use the same value under the name `request_id`.

The summary intentionally exposes gateway/user-facing lifecycle information, not orchestrator implementation details such as batch IDs, internal request states, or speculation-tree structure. `terminal` is intentionally not a response field in the first API: clients can derive it from the public status vocabulary and `completed_at_ms`, avoiding a redundant field that can drift from status semantics.

## Time Window Semantics

The time window is an admission-time filter. A request belongs in `[T1, T2)` when its admission timestamp satisfies `T1 <= started_at_ms < T2`.

This means a request admitted before `T1` does not appear even if it remained active during the window. For example, a request admitted at 09:55 and completed at 10:05 does not appear in `[10:00, 10:30)`, while a request admitted at 10:20 does appear regardless of whether it is still active when `List` is called.

This is an explicit first-version scope decision. Lifecycle-overlap semantics would better answer the separate operational question "what was active at any point during this window?", but that predicate is `started_at_ms < T2 AND completed_at_ms > T1`, with active requests represented by an unbounded completion time. Under the repository's storage constraints, a normal ordered key/value or B-tree index cannot simultaneously bound both interval dimensions and preserve keyset order by `(started_at_ms, request_id)`. The existing admission-order index would still need to inspect requests admitted arbitrarily far before `T1` to discover which remained active.

Making lifecycle overlap bounded and storage-agnostic would require an additional purpose-built projection, such as active-membership records plus historical activity checkpoints, followed by candidate merging, deduplication, and exact filtering in the caller. That adds projection writes, idempotent partial-failure recovery, retention rules, and multiple ordered read streams. It is intentionally deferred until an operational-history API justifies that complexity. The first `List` API instead uses the immutable admission timestamp already owned by `RequestContext`, which supports a single bounded ordered range scan across plausible SQL, key-value, document, and in-memory backends.

`start_time_ms` and `end_time_ms` are required positive Unix-millisecond values and must satisfy `start_time_ms < end_time_ms`. There is no sentinel "unbounded" time value in the first API. An empty `statuses` list means no status filtering. A non-empty list matches requests whose current reconciled status is one of the supplied public status strings.

`List` returns the request's **current** reconciled status at read time for rows that match the window. It is not a historical "status as of T2" API. A historical snapshot API would be a different product shape and should be designed separately if needed.

## Status Filtering

`List` should support filtering by the same customer-facing status strings that `Status` returns: examples include `accepted`, `validating`, `building`, `landing`, `landed`, `error`, `cancelling`, and `cancelled`.

This keeps the API stable at the same abstraction level as `Status`. Clients do not need to learn an internal enum or translate orchestrator state-machine values into display states.

The filter applies to the request's **current** reconciled status after the queue/time-window match has been computed. It does not mean "requests that ever had this status during the window." That historical event query belongs with a timeline/debug API, not the queue summary list.

The filter should accept multiple statuses so the UX can ask for groups such as "currently active" or "terminal outcomes" without making separate RPC calls. The server should validate status strings against the public status vocabulary it can emit; unknown statuses are caller errors rather than silent misses.

## Sorting

`List` should expose sorting as the `ListSort` enum in the API Shape section, not as free-form field names. The first supported modes are admission-time orderings.

The default should be FIFO/admission order so the first page gives the clearest answer to "what is at the head of the queue?" for simple queue views. Clients that want a recent-activity/history view can request `ADMITTED_DESC`.

These names intentionally use "admitted" rather than "queue position." Admission order is gateway-owned and comes from request admission context. Queue position is a stronger scheduler/backend concept: it may differ from FIFO once batching, speculation, cancellation, priority, or retries are involved.

TODO: if a backend supports a real queue-position signal, add a nullable `queue_position` summary column and a corresponding enum value such as `QUEUE_POSITION_ASC`. The RFC should define exactly which requests have positions, when positions change, how terminal requests sort, and how cursor pagination behaves over a mutable position before exposing that mode.

## Request Context

The immutable data needed to identify a request in a list has a different lifecycle and owner from status progress. Queue, original change URIs, and admission time are known exactly once at `Land`. Subsequent status-log producers often have only a request ID, a status, and perhaps a request-state version. Requiring every producer to repeat immutable admission data would make generic status logging depend on a full request snapshot that many producers do not own.

The gateway should therefore persist a standalone immutable `RequestContext` record at successful request admission. It is keyed by `request_id` and contains at least:

- `request_id`
- `queue`
- `change_uris`
- `admitted_at_ms`

`admitted_at_ms` is the immutable source field for the same timestamp exposed and indexed as `RequestSummary.started_at_ms`; the projection copies it without transformation. The context name describes when admission occurred, while the summary/API name preserves the existing customer-facing lifecycle vocabulary.

`Land` is the only normal writer. It receives the queue and original change set before publishing work downstream, so it can validate and write this record without a cross-service lookup. Creation must be idempotent: a retry with the same request ID and identical context succeeds as a no-op, while a conflicting context is an integrity failure.

`RequestContext` is not copied into each `RequestLog` event. Status logs remain status-focused: request ID, timestamp, status, request version when known, last error, and metadata. This preserves the ability for producers with only a request ID to publish a correct log event and removes the need for queue parsing or silent context fallbacks.

The `request_context` record is gateway-owned source data, not an orchestrator-table join. It may be retained as long as its summary is retained, because the summary can be rebuilt from immutable context plus logs.

## Read Model

Serving `List` directly from the append-only request log would force the gateway to scan and reconcile many log rows per request. That is the wrong shape for a queue dashboard. The gateway should maintain a request-summary read model derived from immutable request context and append-only request-log events.

The system has three gateway-owned records with distinct purposes:

- `RequestContext` is immutable admission data used to identify and group requests in
  queue views.
- `RequestLog` is immutable status history used for audit/debugging and point
  reconciliation.
- `RequestSummary` is a mutable current projection used for bounded queue/time-window
  listing.

A summary is created from request context at admission with status `accepted`. Later status logs update only the status-derived portion of that summary. Logs must never overwrite the immutable queue, change URIs, or admission timestamp established by request context.

The log consumer owns projection semantics. It loads the current summary, applies the same reconciliation rules as `Status`, computes the next summary, and uses the summary store's conditional write to persist it. The storage layer is intentionally a mechanical persistence boundary: get by key, create, conditional update, and page query. It does not derive queue fields, decide winner precedence, or silently normalize application mistakes.

The log consumer must make each delivery idempotent. It first inserts the immutable log event with its existing deduplication behavior, then projects that event into the corresponding summary. A projection failure is returned so the delivery retries. Reprocessing must tolerate the already-inserted log and retry only the projection, rather than acknowledging the message and silently leaving List stale.

Context and log writes are ordered by the admission flow: `Land` creates `RequestContext`, creates or initializes `RequestSummary`, persists the accepted log, then publishes pipeline work. The accepted context and summary writes must complete before publication. For legacy or recovery events whose context has not been created, the log consumer should return a retryable missing-context error rather than inventing queue data from the request ID. A separately designed backfill can create contexts for historical requests when an authoritative gateway-owned source exists.

## Reconciliation

Request-log timestamps are useful for display and broad ordering, but they are not always the strongest signal for "current state." Some log entries reflect informational progress, while others reflect versioned request-state changes.

`Status` reconciles by reading all request-log rows at once. The summary projector must apply the equivalent rule incrementally, one incoming event at a time. Each update is a guarded merge between the stored winner and the incoming log record, never a blind last-write-wins overwrite.

The summary persists enough comparison state to make that decision: winning status, winning request version, winning timestamp, and whether the winner is a versioned terminal state. The incoming event replaces the stored winner only when it would have won in the full-log reconciliation:

- terminal request-state records with a request version are authoritative;
- among versioned terminal records, the highest request version wins, with timestamp as
  a tie-breaker;
- if no terminal versioned winner exists yet, the newest log timestamp wins.

When the winning state is terminal, the summary records a completion time. When the winning state is non-terminal, completion time is zero. Completion time is returned for display and status interpretation, but it is not part of the first `List` time-window predicate.

## Query and Index Design

The summary table exists to serve the exact List query, not merely to avoid replaying logs. The first query shape is:

- equality on `queue`;
- admission-time bounds: `started_at_ms >= start_time_ms` and `started_at_ms < end_time_ms`;
- optional current-status filter;
- keyset order by `started_at_ms, request_id` ascending or descending;
- bounded `LIMIT page_size + 1`.

The table must not rely on the primary key `(queue, request_id)` alone. That key supports point reads and conditional updates, but it requires a queue-wide scan and sort for the List query as retained data grows.

For the first API, the SQL implementation should retain the point-write primary key `(queue, request_id)` and add a queue/admission-time index beginning with `(queue, started_at_ms, request_id)`. This enables the admission-time predicate, requested ordering, and cursor continuation to use one bounded index range scan for both supported sort modes. The implementation should use `EXPLAIN` against representative queries before finalizing the schema.

Optional current-status filtering may cause the backend to examine more than `page_size + 1` rows within the requested admission window, but it must not require scanning requests admitted outside that bounded window. Do not add speculative status indexes blindly. The implementation RFC/test plan must name the supported query shapes and demonstrate that their plans do not degrade into a queue-wide filesort or full scan under expected retention.

## Pagination

`List` should be cursor-paginated. Offset pagination is the wrong fit because the underlying set changes while users page through it.

The cursor should be opaque to clients and tied to the original query shape: queue, time window, normalized status filter, sort order, and the last row seen. Reusing a cursor with a different queue, time window, status filter, or sort order should be rejected.

The cursor contains both `started_at_ms` and `request_id`. `started_at_ms` alone is not unique, and `request_id` alone cannot continue a stateless `(started_at_ms, request_id)` keyset scan. The two fields are the complete last-seen ordering key. Internally, an absent cursor is represented by a nil pointer; a non-nil cursor carries that complete ordering key.

Default page size should be modest. The API should cap page size so a single UX request cannot force an unbounded queue scan.

## Retention

The first retention target is 30 days after completion. Non-terminal requests must not be purged by age alone because their current status remains useful for point lookup and because a later admission-window query may still include them when the requested window covers their admission time.

Terminal summaries, their request contexts, and detailed logs can expire 30 days after completion. Detailed logs may have a separate policy later only if the UX no longer needs timeline/debug information for the same period.

## Flow

```
   ┌─────────────────────────────────────────────────────┐
   │ gateway:Land                                        │
   │   validate immutable admission data                 │
   │   create RequestContext + initial RequestSummary    │
   │   persist accepted RequestLog                       │
   │   publish downstream work only after those writes   │
   └─────────────────────────────┬───────────────────────┘
                                 │
   ┌─────────────────────────────▼───────────────────────┐
   │ gateway:log consumer                                 │
   │   insert immutable RequestLog idempotently           │
   │   load context + summary                             │
   │   reconcile one log event in application code        │
   │   conditional-write next RequestSummary              │
   │   return errors so projection failures retry         │
   └─────────────────────────────┬───────────────────────┘
                                 │
   ┌─────────────────────────────▼───────────────────────┐
   │ gateway:List                                         │
   │   validate queue + required time window + statuses   │
   │   validate sort + cursor query shape                 │
   │   execute indexed summary page query                 │
   │   return current request summaries                   │
   └─────────────────────────────────────────────────────┘
```

## Why Not Reuse `Status`

`Status` is a point lookup: one `sqid`, one current answer. Keeping it narrow makes it cheap and predictable for polling and integrations.

`List` is a collection query: one queue, one time window, many request summaries. It needs pagination, time filtering, optional status filtering, and a read model shaped for queue UX. Those semantics do not belong in `Status`.

## Why Not Copy Context Into Every Log

Self-contained events are useful when every producer can reliably own every field. That is not true here: later pipeline stages may have only a request ID and state data, while the original queue and change URIs are known at admission. Copying context into every log would either force unnecessary request lookups at all producers or allow partial/inconsistent payloads.

A standalone immutable context record makes the ownership boundary explicit. It gives the projection enough data to build List rows without coupling status producers to the full request shape.

## Why Not Build List From Logs On Read

Replaying logs is acceptable for a single-request Status query because it begins with one known request ID. A queue List query first needs the set of relevant requests, then must deduplicate and reconcile potentially many event streams before it can filter, sort, and paginate. Repeating that work for every page is the same projection cost paid on the read path, without an indexable current-state row.

The summary read model moves that bounded reconciliation work to event ingestion and gives List a queryable, paginated data shape.

## Why Not Return Timelines
