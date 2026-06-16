# Gateway List API

Design notes for a gateway `List` API that powers a queue-scoped UX for
observing SubmitQueue requests over a time window.

This document captures **design decisions and rationale only**.

## Problem

Users need to inspect what happened in a queue during a time window: which
requests were still running, which reached a terminal state, and what useful
state each request is currently in. The existing `Status` API answers that
question for one `sqid`; the UX needs the same gateway-owned view, but across a
queue and time range.

The gateway owns the request log. The orchestrator may emit request-log events,
but it does not persist or read the log. `List` should preserve that ownership
boundary and must not read orchestrator-owned working tables.

## API Shape

`List` is a read-only gateway RPC, named tersely to match `Land`, `Cancel`, and
`Status`.

At a high level:

- **Input** — queue name, time window, optional status filters, sort order, and
  pagination cursor.
- **Output** — a page of request summaries and the next cursor.

Each request summary should include:

- `sqid`
- queue
- current customer-facing status
- change URIs submitted with the request
- last error, if any
- display/debug metadata
- time the request entered SubmitQueue
- time the visible state last changed
- time the request completed, if terminal
- whether the request is terminal

The summary intentionally exposes gateway/user-facing lifecycle information, not
orchestrator implementation details such as batch IDs, internal request states,
or speculation-tree structure.

## Time Window Semantics

The time window is a lifecycle-overlap filter, not a "started during this
window" filter.

A request belongs in `[T1, T2)` when it was active at any point in that interval:

- it started before `T2`, and
- it either has not completed, or completed at or after `T1`.

This is the behavior the UX needs for questions like "what was running between
10:00 and 10:30?" A request that began at 09:55 and completed at 10:05 should
appear. A request that began at 10:20 and is still running should appear. A
request that completed at 09:59 should not.

`List` returns the request's **current** reconciled status at read time for rows
that match the window. It is not a historical "status as of T2" API. A historical
snapshot API would be a different product shape and should be designed
separately if needed.

## Status Filtering

`List` should support filtering by the same customer-facing status strings that
`Status` returns: examples include `accepted`, `validating`, `building`,
`landing`, `landed`, `error`, `cancelling`, and `cancelled`.

This keeps the API stable at the same abstraction level as `Status`. Clients do
not need to learn an internal enum or translate orchestrator state-machine
values into display states.

The status filter applies to the request's **current** reconciled status after
the queue/time-window match has been computed. It does not mean "requests that
ever had this status during the window." That historical event query belongs
with a timeline/debug API, not the queue summary list.

The filter should accept multiple statuses so the UX can ask for groups such as
"currently active" or "terminal outcomes" without making separate RPC calls. The
server should validate status strings against the public status vocabulary it
can emit; unknown statuses are caller errors rather than silent misses.

## Sorting

`List` should expose sorting as an enum, not as free-form field names. The first
supported sort modes are admission-time orderings:

```
enum ListSort {
  LIST_SORTED_UNSPECIFIED = 0; // server default: LIST_SORTED_ADMITTED_ASC
  LIST_SORTED_ADMITTED_ASC = 1; // FIFO: started_at_ms ASC, sqid ASC
  LIST_SORTED_ADMITTED_DESC = 2; // newest admissions first: started_at_ms DESC, sqid DESC
}
```

The default should be FIFO/admission order so the first page gives the clearest
answer to "what is at the head of the queue?" for simple queue views. Clients
that want a recent-activity/history view can request
`LIST_SORTED_ADMITTED_DESC`.

These names intentionally use "admitted" rather than "queue position." Admission
order is gateway-owned and can be derived from request-log/summary data. Queue
position is a stronger scheduler/backend concept: it may differ from FIFO once
batching, speculation, cancellation, priority, or retries are involved.

TODO: if a backend supports a real queue-position signal, add a nullable
`queue_position` summary column and a corresponding enum value such as
`LIST_SORTED_QUEUE_POSITION_ASC`. The RFC should define exactly which requests
have positions, when positions change, how terminal requests sort, and how
cursor pagination behaves over a mutable position before exposing that mode.

## Read Model

Serving `List` directly from the append-only request log would force the gateway
to scan and reconcile many log rows per request. That is the wrong shape for a
queue dashboard.

The gateway should maintain a request-summary read model derived from the
request log. Every request-log write updates two gateway-owned views:

- the immutable request log, used for audit/debug history and point
  reconciliation;
- the mutable request summary, used for bounded queue/time-window listing.

The summary row is a materialized current view of the same state that `Status`
would report. `Status` may continue reading and reconciling from the log during
rollout; the important invariant is that both views use the same reconciliation
rules.

This is deliberately a query store, unlike the mostly key-oriented stores used
by the pipeline. Its boundary should be page-in/page-out: queue, time window,
statuses, sort, cursor, and limit in; rows plus next cursor out. The backend owns
the indexing strategy for lifecycle overlap and supported sort modes. For SQL,
avoid an unindexed open-ended OR by representing "still running" with an
index-friendly sentinel completion time or by splitting active and completed
scans.

Every request-log persistence path must update this read model through the same
helper: direct gateway writes such as `Land` and `Cancel`, plus the gateway log
sink that persists orchestrator-emitted events. The invariant is
`RequestLogStore.Insert` paired with a guarded summary upsert, not best-effort
ad hoc updates at each call site.

Request-log events should carry `queue` as first-class data. The log sink only
receives the log event, so relying on `sqid` parsing would make the read model
depend on an ID-format convention. Legacy backfills may parse queue from `sqid`
as a fallback, but new events should be queue-attributable at the source.

## Change URIs

Request summaries should include the change URIs submitted with the request. The
UX needs them to make each row recognizable and actionable without an additional
lookup.

To support this cleanly, the gateway must capture change URIs at request
acceptance time. `Land` already receives the change set before handing work to
the orchestrator, so it is the right boundary to persist that display data into
the gateway-owned request log and summary read model.

This should not be implemented by joining from `List` into orchestrator-owned
request tables. That would break the service ownership model and couple a UX
read path to pipeline internals.

For existing requests, change URIs are available only if they can be recovered
from gateway-owned data. If old request-log entries do not contain them, the
backfill can still build summaries, but those older rows will have empty change
URIs unless a separate one-time migration from an authoritative source is
accepted explicitly.

## Reconciliation

Request-log timestamps are useful for display and broad ordering, but they are
not always the strongest signal for "current state." Some log entries reflect
informational progress, while others reflect versioned request-state changes.

`Status` reconciles by reading all request-log rows at once. The summary must do
the equivalent incrementally, one incoming event at a time. Each update is a
guarded merge between the stored winner and the incoming log record, never a
blind last-write-wins overwrite.

The summary should persist enough comparison state to make that decision:
winning status, winning request version, winning timestamp, and whether the
winner is a versioned terminal state. The incoming event replaces the stored
winner only when it would have won in the full-log reconciliation:

- terminal request-state records with a request version are authoritative;
- among versioned terminal records, the highest request version wins, with
  timestamp as a tie-breaker;
- if no terminal versioned winner exists yet, the newest log timestamp wins.

When the winning state is terminal, the summary records a completion time. When
the winning state is non-terminal, completion time is empty and the request is
considered active for future time-window overlap.

## Pagination

`List` should be cursor-paginated. Offset pagination is the wrong fit because the
underlying set changes while users page through it.

The cursor should be opaque to clients and tied to the original query shape:
queue, time window, status filter, sort order, and the last row seen. Reusing a
cursor with a different queue, time window, status filter, or sort order should
be rejected.

Default page size should be modest. The API should cap page size so a single UX
request cannot force an unbounded queue scan.

## Retention

The first retention target is 30 days after completion. Non-terminal requests
must never be purged by age alone; a request that started 40 days ago and is
still running must appear in a current overlap query.

Terminal summaries and detailed logs can expire 30 days after completion.
Detailed logs may have a separate policy later only if the UX no longer needs
timeline/debug information for the same period.

## Flow

```
   ┌────────────────────────────────────────────┐
   │ gateway:Land / gateway:Cancel / log sink   │
   │   persist request-log event                │
   │   update request summary                   │
   └──────────────────────────┬─────────────────┘
                              │
                              ▼
   ┌────────────────────────────────────────────┐
   │ gateway:List                               │
   │   validate queue + time window + statuses  │
   │   validate sort + cursor query shape       │
   │   read summaries by lifecycle/status match │
   │   return page of current request summaries │
   └────────────────────────────────────────────┘
```

## Why Not Reuse `Status`

`Status` is a point lookup: one `sqid`, one current answer. Keeping it narrow
makes it cheap and predictable for polling and integrations.

`List` is a collection query: one queue, one time window, many request summaries.
It needs pagination, time filtering, optional status filtering, and a read model
shaped for queue UX. Those semantics do not belong in `Status`.

## Why Not Return Timelines

Timelines are useful for debugging, but they are not part of the first `List`
shape. Returning per-request histories in every list row would make page cost
scale with both the number of requests and the number of events per request.

The first API should return summaries only. If the UX later needs row expansion,
add a dedicated timeline/debug API that reads the append-only request log for one
`sqid`.
