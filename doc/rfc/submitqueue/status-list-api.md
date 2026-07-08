# Gateway Status and List APIs

## RFC Status

Proposed.

This RFC replaces the unimplemented design in [list-api.md](list-api.md). It defines the gateway-owned request context and materialized status model used by both `Status` and `List`.

## Problem

The gateway currently stores an append-only request log and reconciles it at read time for `Status`. Request-log entries contain status, error, and display metadata, but they do not contain the immutable request context needed by user-facing APIs: queue name, submitted change URIs, and the time the gateway received the request.

The required read patterns are:

1. Look up the current status and request context by sqid.
2. Look up the current status and request context by change URI.
3. List requests received for one queue during a bounded time range.

The append-only request log is not shaped for the second or third query. Serving `List` by scanning and reconciling logs at runtime would also make query cost proportional to historical log volume. The gateway therefore needs a write-time materialized read model in addition to the request log.

## Decisions

1. `Status` accepts exactly one selector: sqid or change URI.
2. A change URI lookup returns all requests containing that exact URI, ordered by receipt time descending.
3. Change URIs are treated as globally meaningful identifiers. `Status` does not require a queue when selecting by URI.
4. `List` accepts one queue and a required receipt-time range. It does not accept sqid or URI selectors.
5. The `List` time range is based only on gateway receipt time, not lifecycle overlap.
6. `Status` and `List` return the same materialized current state and immutable request context. Neither endpoint returns the request-log timeline.
7. Materialization happens on write. Reads do not reconcile the append-only log at request time.
8. The data model uses separate query-shaped stores rather than secondary indexes.
9. Existing rows are not backfilled. Requests received before rollout may be unavailable through the new read model.
10. Retention and pruning are outside the scope of this RFC.

## Vocabulary

`sqid` is the public API name for a SubmitQueue request identifier. Storage contracts may call the same value `request_id`.

`received_at_ms` is the immutable gateway timestamp captured after Land passes synchronous request validation and the gateway begins persisting the request. It is not the time at which the orchestrator validates, accepts, or completes the request.

## API Contract

### Status

`Status` requires exactly one selector: sqid or change URI. An unset selector or an explicitly selected empty value is invalid.

An sqid lookup returns exactly one request summary when found. A change URI lookup returns every matching request summary ordered by `(received_at_ms DESC, sqid DESC)`. The sqid tie-breaker makes the result deterministic when requests share a millisecond.

Both selector modes return the same response shape: a list of request summaries. Each summary contains sqid, queue, change URIs, receipt time, current status, last error, and display metadata.

The URI result is intentionally unpaginated and has a hard maximum of 100 requests. A change is expected to have only a small number of SubmitQueue requests, generally fewer than one hundred. Exceeding the maximum returns an error rather than silently truncating the result.

### List

`List` is a queue-scoped request receipt history query. It requires a queue and a bounded half-open receipt-time interval. Both bounds are required, and the start must be less than the end.

Results are ordered by `(received_at_ms DESC, sqid DESC)`, so the newest requests are returned first. Pagination uses an opaque token bound to the original queue and time range plus the last returned ordering tuple. Reusing a token with different query parameters is invalid.

Page size is optional and subject to a server default and maximum.

New requests may appear before the first page while a caller is paging. The immutable tuple cursor prevents duplicates within the caller's traversal.

The first version does not filter by current status. Current status is mutable, while the queue receipt projection is ordered by immutable receipt time. Efficient status filtering requires an additional application-maintained membership projection keyed by queue, status, receipt time, and sqid. That projection is deferred until a concrete server-side filtering use case justifies its write and reconciliation cost. Clients may filter a returned page for presentation, but client-side filtering is not equivalent to a server-side filtered query.

## Errors

Invalid selectors, invalid time ranges, invalid page sizes, invalid page tokens, and URI results above the hard maximum are user errors. A lookup with no matching request returns a not-found user error.

The transport representation of these errors follows the gateway-wide RPC error contract and is outside the scope of this RFC. This RFC does not introduce an endpoint-specific wire error model.

## Gateway-Owned Read Model

The gateway owns the append-only request log and three new logical read models. The orchestrator's request and change stores are pipeline working state with different retention semantics, so neither API reads them.

### Request Summary by Sqid

The authoritative request summary is keyed by sqid. It contains immutable request context plus the current materialized request-log winner and the reconciliation state needed to compare a later log entry without rereading historical logs.

The immutable context is queue, change URIs, and receipt time. The mutable response state is status, last error, and metadata. Optimistic-lock state is internal to the projection and is not part of the API response.

The sqid key supports authoritative lookup and conditional status updates for one request without a secondary index.

### Request Summaries by Queue

The queue projection is logically keyed by `(queue, received_at_ms, sqid)` and must support a bounded descending scan over `(received_at_ms, sqid)`. A backend may satisfy that contract with a reverse range scan or by descending-encoding the ordered key components; cursors always carry the original receipt time and sqid values.

The row duplicates the complete `List` response deliberately. A page is served by one bounded range scan rather than one follow-up authoritative-summary read per result. Status updates propagate from the authoritative sqid summary to this projection.

The logical key covers the `List` queue predicate, receipt-time range, newest-first ordering, and complete keyset cursor in one bounded scan.

### Requests by Change URI

The URI reverse mapping is logically keyed by `(change_uri, received_at_ms, sqid)` and must support a bounded descending scan over `(received_at_ms, sqid)`. As with the queue projection, a backend may use a reverse range scan or descending-encoded key components while exposing cursors and results in the original values. The mapping contains immutable lookup data and does not duplicate mutable status fields.

The mapping repeats `received_at_ms` because receipt time is part of the promised newest-first ordering. This allows the gateway to perform a bounded ordered scan before resolving the matching authoritative summaries. Without receipt time in the mapping, the gateway would have to fetch and sort every request associated with a URI before enforcing the result maximum.

The logical key supports the bounded `Status(change_uri)` newest-first scan and deterministic sqid tie-breaker without fetching every matching summary first.

The URI is stored in the canonical form received from the validated Land request. URI normalization rules belong to the change contract or source-control integration and are not introduced by this read model.

## Write Flows

### Land Receipt

After synchronous validation, Land generates the sqid and one receipt timestamp. The gateway persists the authoritative summary, URI mappings, and queue projection before appending the initial `accepted` request log and before publishing the request to the orchestrator.

This ordering guarantees that immutable request context exists before the request can produce later status logs or enter the asynchronous pipeline. The logical writes are independent and must be safe to retry for the same sqid and values; conflicting duplicate data is an error.

### Request-Log Materialization

Every gateway request-log persistence path uses the same materialization component. It appends the audit log, compares the incoming entry with the authoritative summary, conditionally advances the winner, and propagates the authoritative value to the queue projection.

The winner comparison preserves the existing `Status` behavior:

1. A terminal request-state entry with a positive request version beats every non-terminal or unversioned winner.
2. Between versioned terminal entries, the greater request version wins.
3. Equal terminal versions use the greater log timestamp as a tie-breaker.
4. When no versioned terminal winner exists, the greater log timestamp wins.

Materialization uses optimistic concurrency so stale or out-of-order consumers cannot replace a newer winner. Version arithmetic and reconciliation decisions belong to the materialization component; stores perform only mechanical creates, reads, conditional updates, and bounded page queries.

## Read Flows

### Status by Sqid

The gateway reads the authoritative summary by sqid and returns its current materialized state and immutable context. It does not fall back to parsing the sqid, reconciling logs, or reading orchestrator stores.

### Status by Change URI

The gateway performs a bounded newest-first scan of the URI reverse mapping and then reads the authoritative summary for each resolved sqid. Results preserve the mapping order. No mapping is a not-found result; exceeding the 100-request maximum is an error. A mapping whose authoritative summary is missing is an internal consistency error that fails the lookup; it is not returned as user-facing not-found and is not silently omitted.

### List by Queue and Receipt Time

The gateway performs one bounded range scan of the queue projection using queue, receipt-time bounds, and an optional keyset cursor. The ordering key is immutable, so later status updates cannot move an item across an issued cursor.

## Consistency

The request log remains the append-only audit record. The authoritative summary and queue projection are eventually consistent views of its winning current state.

Because the authoritative summary and queue projection are separate writes, a short interval can exist where `Status` and `List` show different statuses. Retried materialization repairs the queue projection from the authoritative sqid summary until both converge. Neither API reconciles logs during reads to hide this interval.

Request context has a stronger guarantee than status convergence: it is persisted before the initial log and before the request is published to the orchestrator. Once a queue projection is visible, its queue, sqid, change URIs, and receipt time are complete.

## Compatibility and Rollout

No existing SQL table requires an in-place breaking modification. The request log remains unchanged, and the new query patterns use additive gateway-owned read models.

There is intentionally no backfill from orchestrator working state or historical request logs. Deployments may create the new stores empty and begin populating them for new Land requests. This creates a behavioral cutoff: requests received before rollout are not guaranteed to resolve through the new `Status` implementation or appear in `List`.

The Status request change preserves the existing sqid field number but may be source-breaking because generated clients represent the selector as a protobuf `oneof`. Changing Status from singular status fields to a list of request summaries is a breaking response change. The List RPC is additive.

## Rejected Alternatives

### Put Request Context in Every Log Entry

Repeating queue and change URIs in every append-only row increases write volume and still does not provide a direct queue or URI query path. Immutable context belongs in the summary and reverse-mapping read models.

### Materialize List at Read Time

Scanning request logs, grouping by sqid, and reconciling each group makes latency and database work grow with log history. It also cannot efficiently find requests by queue because queue is not part of the log key.

### Use One Store with Secondary Indexes

A single summary store indexed by queue and each change URI is awkward because a request has multiple URIs, and it makes backend contracts depend on secondary-index capabilities. Separate query-shaped stores keep each operation expressible as a key lookup or bounded ordered scan across SQL, key-value, and document implementations.

### Filter List by Current Status

Filtering the existing queue receipt scan would require server-side filtering outside the storage contract or produce sparse and misleading pagination. Efficient filtering requires a separate mutable membership projection keyed by queue, status, receipt time, and sqid. The first version defers that projection.

### Let List Accept Sqid or URI

Sqid lookup and the small URI submission-history lookup belong to `Status`. `List` has one purpose: enumerate queue receipts in a bounded time range. Keeping these contracts separate gives each storage operation one predictable access pattern.
