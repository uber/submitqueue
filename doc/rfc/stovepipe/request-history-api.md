# Stovepipe Request History API

## Contract

Stovepipe exposes retained request history through queue-scoped point lookups by request ID and exact URI. Both selectors return complete ordered events and the current materialized `RequestSummary`. The published [protobuf contract](../../../api/stovepipe/proto/stovepipe.proto) defines the fields.

`GetRequestHistoryByIDResponse` contains `events` and `request_summary`. `GetRequestHistoryByURIResponse` contains repeated histories, each with its request ID, events, and `request_summary`. These summary fields are additive; existing event fields and field numbers remain unchanged.

Both methods require a queue. A selector belonging to another queue is not resolved across shards. URI lookup retains its plural response; the insert-once `RequestURIStore` resolves exactly one request per `(queue, URI)`.

## Selection and Current State

Request-ID lookup validates the queue and ID, then reads that queue's `RequestLogStore` and `RequestSummaryStore` by request ID. URI lookup validates the queue and URI, resolves the existing request-URI primary-key mapping, and uses the same history reader. It also verifies that the summary's URI matches the selector.

At least one retained log record defines the existence of a history. A summary alone does not establish a retained history, and the operational `Request` entity is not required for either read. A returned summary must match the selected queue and request ID; disagreement is an infrastructure consistency error.

The summary is the existing current lifecycle projection maintained by the request-log materializer. It includes request identity, queue, commit URI, baseline URI, current state, state timestamp, acceptance timestamp, outcome reason, and recorded project outcome counts. The mapper reuses the public summary representation exposed by List, omitting internal storage versions.

History is read before its summary. The two reads are eventually consistent, not a transactional snapshot: the current summary can represent newer state than the retained events returned by the earlier read, or lag a newly appended state event. The API does not replay the returned events to derive a summary or fabricate missing occurrences from current state.

A successful response always includes the summary. Initial state-log persistence precedes summary creation, so a concurrent read or a failed projection write can temporarily expose history without its summary. That gap fails the lookup with a retryable infrastructure error; ingestion retries complete the projection. Other summary-read failures also fail the lookup.

## Events and Ordering

The append-only history model is defined by [Stovepipe Request Log](request-log.md). Event IDs are opaque; clients may compare them but never parse them. Request-state, outcome-reason, and event strings tolerate additive vocabulary changes.

Events are returned by `(timestamp_ms ASC, event_id ASC)`. URI histories are ordered by numeric request ID ascending. Timestamps determine display order; persisted request versions determine causal ordering internally and are not exposed as history-event fields. Later repair may insert an older occurrence into its chronological position.

## Retention and Errors

History, request-summary, and request-URI mapping retention must support the same advertised lookup period. Neither endpoint promises a lifetime longer than the records required by its selector.

- An empty queue or selector is invalid.
- A request ID without retained logs is not found, including when its summary exists.
- An unknown queue-scoped URI mapping is not found.
- A URI mapping without retained logs is an infrastructure inconsistency.
- An absent summary is a retryable infrastructure error; summary identity or URI mismatches are infrastructure consistency errors.
- Storage failures propagate with their underlying error classifications.

Authorization follows the same queue policy as other Stovepipe reads. Possession of a request ID alone does not bypass queue authorization.

## Testing

Tests cover matching events and summaries across selectors, required summaries and retryable materialization gaps, current summaries independent of retained event subsets, queue and URI consistency, storage-read failures, histories absent despite a stored summary, stable event ordering, and unknown future state and outcome-reason strings.

## Alternatives Considered

Replaying returned history into current state is rejected because retention can remove events and the existing materialized summary already owns that projection. Querying history directly by URI is rejected because it would require an attribute query in `RequestLogStore`; the existing mapping resolves the primary request key for any backend. Returning events without a summary is rejected because a successful lookup must include current request status; callers can retry a temporary projection gap.
