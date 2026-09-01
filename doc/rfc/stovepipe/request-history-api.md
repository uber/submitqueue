# Stovepipe Request History API

## Summary

Stovepipe exposes retained request history through queue-scoped point lookups by request ID and exact URI. URI lookup returns every retained request history associated with that commit; the current insert-once mapping produces exactly one. Both selectors return complete ordered events, and URI lookup groups them by request ID.

The API reads the append-only model defined by [Stovepipe Request Log](request-log.md) directly. It does not replay history into current state or introduce a second persisted history projection. Current commit status remains a separate read concern derived from operational entities rather than request history.

## API Coverage

The API supports the same selectors as [SubmitQueue Gateway Request History APIs](../submitqueue/history-api.md):

1. request ID, for callers retaining the receipt returned by `Ingest`;
2. exact URI, for callers starting from a commit identity.

Both methods require a queue because Stovepipe storage and authorization are queue-scoped. A selector belonging to another queue is not found rather than resolved across shards.

SubmitQueue's URI method returns several histories because the same change may be submitted repeatedly. Stovepipe uses the same plural response shape. Its current insert-once `RequestURIStore` permits only one request per `(queue, URI)`, so the initial implementation returns one history. If revalidation later retains multiple attempts, the URI index must enumerate their request IDs, but the public contract does not need to change.

## Representative Contract

The final protobuf receives a separate compatibility review before implementation. Its representative shape is:

```proto
message GetRequestHistoryByIDRequest {
    string queue = 1;
    string request_id = 2;
}

message GetRequestHistoryByURIRequest {
    string queue = 1;
    string uri = 2;
}

message HistoryEvent {
    string event_id = 1;
    int64 timestamp_ms = 2;
    oneof occurrence {
        string request_state = 3;
        string event = 4;
    }
    string superseded_by_request_id = 5;
    string build_id = 6;
    string outcome_reason = 7;
}

message RequestHistory {
    string request_id = 1;
    repeated HistoryEvent events = 2;
}

message GetRequestHistoryByIDResponse {
    repeated HistoryEvent events = 1;
}

message GetRequestHistoryByURIResponse {
    repeated RequestHistory histories = 1;
}

service Stovepipe {
    rpc GetRequestHistoryByID(GetRequestHistoryByIDRequest) returns (GetRequestHistoryByIDResponse) {}
    rpc GetRequestHistoryByURI(GetRequestHistoryByURIRequest) returns (GetRequestHistoryByURIResponse) {}
}
```

The response shapes and `HistoryEvent` name match their SubmitQueue equivalents: request-ID lookup returns events directly, while URI lookup groups each request's events under a `RequestHistory` in a repeated field. Stovepipe retains `GetRequestHistoryByURI` because URI is its VCS-agnostic commit identity; SubmitQueue's corresponding selector is specifically named `GetRequestHistoryByChangeURI` after its change model.

Event IDs are opaque. Clients may compare them but never parse their format. Request-state and event values are strings so clients can tolerate additive vocabulary changes. Request version remains persisted for internal transition ordering but is not part of the public event, matching SubmitQueue's API boundary.

## Selection Flow

Request-ID lookup validates the queue and ID, loads the queue's `Request` to validate the selector, and lists its `RequestLogStore` records. Its response projects only the ordered events, matching SubmitQueue's equivalent API.

URI lookup currently resolves one request ID from the existing `RequestURIStore` primary key and delegates to the same request-ID path. Supporting multiple retained attempts later requires the URI index to enumerate their request IDs; each history still uses primary-key reads for its Request and log. The API does not scan the request log by URI. A missing mapping is not found; a mapping whose Request is missing is an internal consistency error.

Every request-URI mapping must be repaired and retained with its Request and history. Otherwise URI lookup could lose coverage while request-ID lookup still succeeds. Each `RequestHistory` contains only the selected request ID and its events; URI, build strategy, and base URI remain resolvable from the Request rather than being duplicated in the history response.

## Public Projection

Each stored `RequestLog` maps to exactly one public `HistoryEvent`. The API does not deduplicate, coalesce, or infer missing occurrences.

History events representing state changes set `request_state` and preserve each durable transition. The public vocabulary is:

| Public request state | Meaning |
|---|---|
| `accepted` | The request was durably admitted. |
| `processing` | The request was admitted to validation with a selected strategy. |
| `superseded` | A newer request replaced this request before validation completed. |
| `succeeded` | The request's validation work completed successfully. |
| `failed` | The request's validation work failed or could not continue. |
| `cancelled` | The request's validation work was cancelled. |

These strings intentionally match the current domain states one-to-one, but they are a stable public history vocabulary: an internal refactor cannot rename or reinterpret an existing wire value. In particular, `succeeded` and `failed` remain distinct rather than collapsing into a derived snapshot phase such as `finalizing`.

Build events set `event` and decode `build_id` from the reserved request-log metadata key. `validation_fact_recorded` records that a fact was established, while its degree remains internal metadata until a typed public projection is designed. Superseded state events similarly decode `superseded_by_request_id`. The `occurrence` oneof makes state and event mutually exclusive without a redundant type field. A terminal Request state entry never substitutes for the fact event.

The raw `RequestLog.Metadata` map, unknown metadata keys, dependency errors, credentials, and stack traces are not exposed. `outcome_reason` uses a bounded public vocabulary.

## Materialization

SubmitQueue's `PersistLog` operation performs two jobs after receiving a log message:

1. append the `RequestLog` row that is itself returned as history;
2. consider status rows for a gateway-owned `RequestSummary`, resolve out-of-order candidates using request version, terminal precedence, and timestamp, then update URI and queue-list projections.

Event rows remain in SubmitQueue history but never participate in current-status materialization. The materializer exists because the gateway owns public reads but cannot read the orchestrator's mutable Request store.

Stovepipe has no equivalent ownership gap. The same service owns the queue-scoped `Request`, `ValidationFact`, request-URI mapping, and request-log store. Operational reads use their owning entities, while request history reads retained log records directly.

Stovepipe therefore does not add `RequestSummary`, replay history to determine current state, or materialize another history table. The controller performs only an in-memory wire projection from stored log records to protobuf messages. This avoids a second winner-selection algorithm competing with Request CAS state.

## Ordering and Consistency

Events are returned by `(timestamp_ms ASC, event_id ASC)`. URI histories are ordered by the numeric request-ID counter ascending, then request ID ascending. Timestamps are display order, not conflict resolution. Persisted Request versions provide internal causal ordering for state transitions, while Build identity and write-once terminal status ensure one triggered and one finished occurrence per build.

History may briefly lag a source entity between the source write and history creation. The pipeline blocks its dependent handoff during that window, and retry or reconciliation repairs the missing entry. The read API never fabricates an occurrence from current state.

Request-ID lookup and the corresponding history within URI lookup return the same stored records and event ordering. A later repair may insert an older occurrence into its correct chronological position.

## Retention

Request-ID lookup returns all currently retained events for the selected request. URI lookup returns every retained request history associated with the exact URI. This matches SubmitQueue's request-history response shapes. The bounded lifecycle vocabulary and reasonable per-request build limits keep each history modest.

History, `Request`, and request-URI mapping retention must support the same advertised lookup period. The API does not promise a lifetime longer than every record required by its selector. The initial rollout exposes only requests accepted after all history writers and repair paths are active; older requests are outside the lookup period and are not backfilled.

## Errors and Authorization

- Empty queue or selector is invalid.
- An unknown request ID or URI, including one scoped to the wrong queue, is not found.
- A URI mapping whose Request is missing and a request within the advertised history lookup period whose required history is missing are internal consistency errors.
- Retryable storage failures are unavailable; context cancellation and deadline errors retain their canonical codes.

Authorization follows the same queue policy as other Stovepipe reads. Possession of a request ID alone does not bypass queue authorization.

## Testing

Contract and controller tests cover:

- matching request-ID events and the corresponding history inside the plural URI response;
- deterministic equal-timestamp ordering;
- state, build-event, and fact-event public mapping;
- queue isolation and cross-queue not found;
- unknown selectors versus dangling mappings;
- a repaired older occurrence appearing in chronological position;
- unknown future request-state and event strings remaining readable.

## Alternatives Considered

### Materialize current status from history

Rejected because `Request` and `ValidationFact` already own current operational state and verdicts. Replaying history would add another reconciliation path without enabling either selector.

### Query history directly by URI

Rejected because it would add query-by-attribute capability to `RequestLogStore`. The existing exact request-URI mapping resolves the primary request key for any backend.

### Return only the authoritative history by URI

Rejected because a singular response would require another API or a breaking shape change if Stovepipe later retains multiple validation attempts for one URI. The repeated response accurately contains one history under the current invariant and can grow without changing existing clients.
