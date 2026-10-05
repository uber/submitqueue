# Stovepipe List API

The [protobuf contract](../../../api/stovepipe/proto/stovepipe.proto) is included for review; controller and storage implementation are deferred.

## Proposal

`List(ListRequest) -> ListResponse` returns current request summaries for one queue, newest acceptance time first. This is a request listing, not scheduler position or historical state.

## Contract

| Request field | Type | Meaning |
| --- | --- | --- |
| `queue` | string | Required configured queue. |
| `accepted_at_or_after_ms` | optional int64 | Inclusive lower acceptance-time bound. |
| `accepted_before_ms` | optional int64 | Exclusive upper acceptance-time bound. |
| `page_size` | int32 | Default 50; maximum 200. Zero means default. |
| `page_token` | string | Empty for the first page; otherwise an opaque continuation. |

The response contains `requests`, `next_page_token`, and the two resolved time bounds. No matches returns an empty page; an empty continuation means no further row was observed.

Continuations repeat the same queue. Time bounds may be omitted or must match the token; page size may change. Tokens preserve the query bounds and continue exclusively after the last returned ordering key. Invalid inputs and mismatched tokens fail the request. Every page follows the service's queue-access policy.

## Ordering and Time Defaults

Order is descending `(accepted_at_ms, request_id)`, with bytewise descending request IDs only to break timestamp ties. Only requests with known acceptance times are included; numeric request-ID ordering is not offered.

All timestamps are Unix milliseconds. Time selection is `lower <= accepted_at_ms < upper`. Omitted lower defaults to `0`; omitted upper defaults to server `now`, sampled once on the first page and retained in the token. Require `0 <= lower < upper`; explicit future bounds are allowed. There is no mandatory window, implicit 24-hour cutoff, or maximum window width.

Returned state is current, not state as of the upper bound. Pagination is not a snapshot: concurrent materialization may affect membership and state.

## Request Summary

This is the wire representation of the existing domain `RequestSummary`, not another stored projection. The status RPC remains unchanged; it can later add this message as a nested field while retaining its existing flat fields for compatibility. Nested messages have independent field numbers. Summary `state_updated_at_ms` is lifecycle-only, unlike status `updated_at_ms`, which also includes validation results.

| Field | Type | Meaning |
| --- | --- | --- |
| `request_id` | string | Opaque request ID. |
| `queue` | string | Owning queue. |
| `change_uri` | string | Exact ingested URI. |
| `base_uri` | string | Selected baseline; empty before selection or for a full build. |
| `request_state` | string | Current lifecycle state: `accepted`, `processing`, `superseded`, `succeeded`, `failed`, or `cancelled`. |
| `state_updated_at_ms` | int64 | Timestamp of the represented state entry. |
| `accepted_at_ms` | optional int64 | Immutable timestamp of the original retained accepted entry. |
| `outcome_reason` | string | Reason for the represented state; empty when unavailable or inapplicable. |

The domain already defines a typed [RequestState](../../../stovepipe/entity/request.go). The wire field remains a string to match existing status/history APIs. States and reasons use the existing [public vocabulary](request-log.md#outcome-reasons); clients tolerate future values. Duplicate Ingest calls resolving to the same request produce one row.

## Data and Storage Work Required

The first six fields already exist in [RequestSummary](../../../stovepipe/entity/request_summary.go). Acceptance time and outcome reason already exist in retained logs but must be added to the summary. Acceptance time is acceptance-log time, not first RPC receipt time. No new producer signal or per-queue counter is needed.

Listing uses an immutable mapping keyed by `(queue, accepted_at_ms, request_id)`, then point-reads the corresponding summaries. This adds one small record per request and bounded extra reads, not another mutable status projection. Existing summary keys and public request IDs remain unchanged; no numeric-key migration is needed.

The mapping follows the repository's [storage contract](../../../submitqueue/extension/storage/README.md#key-value-contract): a needed alternate lookup is an explicit primary-key mapping, not a SQL secondary-index requirement. Maintain it idempotently after summary persistence and before dependent publication, using existing retries rather than cross-entity transactions.

## Coverage and Scope

Older requests without a known acceptance time or time mapping do not appear in List until repaired or backfilled. Compatible writers materialize both for newly accepted requests. Automatic historical backfill is not part of this proposal; incomplete older-history coverage is an accepted limitation.

V1 has no fixed retention duration or automatic pruning. State/verdict filters, recently-updated order, totals, scheduler position, and snapshot exports are deferred. Validation details and histories remain separate APIs.
