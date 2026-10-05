# Stovepipe List API

The [protobuf contract](../../../api/stovepipe/proto/stovepipe.proto) is included for review; controller and storage implementation are deferred.

## Proposal

`List(ListRequest) -> ListResponse` returns current request summaries for one queue. Default to newest request-ID order; offer acceptance-time order when callers want timestamp selection. This is a request listing, not scheduler position or historical state.

## Contract

| Request field | Type | Meaning |
| --- | --- | --- |
| `queue` | string | Required configured queue. |
| `order` | ListOrder | `REQUEST_ID_DESC = 0` (default) or `ACCEPTED_AT_DESC = 1`. |
| `accepted_at_or_after_ms` | optional int64 | Inclusive lower acceptance-time bound; time order only. |
| `accepted_before_ms` | optional int64 | Exclusive upper acceptance-time bound; time order only. |
| `page_size` | int32 | Default 50; maximum 200. Zero means default. |
| `page_token` | string | Empty for the first page; otherwise an opaque continuation. |

The response contains `requests`, `next_page_token`, and, in time mode, the two resolved time bounds. No matches returns an empty page; an empty continuation means no further row was observed.

Continuations repeat the same queue and order. Time bounds may be omitted or must match the token; page size may change. Tokens preserve the query bounds and continue exclusively after the last returned ordering key. Invalid inputs and mismatched tokens fail the request. Every page follows the service's queue-access policy.

## Ordering and Time Defaults

- `REQUEST_ID_DESC`: descending numeric per-queue request sequence. Includes all retained summaries, even those with unknown acceptance time. No time cutoff; supplied time bounds are invalid.
- `ACCEPTED_AT_DESC`: descending `(accepted_at_ms, request_id)`, with bytewise request-ID ties. Includes only known acceptance times. Request-ID order cannot substitute for this: IDs are allocated before acceptance is recorded, so timestamp order can differ.

All timestamps are Unix milliseconds. Time selection is `lower <= accepted_at_ms < upper`. Omitted lower defaults to `0`; omitted upper defaults to server `now`, sampled once on the first page and retained in the token. Require `0 <= lower < upper`; explicit future bounds are allowed. There is no mandatory window, implicit 24-hour cutoff, or maximum window width.

Returned state is current, not state as of the upper bound. Pagination is not a snapshot: concurrent materialization may affect membership and state.

## Request Summary

This is the wire representation of the existing domain `RequestSummary`, not another stored projection. Its first five fields match the names, types, and numbers in `GetProjectStatusByURIResponse`; numbers 6–10 are reserved to avoid colliding with status-only data. New summary fields use 11–13. The status RPC remains unchanged; refactoring it to share this message is deferred and must preserve its existing wire fields. Summary `state_updated_at_ms` is lifecycle-only, unlike status `updated_at_ms`, which also includes validation results.

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

The domain already defines a typed [RequestState](../../../stovepipe/entity/request.go). The wire field remains a string to match existing status/history APIs; replacing it with a protobuf enum would not be wire-compatible. States and reasons use the existing [public vocabulary](request-log.md#outcome-reasons); clients tolerate future values. Duplicate Ingest calls resolving to the same request produce one row.

## Data and Storage Work Required

The first six fields already exist in [RequestSummary](../../../stovepipe/entity/request_summary.go). Acceptance time and outcome reason already exist in retained logs but must be added to the summary. Acceptance time is acceptance-log time, not first RPC receipt time. No new producer signal or per-queue counter is needed.

Two bounded access paths are needed:

- Request-ID listing reads summaries directly. The current string key sorts lexically, not numerically, so it needs an order-preserving primary-key representation and compatible migration.
- Time listing uses an immutable mapping keyed by `(queue, accepted_at_ms, request_id)`, then point-reads the corresponding summaries. This adds one small record per request and bounded extra reads, not another mutable status projection.

The mapping follows the repository's [storage contract](../../../submitqueue/extension/storage/README.md#key-value-contract): a needed alternate lookup is an explicit primary-key mapping, not a SQL secondary-index requirement. Maintain it idempotently after summary persistence and before dependent publication, using existing retries rather than cross-entity transactions.

## Coverage and Scope

Legacy summaries without a verified accepted log retain an absent acceptance time: request-ID mode includes them; time mode excludes them. Complete time-mode coverage begins after compatible writers are enabled; older requests need context and mapping repair. Automatic historical backfill is not part of this proposal.

V1 has no fixed retention duration or automatic pruning. State/verdict filters, recently-updated order, totals, scheduler position, and snapshot exports are deferred. Validation details and histories remain separate APIs.
