# Stovepipe List API

## Status and Decisions

Proposed contract; the RPC and storage changes are not implemented.

List serves retained request summaries for one queue. Its default is numeric request-ID ingest order, newest first, with 50 rows per page. Callers can instead select acceptance-time order and an optional time range. Superseded requests are included, and duplicate Ingest calls resolving to one request produce one row.

Acceptance time is copied from the original retained accepted event into the existing summary. No new producer signal or per-queue counter is needed. Request-ID listing reads the summaries directly. Acceptance-time listing uses a small immutable time-to-request mapping and then reads those same summaries. Mutable lifecycle state has one materialized copy.

## RPC Contract

The RPC is `List(ListRequest) -> ListResponse` on the existing Stovepipe service. Published protobufs belong under `api/stovepipe/proto/`; controller inputs and results remain domain values.

| Request field | Type | Meaning |
| --- | --- | --- |
| `queue` | string | Required exact configured queue; non-empty and at most 255 bytes. |
| `order` | ListOrder enum | `REQUEST_ID_DESC = 0` is the default; `ACCEPTED_AT_DESC = 1` selects acceptance-time order. Other values are invalid. |
| `accepted_at_or_after_ms` | int64 with presence | Optional inclusive lower acceptance-time bound, in Unix milliseconds; valid only with acceptance-time order. |
| `accepted_before_ms` | int64 with presence | Optional exclusive upper acceptance-time bound, in Unix milliseconds; valid only with acceptance-time order. |
| `page_size` | int32 | Zero or omitted selects 50; valid nonzero values are 1 through 200. |
| `page_token` | string | Opaque continuation token; empty selects the first page. |

| Response field | Type | Meaning |
| --- | --- | --- |
| `requests` | repeated RequestSummary | Summaries in the selected order. |
| `next_page_token` | string | Continuation when a further row was observed; empty otherwise. |
| `accepted_at_or_after_ms` | int64 with presence | Effective lower bound in acceptance-time mode; absent in request-ID mode. |
| `accepted_before_ms` | int64 with presence | Effective upper bound in acceptance-time mode; absent in request-ID mode. |

On every page, callers supply the queue and order used for the first page. Page size may change. Time bounds may be omitted on continuations; if supplied, each must match its token value. There is no cross-queue selector, and every call follows the service's queue-access policy. IDs remain opaque to clients.

## Ordering and Time Defaults

`REQUEST_ID_DESC` uses the numeric per-queue sequence already used by [CompareRequestID](../../../stovepipe/entity/request_id.go) and [backlog coalescing](steps/process.md#backlog-coalescing). It lists all retained summaries, including legacy rows whose acceptance time is unknown. It has no time cutoff and rejects supplied time bounds. Counter gaps are valid and do not represent requests to synthesize or individually probe.

`ACCEPTED_AT_DESC` orders by `(accepted_at_ms DESC, request_id DESC)`. The ID tie-breaker uses UTF-8 byte ordering, independent of locale or numeric interpretation. This mode includes only summaries with a verified acceptance timestamp. It never promises numeric ingest order within a time interval.

Resolve an acceptance-time query's bounds once on the first page:

| Supplied bounds | Effective interval |
| --- | --- |
| Neither | `[0, now)`, sampling the server clock once. |
| Lower only | `[lower, now)`, sampling the server clock once. |
| Upper only | `[0, upper)`. |
| Both | `[lower, upper)`. |

Supplied bounds must be non-negative, and the resolved lower bound must be strictly less than the upper bound. Explicit future bounds are allowed. Scalar presence distinguishes omission from an explicit zero. Every acceptance-time response echoes the resolved bounds; a token preserves them, so later pages never resample the clock. Starting with an empty token resolves a new query. Neither mode has an implicit 24-hour window or a maximum interval width: the ordered access path and page limit bound each call.

Time selection uses `lower <= accepted_at_ms < upper`, with current materialized lifecycle state at read time. It does not reconstruct state at the upper bound or find every request active during the interval. A request accepted before the lower bound is excluded even if still processing.

## Summary Fields and Existing Data

The [domain summary](../../../stovepipe/entity/request_summary.go) and [SQL schema](../../../stovepipe/extension/storage/mysql/schema/request_summary.sql) already contain six response fields. Acceptance time and outcome reason are recorded in logs but need to be added to the summary.

| Wire field | Type | Current source | Required work |
| --- | --- | --- | --- |
| `request_id` | string | RequestSummary.RequestID | Already materialized. |
| `queue` | string | RequestSummary.Queue | Already materialized. |
| `change_uri` | string | RequestSummary.URI | Already materialized; preserve the exact URI. |
| `base_uri` | string | RequestSummary.BaseURI | Already materialized; empty before selection or for a full build. |
| `request_state` | string | RequestSummary.State | Already materialized. |
| `state_updated_at_ms` | int64 | RequestSummary.StateTimestampMs | Already materialized; timestamp of the represented state entry. |
| `accepted_at_ms` | int64 with presence | Canonical version-1 accepted log's TimestampMs | Add immutable acceptance time, preserving absence for legacy summaries. |
| `outcome_reason` | string | Winning state log's OutcomeReason | Add to the summary and advance with the winning state; empty when unavailable or inapplicable. |

Acceptance time is the timestamp of the original retained `accepted` entry for request version 1. The materializer samples time before an insertion attempt, and duplicate reconciliation preserves the first successfully retained entry's timestamp. It records acceptance-log time, not first RPC receipt, commit author time, build start, or completion. Once known, it never changes. `StateTimestampMs` advances with lifecycle state and is not a substitute.

A retained accepted entry can supply its timestamp directly. To initialize an older summary, retrieve that one occurrence by stable ID. Missing historical acceptance leaves the field absent, rather than inventing a time or using zero as unknown. The row remains available in request-ID mode and is excluded from acceptance-time mode. Infrastructure lookup failures remain errors.

Outcome reason comes from the retained log matching the summary's current `RequestVersion`, not an older entry that triggered repair. Initialize missing context conditionally and reload after a version race. Filling context advances the summary's projection version without changing the represented request version. Lifecycle winners advance state, baseline, state timestamp, and reason together. Timestamps do not decide the winner; existing request-version reconciliation remains authoritative.

The public lifecycle vocabulary is `accepted`, `processing`, `superseded`, `succeeded`, `failed`, and `cancelled`; reasons use the existing [public vocabulary](request-log.md#outcome-reasons). Clients tolerate future values. A terminal lifecycle does not guarantee record-stage completion or a validation fact. List omits validation verdicts, project results, build details, arbitrary metadata, and histories; existing status and history APIs serve those concerns.

## Storage Access Paths

Keep `RequestSummaryStore` as the authoritative lifecycle read model and add a bounded request-ID range read for the resolved queue. Its current VARCHAR primary key does not provide numeric order: lexical order places 9 after 10. Use an order-preserving primary-key representation of the existing sequence, such as fixed-width encoded numeric keys, while retaining the public ID as a string. Point reads derive the same internal identity key. This requires a compatible storage-key migration, not a new event or a separate mutable projection. A numeric cast followed by sorting the entire queue is not a bounded range implementation.

For time queries, add an immutable `RequestAcceptance` mapping with logical key `(queue, accepted_at_ms, request_id)`. Its only information is the verified acceptance tuple identifying an existing summary. The corresponding queue-scoped store provides idempotent create and a limited ordered range read with an exclusive tuple cursor. A MySQL implementation uses this tuple as its primary key with binary ID comparison; an ordered key-value backend uses the equivalent key. No secondary-index capability is required.

Request-ID mode reads at most `page_size + 1` summaries directly. Acceptance-time mode reads at most `page_size + 1` mappings, then retrieves at most `page_size` summaries by individual primary-key reads. The extra mapping determines whether another page exists and needs no summary lookup. Reads may use bounded concurrency; the contract does not require multi-key queries or joins. A missing mapped summary, mismatched queue, or acceptance timestamp disagreeing with its mapping is an internal consistency error and fails the page.

Both modes return current state from the same authoritative summary. There is no replicated mutable queue summary to synchronize. A per-queue latest ID or timestamp cannot replace the time mapping: IDs are allocated before acceptance is recorded, so one call may allocate 41 and stall while 42 records acceptance first. Scanning IDs and stopping at an old timestamp can therefore miss matching requests.

This design follows the repository's [key-value storage contract](../../../submitqueue/extension/storage/README.md#key-value-contract), which permits bounded reads over leading primary-key components and first-class mapping stores for needed alternate lookups. A SQL secondary index on the summary could implement time selection for MySQL, but would make that alternate lookup implicit and require other backends to supply a capability the existing point-store contract does not express. The explicit immutable mapping is the portable index, not a way to avoid maintaining an index. Its cost is one additional durable mapping per request, an idempotent create/ensure step on state reconciliation, and bounded summary point reads per time-ordered page. V1 does not introduce an exception to the rule against secondary-index-dependent store queries.

## Writes and Repair

Extend the existing [materializer](../../../stovepipe/core/requestlog/request_summary.go) to retain the canonical state log, advance or initialize the authoritative summary, and ensure its acceptance mapping before dependent publication. New accepted entries must establish their timestamp and mapping before process publication. A publish failure leaves the durable admission visible; retry of the same queue and URI repairs the handoff.

Mapping creation is idempotent and has no lifecycle update operation. If the incoming state loses to a newer summary, still ensure the mapping from that authoritative summary. If a legacy summary lacks trustworthy acceptance data, continue its lifecycle projection without creating a mapping. Once verified context becomes available, create the mapping from it. Informational event entries neither change lifecycle state nor determine acceptance time.

The write order is source state, retained log, authoritative summary, acceptance mapping, then dependent publication. There is no cross-entity transaction. A mapping failure returns an error so the existing delivery or reconciliation trigger repairs it; Ingest failures rely on caller retry. Lifecycle changes do not rewrite mapping content. All conditional summary writes preserve caller-owned values, compute versions outside storage, and assign versions only after successful persistence.

## Pagination and Errors

The cursor is exclusive: request-ID mode continues below the last returned numeric ID, and acceptance-time mode below the last returned time/ID tuple. Return at most `page_size` rows and issue a token only when a further row was observed. An exhausted query returns an empty page and empty token.

Tokens contain a format version, queue, order, cursor, and time-mode bounds. Reject malformed or unsupported tokens, invalid IDs, a query mismatch, and time cursors outside the token's interval. A cursor row need not still exist. Tokens confer no authorization.

Identity order and verified acceptance tuples are immutable, preventing duplicates during forward traversal. Paging is not a snapshot: delayed summary or mapping creation can insert a row into a portion already passed, later pages may show newer state, and future pruning may remove rows. Refresh starts from an empty token. An empty continuation means no further row was observed in that read.

| Condition | RPC result |
| --- | --- |
| Missing, invalid, or unconfigured queue; invalid order, bounds, page size, or token | Invalid argument. |
| Configured queue with no matching retained rows | Successful empty page. |
| Caller lacks access under the service's queue policy | Permission denied. |
| Classified retryable storage failure | Unavailable. |
| Cancellation or deadline | Corresponding canonical transport code. |
| Corrupt summary or dangling/inconsistent mapping | Internal error; fail the page rather than silently omit rows. |

List performs no source-control query, history replay, initialization, repair, or publication during reads. An absent legacy acceptance timestamp is valid, not corrupt content.

## Rollout and Acceptance Criteria

Add acceptance time with explicit migration presence, such as a nullable SQL column and corresponding domain presence value. Add outcome reason with a compatible empty default. Existing summaries remain usable with an absent timestamp; only a retained accepted record can initialize it. Automatic historical backfill and reconstruction of Requests never summarized are outside v1.

Deploy the schema, numeric-order key migration, and compatible materializers before exposing List. Enable the mapping handoff only once all writers can preserve initialized context and ensure mappings. Complete time-mode coverage starts with admissions after that writer cutover; older requests may appear when their context and mapping are repaired. Request-ID coverage is all retained summaries. A rollback to incompatible writers must disable the affected time-mode guarantee.

V1 has no automatic pruning or fixed retention duration. Future pruning must coordinate summaries, mappings, retained history, URI mappings, and retry triggers so a dangling mapping does not fail reads or repair recreate an expired request. Neither mode promises a snapshot export, complete operational-request reconstruction, or lifetime retention.

Implementation and review must verify:

- Summary fields preserve the original accepted timestamp across retries and later states; conditional initialization uses the current winning reason and tolerates missing legacy history.
- Request-ID storage preserves numeric order across digit boundaries and counter gaps, retains string wire IDs, and preserves point lookup across the key migration.
- Acceptance mappings are immutable, use binary ID ties, and are repaired after partial writes even when an incoming state loses.
- Both query modes enforce page limits, token binding, queue isolation, and empty-page semantics; omitted time bounds remain fixed across continuations.
- Request-ID mode includes legacy summaries with absent timestamps; time mode excludes them and fails on a mapped summary that is missing or inconsistent.
- Source, log, summary, mapping, and publication failures retain the existing repair path, without requiring transactions or mutable-state duplication.
- New protobufs, domain values, controller, mapper, store implementations, mocks, and wiring preserve existing status/history contracts and tolerate future lifecycle values.

State or verdict filtering, recently-updated ordering, lifecycle-overlap queries, real scheduler position, totals, exact snapshots, automated backfill, and pruning remain separate extensions. In particular, indexing mutable state-update time would need a different membership and pagination contract; it is not part of this acceptance index.
