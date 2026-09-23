# MySQL Schema

The gateway's read model: the append-only request log and the three materialized projections behind request-summary retrieval and `List`. The orchestrator's pipeline working state is a separate schema — see [../../../../../orchestrator/extension/storage/mysql/schema/README.md](../../../../../orchestrator/extension/storage/mysql/schema/README.md).

## Queue-leading primary keys

Every table leads its primary key with `queue`: `request_summary` on `(queue, request_id)`, `request_log` on `(queue, request_id, timestamp_ms, salt)`, `change_uri_request_mapping` on `(queue, change_uri, received_at_ms, request_id)`, and `request_summary_by_queue` on `(queue, received_at_ms, request_id)`. A queue-bound store instance prefixes every read and stamps every write with its bound queue, so one queue's rows are unreachable through another queue's binding and every table is shardable by queue. `//tool/linter/queueshard` enforces this, and also rejects any secondary index that does not itself lead with `queue`, since such an index would reintroduce a cross-queue access path.

## Read model

The gateway request read model uses three additive tables and requires no alteration of existing tables. Deployments create these tables empty and populate them only for requests received after rollout; historical request logs and orchestrator working tables are intentionally not backfilled.

### `request_summary`

`request_summary` is keyed by `(queue, request_id)` and serves direct Status lookup within one queue. It stores immutable receipt context plus the current materialized request-log winner and its optimistic-lock projection version.

### `request_summary_by_queue`

`request_summary_by_queue` is keyed by `(queue, received_at_ms, request_id)`. This key covers the List predicate, descending sort, and keyset continuation for one bounded receipt-time window without a secondary index. The row duplicates the complete List response so one page is served by one range scan rather than one follow-up read per request ID.

### `change_uri_request_mapping`

`change_uri_request_mapping` is keyed by `(queue, change_uri, received_at_ms, request_id)` and serves bounded newest-first Status lookup by change URI within one queue. The gateway reads at most 101 mappings to enforce the API maximum of 100 results without silently truncating. A change URI landed into several queues has independent mappings in each, so looking it up across queues is one call per queue.

### `request_log`

`request_log` is keyed by `(queue, request_id, timestamp_ms, salt)` and holds the append-only audit trail behind History. `salt` disambiguates entries sharing a request, queue and millisecond; it is part of the key but never exposed through the storage interface.

### JSON collections

`change_uris` and `metadata` are non-null application values. MySQL JSON columns can contain the JSON value `null` despite `NOT NULL`, so stores normalize nil slices and maps to empty values on both write and read.
