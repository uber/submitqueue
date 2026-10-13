# MySQL Schema

The gateway's read model consists of an append-only request log, one authoritative summary, and two immutable lookup tables behind request-summary retrieval and `List`. The orchestrator's pipeline working state is a separate schema — see [../../../../../orchestrator/extension/storage/mysql/schema/README.md](../../../../../orchestrator/extension/storage/mysql/schema/README.md).

## Queue-leading primary keys

Every table leads its primary key with `queue`: `request_summary` on `(queue, request_id)`, `request_log` on `(queue, request_id, timestamp_ms, salt)`, `change_uri_request_mapping` on `(queue, change_uri, received_at_ms, request_id)`, and `request_receipt` on `(queue, received_at_ms, request_id)`. A queue-bound store instance prefixes every read and stamps every write with its bound queue, so one queue's rows are unreachable through another queue's binding and every table is shardable by queue. `//tool/linter/queueshard` enforces this, and also rejects any secondary index that does not itself lead with `queue`, since such an index would reintroduce a cross-queue access path.

## Read model

`request_summary` holds the full projection. `request_receipt` and `change_uri_request_mapping` contain only immutable lookup keys.

### `request_summary`

`request_summary` is keyed by `(queue, request_id)` and serves direct request-summary lookup within one queue. It stores immutable receipt context plus the current materialized request-log winner and its optimistic-lock projection version.

### `request_receipt`

`request_receipt` is keyed by `(queue, received_at_ms, request_id)`. List scans these keys in descending order, then point-reads `request_summary` for each result. The key supports receipt-time bounds and keyset pagination without a secondary index.

### `change_uri_request_mapping`

`change_uri_request_mapping` is keyed by `(queue, change_uri, received_at_ms, request_id)` and serves bounded newest-first request-summary lookup by change URI within one queue. The gateway reads at most 101 mappings to enforce the API maximum of 100 results without silently truncating. A change URI landed into several queues has independent mappings in each, so looking it up across queues is one call per queue.

### `request_log`

`request_log` is keyed by `(queue, request_id, timestamp_ms, salt)` and holds the append-only audit trail behind History. `salt` disambiguates entries sharing a request, queue and millisecond; it is part of the key but never exposed through the storage interface.

### JSON collections

`change_uris` and `metadata` are non-null application values. MySQL JSON columns can contain the JSON value `null` despite `NOT NULL`, so stores normalize nil slices and maps to empty values on both write and read.

## Legacy table retirement

Retire unused tables separately from application cleanup:

1. Move readers to the replacement before stopping legacy writes. Once writes stop, rollback must use a reader whose data is still maintained.
2. Retain the table and its SQL definition until no deployed or rollback-supported binary depends on them.
3. Delete the table through an explicit, reviewed migration using the deployment's schema tooling, then remove its SQL definition. Removing a schema file alone is not a deletion migration.

`request_summary_by_queue` is unused by the gateway and retained pending retirement through this process.
