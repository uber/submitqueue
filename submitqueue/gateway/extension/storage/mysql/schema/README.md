# MySQL Schema

The gateway's read model consists of an append-only request log, one authoritative summary, two immutable lookup tables behind request-summary retrieval and `List`, and advisory status membership records behind listing a queue's requests by status. The orchestrator's pipeline working state is a separate schema — see [../../../../../orchestrator/extension/storage/mysql/schema/README.md](../../../../../orchestrator/extension/storage/mysql/schema/README.md).

## Queue-leading primary keys

Every table leads its primary key with `queue`: `request_summary` on `(queue, request_id)`, `request_log` on `(queue, request_id, timestamp_ms, salt)`, `change_uri_request_mapping` on `(queue, change_uri, received_at_ms, request_id)`, `request_receipt` on `(queue, received_at_ms, request_id)`, and `queue_request_status` on `(queue, status, request_id)`. A queue-bound store instance prefixes every read and stamps every write with its bound queue, so one queue's rows are unreachable through another queue's binding and every table is shardable by queue. `//tool/linter/queueshard` enforces this, and also rejects any secondary index that does not itself lead with `queue`, since such an index would reintroduce a cross-queue access path.

## Read model

`request_summary` holds the full projection. `request_receipt` and `change_uri_request_mapping` contain only immutable lookup keys. `queue_request_status` is the one mutable index, and it is advisory: the summary stays the only source of truth.

### `request_summary`

`request_summary` is keyed by `(queue, request_id)` and serves direct request-summary lookup within one queue. It stores immutable receipt context plus the current materialized request-log winner and its optimistic-lock projection version.

### `request_receipt`

`request_receipt` is keyed by `(queue, received_at_ms, request_id)`. List scans these keys in descending order, then point-reads `request_summary` for each result. The key supports receipt-time bounds and keyset pagination without a secondary index.

### `change_uri_request_mapping`

`change_uri_request_mapping` is keyed by `(queue, change_uri, received_at_ms, request_id)` and serves bounded newest-first request-summary lookup by change URI within one queue. The gateway reads at most 101 mappings to enforce the API maximum of 100 results without silently truncating. A change URI landed into several queues has independent mappings in each, so looking it up across queues is one call per queue.

### `queue_request_status`

`queue_request_status` is keyed by `(queue, status, request_id)` and files each in-queue request under its public status, so "the requests of queue Q at status S" is a primary-key-prefix scan and `request_summary` keeps no secondary index. Each row carries only the request's `received_at_ms` and the `entered_at_ms` of the log that moved it into the status, enough to count and age a queue's requests by status.

The materializer maintains it on every status-log delivery, after the summary update: it files the record for the summary's current status (a no-op when present, which also repairs a delivery that failed after the update) and, when the delivery moved the summary, then deletes the record under the previous status. Accepting and terminal statuses have no record, and event logs, which never move the summary, write nothing. Every write is a single-key idempotent put or delete, so the records need no multi-row transaction.

Stale rows are a normal occurrence, not only a failure mode, and two rules bound them:

- Land and Cancel persist logs outside the log consumer's per-request ordering, so another writer can move the summary on and delete a status's record before a late put re-creates it. After each put the materializer re-reads the summary and deletes the record it just filed if the status has moved on; every writer updates the summary before deleting, so the re-read sees any such move.
- A terminal summary deletes the request's record under every in-queue status, on every terminal delivery. That retries a failed removal and clears records stranded by earlier failed moves.

A record stranded by a failed move while the request is still in the queue remains until the request finishes. Readers must therefore confirm each listed request against its summary, count it only under the summary's status, and delete rows whose summary is finished or at a different status. That costs one summary read per listed row per reader per sample, which is small while queues are short but grows with queue depth and the number of reading instances.

The records are kept only by a storage built with `WithQueueRequestStatusRecords`, which a deployment enables once the table exists; without it, record writes are no-ops and listing fails. Requests already in flight at that point gain a record on their next log delivery.

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
