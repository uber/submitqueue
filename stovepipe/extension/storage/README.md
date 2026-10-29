# Storage

Pluggable persistence interfaces for Stovepipe entities (`RequestStore`, `RequestURIStore`, `QueueStore`, `BuildStore`, `BuildIssueStore`, `IssueStateStore`, `IssueLogStore`, `IssueTrackingStore`). Implementations live under `extension/storage/<impl>/`.

The aggregate is resolved per queue through a factory keyed by queue name, mirroring the extension contract and the [submitqueue storage seam](../../../submitqueue/extension/storage/README.md#queue-scoped-resolution): a resolved instance is bound to its queue, entity arguments whose queue disagrees with the binding are rejected, and reads never surface another queue's records. Stovepipe has no cross-queue read paths, so every store — including `QueueStore`, whose row key is the queue name itself — lives inside the queue-scoped aggregate; there is no global remainder.

This is a separate contract from `submitqueue/extension/storage` — same shape and conventions by design, but its own interfaces and its own `ErrNotFound`/`ErrAlreadyExists`/`ErrVersionMismatch` sentinels, since Stovepipe and SubmitQueue are independent domains. `ErrVersionMismatch` is declared as a retryable infrastructure error so callers can return it without reclassifying it.

## Optimistic locking contract

Entities that support concurrent mutation (`Request`, `Build`, `IssueState`, `IssueTrackingState`) carry an `int32 Version` field. `Update` methods take both `oldVersion` (the where-clause guard) and `newVersion` (the value to write) — the store performs a pure conditional write and never computes `oldVersion + 1` itself. Version arithmetic is owned by the controller: it computes `newVersion`, calls `Update`, and only assigns `entity.Version = newVersion` after the call succeeds. See [AGENTS.md](../../../AGENTS.md) and the [submitqueue storage README](../../../submitqueue/extension/storage/README.md#optimistic-locking-contract) for the full rationale and the caller pattern — the convention is identical here.

## Read-after-write consistency

A `Get` immediately following a successful write (`Create`/`Update`) — by the same caller, or a causally-dependent one such as a queue consumer processing a message published after the write committed — must return that write. This is a requirement on every storage implementation, not a condition callers negotiate around.

**Controllers must not treat `ErrNotFound` as "not visible yet, retry."** The store interface is intentionally general enough to run over any backend, so a controller has no way to know whether a missing row will appear shortly or does not exist at all — retrying on that assumption just reintroduces, in business logic, the consistency gap the storage contract exists to close. If a `Get` misses a row that a causally-prior write should already have produced (e.g. `build` loading the `Request` that `process` published its message for, or `process` loading the `Queue` row `ingest` get-or-created before publishing), that is a storage implementation defect: let the error surface as a normal (non-retryable, per [`platform/errs`](../../../platform/errs/README.md)'s default) failure rather than absorbing it with a retryable wrapper. See [build.md](../../../doc/rfc/stovepipe/steps/build.md#error-classification) and [process.md](../../../doc/rfc/stovepipe/steps/process.md) for the pipeline stages this applies to.

## Key-value contract

Same design space as [`submitqueue/extension/storage`](../../../submitqueue/extension/storage/README.md#key-value-contract): every method must be satisfiable by a plain key-value backend as cheaply as by MySQL — get/put/conditional-update by primary key only, no query-by-attribute or server-side filtering. `RequestURIStore` is the reverse-lookup example here (which request owns a given commit URI), kept as its own store rather than a secondary index on `RequestStore`.

`RequestAcceptanceStore` is the ordered lookup mapping for List: immutable `(queue, accepted_at_ms, request_id)` keys with bounded primary-key range scans, not a secondary index or a second status projection. The request-log materializer ensures each known acceptance has a mapping after summary persistence, including retries where the summary is unchanged. Unknown acceptance times are not mapped; historical backfill is separate.

`QueueStore` owns internal enablement alongside pipeline coordination. `QueueSummaryStore` persists the current read model projected from immutable `QueueLogStore` occurrences. Enablement sequences, operational row versions and projection versions are distinct. These stores use primary-key operations; version arithmetic, duplicate reconciliation and projection repair belong to business logic. See [queue state](../../../doc/rfc/stovepipe/queue-state.md).

`ProjectFactByDegreeStore` is the ordered lookup mapping for named project facts. Its immutable `(queue, URI, degree DESC, project ASC)` primary key stores the original degree and orders by descending degree and ascending bytewise project ID, so listing is a bounded key-range read. It holds no duplicate validation payload; readers hydrate facts by their original keys and verify the mapping. The record controller ensures mappings from persisted winning facts before recording project completion, including retries after partial writes. Create the table before deploying writers. The table starts empty: only results processed by index-aware writers receive mappings, with no historical population or read-time fallback.

## Build issue observations

`BuildIssueStore` accumulates observations identified by build, task, and unit within the bound queue. The build ID is supplied separately from the detected issue. Repeated observations return `ErrAlreadyExists`, which callers may treat as successful persistence. Build and task IDs must be nonempty; empty unit IDs represent task-wide observations.

Listing fixes the queue/build primary-key prefix and returns bounded pages in ascending bytewise task/unit order. A task/unit cursor continues after the last returned key; the zero cursor starts the range, and a unit without a task is invalid. Pages are not a snapshot across calls: concurrent observations can affect membership. Empty pages represent no observations in that range, not build success or completed analysis.

Each observation is stored separately, not in a serialized collection. Observations are historical evidence; they do not track whether an issue is currently open or closed. Repair metadata is mutable; observation identities remain durable, with no deletion or absence-based recovery.

The MySQL primary key contains the queue, build ID, task ID, and unit ID directly as binary strings, preserving byte-exact identity. Their limits are 255, 1020, 255, and 1024 bytes respectively, for a combined maximum of 2554 bytes. Creates and reads reject identifiers exceeding these limits before querying the database; identifiers are never truncated.

`BuildIssue.FixID` associates an observation with an opaque executor-provided repair ID. An empty ID means no association is recorded. `Update` uses the existing entity CAS convention: it guards `oldVersion` and writes `newVersion`, returning `ErrVersionMismatch` for a stale or missing record. The caller owns version arithmetic and repair policy; storage does not enforce a per-build submission limit. Observation identity and membership survive recovery.

Existing MySQL deployments must add `fix_id VARCHAR(255) NOT NULL DEFAULT ''` and `version INT NOT NULL DEFAULT 0` to `build_issue` before deploying the new readers and writers. The schema's `CREATE TABLE IF NOT EXISTS` does not alter existing tables.

## Issue state

`IssueStateStore` retains the latest open or closed state for each task/unit within its queue. The primary key remains `(queue, task, unit)`; status is a value, not a key component. A closed record retains its first-seen build, last-processed build, last-changing build, transition timestamp, and positive CAS version. Reopening updates the same record with a new first-seen build and a higher version. Records are never deleted, so a stale create cannot reuse an empty key after closure.

Create is conditional on absence, including closed records. Update checks the supplied old version and writes the caller's replacement version. Controllers compute versions and update their in-memory values only after success. The store does not decide which source build is allowed to change an issue; that decision belongs to the issue-tracking controller.

Listing reads a bounded primary-key range in bytewise task/unit order and includes both statuses. Callers filter closed records in memory and advance after the last scanned key, including closed records. A page containing only closed records is not the end of the range. No status index or server-side filtering is required. Task IDs must be nonempty; empty units identify task-wide issues. Identifier byte limits match the build-issue store.

The MySQL backend uses `issue_state` for operational tracking. The separate `open_issue` table is a derived lookup, not a replacement for retained issue state. The repair executor still receives an `OpenIssue`, which describes an open occurrence rather than the operational storage record.

## Issue summaries

`IssueSummaryStore` retains one read model per task/unit within its queue, independently of operational `IssueState`. It supports only complete-key create, get, and CAS replacement. Closed summaries remain present; removing them would also remove the source-order information needed to reject an older opening. There is no deletion or listing operation.

`RequestID` and `BuildID` identify the latest represented transition, while `FirstSeenBuildID` identifies its current or most recent opening. Source order follows request ingest order, not opaque build IDs or timestamps. Within the same request, a closing transition supersedes an opening. `Version` is only the summary's CAS guard, not the source order. The materializer must compare incoming transitions before replacing a summary; the store does not enforce lifecycle ordering.

The MySQL primary key is `(queue, task_id, unit_id)` with byte-exact identity and the same identifier limits as the issue-state store. Source request IDs follow the existing canonical positive decimal ID contract. The new `issue_summary` table must exist before deploying readers or writers. The [issue-log materializer](../../core/issuelog/README.md) updates summaries during normal issue tracking and recovery. The [open-issues API](../../../doc/rfc/stovepipe/open-issues-api.md) lists summaries through the derived lookup.

## Open-issue lookups

`OpenIssueStore` exposes the existing `open_issue` table as a queue-scoped lookup keyed by task/unit. Each open-issue row contains only the opening's `FirstSeenBuildID`, using the existing `OpenIssue` entity and identifier byte limits. No columns, indexes, or operational issue-state fields are added.

Create returns `ErrAlreadyExists` for any existing task/unit key. Replace compares the expected opening build ID before changing it; Delete compares the opening in the supplied open-issue row before removing it. Both operations return whether they changed the open-issue row, with false for a missing key or a different opening. Replacing an opening with itself is also a no-op returning false. These are atomic conditional writes to one complete key, not build-order checks. An old close that still expects a previous opening cannot delete a newer opening.

Listing traverses a bounded queue-prefix primary-key range in ascending bytewise task/unit order. The zero cursor starts the range; otherwise the cursor excludes its key and all earlier keys, even if that open-issue row has been deleted. Empty units identify task-wide issues, while task IDs must be nonempty. Membership can change between reads. A consumer that skips stale references must still advance after the last returned open-issue row, not the last accepted issue.

Open-issue rows are not authoritative issue status. A delayed create can leave a reference to a closed issue, so readers must verify that the summary is open and its opening matches before returning a result. The store does not read summaries, enforce source ordering, backfill existing rows, or populate open-issue rows automatically. The [issue-log materializer](../../core/issuelog/README.md#open-issue-lookup) maintains open-issue rows after summary persistence during normal processing and recovery. The open-issues API validates summaries before returning results and advances its continuation after the last examined open-issue row, including skipped references.

## Issue lifecycle history

`IssueLogStore` retains immutable open/closed status transitions independently of current issue state and per-build observations. Each transition is keyed by task, unit, build, and status within the bound queue. An empty unit identifies a task-wide issue. History remains when an issue closes or reopens; transitions do not carry a separate reference to the opening they belong to.

The build ID identifies the build where the issue was first seen or confirmed closed. The application supplies a positive Unix-millisecond timestamp for when the transition was first recorded, not when the build finished. A duplicate identity returns `ErrAlreadyExists` without replacing the original timestamp. Callers can treat replay as success or read the original transition. Build IDs are opaque and do not establish transition ordering.

The MySQL key uses the same binary identifier limits as the issue stores, plus the existing 32-byte `event` column containing the status, for a maximum of 2586 bytes. Its column name and primary key remain unchanged. Reads and creates use primary-key operations; there is no update or deletion API, metadata collection, or repair state.

History persistence does not alter issue state or validate transitions against it. The issue-tracking controller owns opening/closing decisions and invokes the issue-log materializer to retain history and update summaries. History enumeration is outside this storage contract.

## Issue reconciliation progress

`IssueTrackingStore` retains one queue-scoped operational `IssueTrackingState`, independently of open-issue membership and user-facing summaries. It records the source request and build, the refresh or finalization phase, the owning notification, one task/unit cursor, and its optimistic version. The zero cursor starts the current phase's range. Every active pass revisits the cursor issue before scanning later keys, including when the previous delivery already finished that issue. `Finalize` retains the terminal notification intent during refreshing, while `HasTaskWideIssues` records whether a task-wide issue has been encountered in that pass. Once all build issues have been applied, a terminal pass with task-wide failures finishes without closing; otherwise it resets the cursor and finalizes against issue states. These fields and the cursor are saved together under the same version guard. Notification identity lets the controller distinguish its own continuation from helping another pass. The record remains after all issues close to preserve the source-order boundary. The MySQL table is keyed only by queue; creates, reads, and updates use complete primary keys with no secondary indexes or cross-table transaction.

The store does not decide source ordering, apply issue transitions, or advance versions. Its caller supplies the guard version and replacement version. A later controller will own acceptance, resumption of cursor work, and completion; no runtime writer is enabled here.

Recovery uses the same record's `abandoning` and `abandoned` phases; no additional schema fields or storage operations are required. The source, notification, cursor, and terminal intent remain present after abandonment. See [issue tracking recovery](../../controller/issuetrack/README.md#recovery-and-activation) for the conditional-write ordering that protects the current cursor before newer work is admitted.

The issue-tracking table must include the notification, cursor, `finalize`, and `has_task_wide_issues` columns before updated readers or writers are deployed. Both boolean columns default to false. They describe one pass, not analysis state for every build in the queue; terminal intent is required before absence-based reconciliation is permitted. No runtime writer is enabled by the storage layer. Notification IDs allow up to 1020 bytes, matching the queue message ID byte capacity; cursor limits match the issue stores.

### Applied transition facts

`IssueState.LastProcessedBuildID` identifies the most recent build applied to an issue, including unchanged results. `LastChangedBuildID` and `ChangedAtMs` identify the actual opening or closing and supply its history identity and timestamp. They do not advance when only the processed build changes. `FirstSeenBuildID` remains the opening identity across closure and changes on reopening. These references must remain readable while used for source ordering. The current-issue cursor and the row's processed source provide the storage needed for retry-safe application without retaining a future write payload.
