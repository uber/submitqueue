# Open issues API

`ListOpenIssues` returns current open issue summaries for one configured queue in ascending bytewise task/unit order. An empty unit identifies a task-wide issue. The response exposes summary data without internal storage versions.

## Bounded pagination

The request selects a queue, page size, and opaque continuation token. Page size defaults to 50 and is limited to 200. It bounds the number of matching issues returned. The controller scans until the page fills, the lookup range is exhausted, or 500 lookup entries have been validated. Each storage batch reads one additional entry to detect whether the range continues; this lookahead entry is not validated or consumed by that batch.

Continuations are bound to the selected queue and advance after the last examined task/unit key. Page size may change between calls. Malformed tokens, mismatched queues, unconfigured queues, and invalid page sizes are rejected.

A page can contain fewer issues than requested, including zero, while still carrying a continuation when the inspection budget is reached. Clients must use the continuation token rather than response length to decide whether to keep reading. Pagination is not a snapshot: summaries and lookup membership can change during or between calls.

## Summary validation

The controller scans bounded ranges from the open-issue lookup and point-reads the corresponding summaries. It returns a summary only when its status is open and its opening matches the lookup reference. References to closed issues or previous openings are skipped. Skipped references still advance the cursor and count against the inspection budget.

Summary-read failures fail the request rather than returning a partial page. A missing summary is also an error: summaries retain closed rows, and lookup writers persist the summary first. A lookup that returns a different task/unit identity from its summary is an infrastructure inconsistency.

The API is read-only. It does not repair open-issue rows, scan operational issue state, perform database joins, or require secondary indexes. The issue-log materializer maintains the lookup during normal processing and recovery.

An empty result does not establish build success or complete issue coverage. Historical rows without a materialized lookup are not backfilled by reads. Project-specific filtering, issue history, repair details, and the frontend are outside this API.
