# Request issues API

`ListIssuesByRequestID` lists durable issue observations across builds identified by one request’s retained history. Callers supply a configured queue, canonical decimal request ID, page size, and opaque continuation. The service resolves build IDs internally from retained occurrence metadata; no reverse lookup store or schema change is required. Builds without retained history are outside this view.

Each observation contains its source build ID, task ID, optional unit ID, and optional opaque repair ID. Empty units identify task-wide failures. Observations remain after an issue recovers; this API does not return current issue status. `ListOpenIssues` remains the queue-wide view of currently open issues. Identical task/unit observations from different build attempts remain separate.

Every response includes the selected queue and request, all retained build IDs in bytewise order, and the build establishing the terminal request outcome when retained. Each build’s analysis status is `completed`, `abandoned`, or empty when no outcome is retained. Completion takes precedence over abandonment, allowing a later successful pass to recover. Empty status does not imply running: losing attempts and older retained builds may never record completion. Analysis completion does not imply build success, issue recovery, or completed issue reconciliation.

## Pagination and consistency

Observations are ordered bytewise by build ID, task ID, then unit ID. Page size defaults to 50 and accepts 1 through 200. Build discovery reads the existing retained request log, as the history API does. One call reads observation ranges for at most 100 builds, with at most one bounded range per build. Continuations identify the next build/range position and are bound to the queue and request; a continuation cannot select a build absent from that request’s retained history. Page size can change between calls.

A page can be empty with a continuation, including when a build boundary or the build inspection budget is reached. Only an empty continuation ends traversal. Pagination is live rather than a snapshot: newly retained builds or observations before the cursor require refreshing the first page. Analysis status and pagination exhaustion are independent. An exhausted empty listing does not establish build success or complete issue coverage.

Invalid identifiers, unconfigured queues, invalid page sizes, and malformed or mismatched continuations return `InvalidArgument`. Requests without retained history return `NotFound`. Storage failures fail the entire call without returning a partial observation page. Missing materialized summaries during history publication remain retryable infrastructure failures. The API never repairs or backfills data.
