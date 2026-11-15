# Request issues API

The request-issues lookup returns accepted results for a queue and request ID, not the queue's full open-issue set. Each result is new (opened or reopened), existing (reported but already open), or closed (actually closed by this request). Older issues retained because closure was suppressed are not existing results unless this request reported them.

The issue log retains openings, repeat reports, and closures. Its materializer resolves the source build to its request and derives New, Existing, or Closed results in the request-issue summary. Repeat reports leave lifecycle status and timestamps unchanged. The retained opening reference supplies historical provenance without consulting current issue state; legacy events without this reference do not create request summaries.

Results are read from the request-issue summary under the queue/request primary-key prefix, in task/unit order. The default page size is 50 and the maximum is 200. A queue- and request-bound opaque continuation resumes after the last returned key; one extra row establishes whether another page exists. No joins or secondary indexes are required. A closure wins over earlier reports or openings from the same request.

The response reports in-progress recording, complete reconciliation, incomplete abandoned work, or unavailable coverage. Completion is read before the result page and is written only after all accepted issue events and their projections are durable. Completed or abandoned results remain stable as later requests update the queue. Complete does not mean the build passed or all queue issues closed; canceled builds and task-wide failures can suppress closure.

In-progress pages are not a snapshot: concurrent recording can add keys behind the cursor. Restart from the first page once recording finishes. Unavailable means recording has not started or the request predates the retained start marker; an empty page then does not establish that the request had no issues. Historical results are not backfilled.

Recording coverage assumes every issue-tracking worker writes request results. Drain older workers before admitting requests whose summaries need complete coverage; requests started before that deployment boundary are not backfilled.
