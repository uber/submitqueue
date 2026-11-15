# Issue tracking

Issue tracking maintains current issue state and opening/closing history from persisted build issues. Each delivery applies a bounded batch of issues and requests immediate redelivery when more work remains. The reference service registers both the normal and recovery consumers.

## Validation order

Admission registers one request-scoped tracking job before committing the request to processing or dispatching its build. A queue-version check after registration prevents a delayed publisher from admitting an older validation behind a newer registration. A request skipped after registration leaves a harmless job that acknowledges its terminal, unbuilt request. Build dispatch retains the request-to-build association before starting status polling and analysis; retries reuse that association.

The tracking subscription must fetch only the first unacknowledged message in each queue partition. The reference host uses the shared `SingleMessageSubscriptionConfig`, whose message batch size is one; reconciliation still processes up to 100 issue keys per delivery. A larger message batch can prefetch later requests before the current job establishes its hold. Build execution and per-build detection remain concurrent.

A request's job stays at the queue head through ordinary refreshes. Successful refreshes hold for 30 seconds; unfinished batches request immediate redelivery. The current request cannot be replaced until it is finalized or safely abandoned. A later request that reaches processing during recovery waits for that boundary rather than retrying the failed request's remaining work.

Terminal readiness comes from the retained `build_analysis_completed` event, written only after the complete terminal issue set has been saved. `build_analysis_abandoned` explicitly ends incomplete analysis without allowing absence-based closure. These are point reads by the request and deterministic event ID, not scans of history. A completion event takes precedence if duplicate analysis deliveries retained both events. Neither CI status nor an empty issue set establishes readiness. The request, build, issue set, and analysis history must remain available until tracking finishes.

This preserves a close in one validation followed by a new opening in the next, including its first-seen build and fixer eligibility. The next build's issues can accumulate in `build_issue` while its tracking job waits, but they do not change shared issue state or trigger repair until that job becomes current.

## Applying one issue

1. Read whether the build reports the issue and load its current state.
2. Reject a source older than the issue's last processed build, using Request order rather than opaque Build IDs.
3. Before changing the issue, conditionally save its task/unit key as the current queue cursor **after** reading the issue row. Repeat this conditional write even when resuming the same cursor.
4. Conditionally create or update the issue row. Record the processed build even when its open/closed status stays the same; update the changing build and timestamp only on an actual transition.
5. Retain opening or closing history from the issue row's changing build and timestamp, then materialize its summary and open-issue lookup. On retry, the inclusive cursor revisits that row and repairs all three before proceeding.

Closed issue rows are retained, so their versions cannot reset on reopening. A repeated result from the same build needs no new issue write. Its history is repaired when revisiting the current cursor.

## Ordering requirements

During issue application, the caller must finish the current cursor and its history before moving on or admitting another build. If a replacement worker advances, completing that cursor changes the issue version first. A paused worker's earlier issue write then fails its row guard. A worker that reads later is rejected by source ordering or the conditional cursor write.

The [issue-log materializer](../../core/issuelog/README.md) also orders summary updates by source request, with closing after opening within the same request. A paused projection writer cannot overwrite a newer summary: its conditional write fails, and reloading the winner repeats the source-order check. Lookup entries are then maintained from the latest summary, not the incoming transition. Summary and lookup failures keep the cursor unfinished, just like history failures.

The post-read cursor write and processed-build updates, including unchanged statuses, are required for this ordering. A progress version alone is not a lock around issue writes. These operations use individual K-V reads, conditional writes, and immutable history creates; no cross-record transaction or stored future-change payload is needed.

## Refreshing and finalizing

Every pass starts in `refreshing`, applying the build's accumulated issues in primary-key order. Reported issues are opened or kept open; nothing is closed. A terminal pass retains `Finalize = true` in the progress record, so a replacement worker can distinguish terminal work from an ordinary refresh. Encountering a task-wide issue (empty unit ID) sets `HasTaskWideIssues` in that same versioned record. This fact remains set across batches and retries, including when the issue was already applied by an earlier pass.

Once the entire build-issue range is exhausted, an opening pass with a nonempty task cursor publishes a build-scoped repair request before saving its next phase. Empty passes and closing sweeps do not publish. Publication failures leave the pass active so retries repeat the handoff after ensuring issue state and history are durable. The repair message identity includes the persisted checkpoint version: retries before phase completion share an identity, while later refreshes of one long-lived job can trigger another evaluation without depending on queue garbage collection. The fix controller offers each registered executor all pending issue openings first seen in that build; each executor chooses how to group its repairs.

Once the entire build-issue range is exhausted:

- An ordinary refresh becomes `refreshed`.
- A terminal pass with a task-wide failure, or a canceled build, becomes `finalized` without closing anything. Reported closed issues may still have reopened during refreshing; unreported closed issues remain closed.
- Any other terminal pass becomes `finalizing`, resetting its cursor to the start of the issue-state range.

The `finalizing` phase traverses retained issue states in primary-key order. Each key is looked up in the build's issues: a reported issue remains open, while an absent issue closes. Actual openings and closings are retained in history. Exhausting the issue-state range marks the pass `finalized`, so late refreshes are ignored.

No closure occurs before all accumulated build issues have been processed. A task-wide failure in a later batch therefore protects existing issues across every task, not just keys encountered afterward. A later build makes its own decision; the restriction does not carry over. Both walks consume one ordered range at a time; no merge is required.

## Issue lifecycle hooks

After a transition's history and projections are durable, issue tracking publishes an `issue.opened` or `issue.closed` hook event. Openings include reopenings after closure; an unchanged status in a later build creates no transition event. Both payloads contain `queue`, `task_id`, and `unit_id` (empty for task-wide issues). An opening adds `opening_build_id`; a closure adds `closing_build_id`.

The timestamp is the retained transition timestamp, and the ID hashes the queue/task/unit/changing-build identity under the event type. Retrying publication reproduces the event. Publication must succeed before advancing the cursor. Hosts bind the shared hook topic and pass the topic registry to the normal controller. DLQ recovery repairs accepted state and history without publishing hooks. If publication exhausts normal retries, the transition notification may be lost; reliable delivery beyond that boundary requires a separate durable delivery mechanism.

Closure consumers can resolve the original opening with two direct reads, even after the issue has reopened:

```go
closed, err := logs.Get(ctx, taskID, unitID, closingBuildID, entity.IssueEventClosed)
// Handle err before using closed.
opened, err := logs.Get(ctx, taskID, unitID, closed.FirstSeenBuildID, entity.IssueEventOpened)
```

The opening build's request identifies any request-scoped extension metadata. Consumers that only need that request can skip the second log read and load `closed.FirstSeenBuildID` directly. Use the immutable closure log rather than the latest issue state, which may already describe a later reopening. Older log records may lack the opening reference; consumers must handle that absence.

The hook framework delivers at least once. Implementations that perform external side effects must deduplicate by event ID in durable storage; queue deduplication alone cannot guarantee one external action.

Issue state includes generic namespaced metadata for hook implementations. It survives subsequent observations, closing, and reopening. Hooks resolve the queue's issue store and update a copy of the latest state with its version guard; a concurrent lifecycle or hook write requires reloading before retrying. Metadata changes do not publish lifecycle events.

## Bounded passes

Every delivery processes at most 100 task/unit keys from one ordered range, including a revisited cursor and retained closed rows. Refreshing reads build issues; finalizing reads issue states. The cursor is always inclusive: revisit it to finish the issue and its history, then continue after it. A full batch retains its active phase and requests redelivery. Moving from refreshing to finalizing conditionally saves a reset cursor before any closing work begins.

A validation remains current after a completed refresh. Its job is acknowledged only after finalization or whole-validation abandonment; older jobs and duplicate completed jobs are ignored. A terminal pass requires a terminal build chosen by its Request in both active phases.

Build IDs are opaque; source order comes from Request IDs. All scans are bounded reads under leading primary-key prefixes. There is no transaction spanning progress, issue state, and history.

A finalized checkpoint retains `issues_reconciled` in its Request's history before acknowledgement or replacement by a newer pass. The build ID identifies the occurrence. Metadata distinguishes normal reconciliation from cancellation or task-wide suppression of absent-issue closure. A completed nonterminal refresh adds no request-history entry. If history fails after checkpoint persistence, the next primary or recovery delivery repairs that entry before replacing the checkpoint.

## Recovery and activation

Retryable errors return to normal consumer retry, and the next delivery resumes the cursor before advancing. Abandoned analysis and requests that end without a build outcome cannot complete tracking, so they return non-retryable errors and go directly to the DLQ. The DLQ controller owns all abandonment cleanup and stops the failed pass rather than rerunning its remaining batches:

1. Conditionally enter `abandoning`, retaining the source, notification, and cursor. Normal deliveries hold for 30 seconds while this phase is present; they cannot perform cleanup or restart issue application.
2. If there is a cursor, read its issue and conditionally advance progress after that read. Advance the issue's version without changing its status or transition metadata. This invalidates a paused worker's earlier row guard. If the row has not been created yet, finish that one opening instead: the normal worker can only have saved such a cursor after finding a reported build issue. Creating it prevents the delayed create from succeeding. Closed issue rows are never deleted.
3. Repair the cursor's opening or closing history, summary, and open-issue lookup from the retained row. Record an `issue_tracking_abandoned` Request-history event keyed by the failed notification, with its build ID.
4. Conditionally enter `abandoned`. Keep the source-order boundary and notification identity, so the same delivery and older builds cannot restart the abandoned work.

Accepted opens and closes remain in place. Recovery does not scan either issue range, close the remaining issues, change Request or Build outcomes, or release build slots. Abandoning a request-scoped job ends that validation's issue tracking, even if only an ordinary refresh had started. A pre-build abandonment retains the request boundary with an empty build ID and no issue cursor, fencing delayed workers before newer requests proceed. A later successful build can reconcile issues left unchanged by the failed pass.

Recovery must finish fencing the current issue and retaining its history before admitting newer work. If a normal write wins the issue CAS first, recovery retries against that accepted row. If a replacement recovery worker advances first, its issue-version change rejects the old write. Merely marking the pass abandoned without touching the cursor row would not provide this protection.

If a newer job reaches recovery while an earlier checkpoint is unfinished, recovery abandons that checkpoint first, then requests immediate redelivery to abandon the failed job's own work. A delayed DLQ delivery never abandons a newer request.

Storage and history failures return for retry; hosts must pair recovery with an always-retryable, unlimited-retry DLQ consumer. Only recovery work repeats. Malformed message identities, unresolvable queues, and missing source records are logged and acknowledged.

The reference service registers queue-partitioned `issuetrack` and `issuetrack_dlq` subscriptions. Normal processing uses backend error classification and the extended bounded retry policy, while recovery uses the existing always-retryable DLQ consumer. The DLQ consumer starts before normal consumption and stops after it. Deploy recovery-aware controllers and provision the issue tables before enabling consumption; older controllers do not recognize the abandonment phases. Completed opening passes publish repair notifications; recovery does not. The reference host also consumes repair notifications with a no-op executor; fix dead-letter recovery retains submission outcomes without calling the executor.

## Analysis handoff

Each validation uses the single job registered at admission; analysis does not publish tracking messages. Finalization requires successful terminal detection and the Request's recorded terminal build. Terminal detection returns the complete cumulative issue set, including issues returned by earlier calls, so finalization does not depend on earlier workers finishing their writes. Nonterminal detection may omit earlier issues without removing them from the build's accumulated results. The reference host uses a no-op detector; deployments supply their own detector and register the same normal and recovery paths.

## Rollout

Provision the non-null `request.build_id` column with an empty default before deploying. This is an additive association; existing terminal requests can still use their winning build ID. No new tables, secondary indexes, or cross-key transactions are required.

Pause admission and drain pre-upgrade validations, completing or explicitly abandoning the existing tracking checkpoint before enabling request-scoped registration. Upgrade normal and recovery consumers together and use the single-head tracking subscription before resuming admission. Old per-analysis notifications are not supported. Do not mix old publishers or consumers with the request-scoped flow. To revert, pause admission and finish or safely abandon request-scoped jobs before restoring the old consumers; the added column can remain.

## Per-request issue summaries

The existing issue log is the source for both queue-level lifecycle projections and request-level summaries. Opening and closing events produce New and Closed request results. A reported event records an already-open issue encountered by a later build and produces Existing; it does not change issue status, transition time, or the open-issue lookup. Each event retains its opening build reference, and the materializer loads the source build to identify the request. Request-wide completion and abandonment events remain in request history.

The cursor cannot advance until the accepted event and all its projections are durable. Retry repairs missing projections from the retained event, preserving the original timestamp and opening. A closure wins over an opening or report from the same request, with conditional writes preventing an older replay from changing it back. Recovery repairs lifecycle history and any accepted report before abandonment; merely fencing an older row does not attribute a report to the abandoned request.

Existing lifecycle events can lack the additive opening reference. Their lifecycle projections continue using the supplied opening, but those records do not create request summaries from information absent from retained history. Requests begun before recording was deployed have no coverage marker and are not backfilled. Deploy the opening-reference column before updated writers; recording coverage assumes every worker uses the updated contract.
