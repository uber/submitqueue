# Issue tracking

Issue tracking maintains current issue state and opening/closing history from persisted build issues. Each delivery applies a bounded batch of issues and requests immediate redelivery when more work remains. The reference service registers both the normal and recovery consumers.

## Applying one issue

1. Read whether the build reports the issue and load its current state.
2. Reject a source older than the issue's last processed build, using Request order rather than opaque Build IDs.
3. Before changing the issue, conditionally save its task/unit key as the current queue cursor **after** reading the issue row. Repeat this conditional write even when resuming the same cursor.
4. Conditionally create or update the issue row. Record the processed build even when its open/closed status stays the same; update the changing build and timestamp only on an actual transition.
5. Record opening or closing history from the issue row's changing build and timestamp. On retry, the inclusive cursor revisits that row and repairs its history before proceeding.

Closed issue rows are retained, so their versions cannot reset on reopening. A repeated result from the same build needs no new issue write. Its history is repaired when revisiting the current cursor.

## Ordering requirements

During issue application, the caller must finish the current cursor and its history before moving on or admitting another build. If a replacement worker advances, completing that cursor changes the issue version first. A paused worker's earlier issue write then fails its row guard. A worker that reads later is rejected by source ordering or the conditional cursor write.

The post-read cursor write and processed-build updates, including unchanged statuses, are required for this ordering. A progress version alone is not a lock around issue writes. These operations use individual K-V reads, conditional writes, and immutable history creates; no cross-record transaction or stored future-change payload is needed.

## Refreshing and finalizing

Every pass starts in `refreshing`, applying the build's accumulated issues in primary-key order. Reported issues are opened or kept open; nothing is closed. A terminal notification retains `Finalize = true` in the progress record, so a replacement worker can distinguish terminal work from an ordinary refresh even when helping it through another notification. Encountering a task-wide issue (empty unit ID) sets `HasTaskWideIssues` in that same versioned record. This fact remains set across batches and retries, including when the issue was already applied by an earlier pass.

Once the entire build-issue range is exhausted, an opening pass with a nonempty task cursor publishes a build-scoped repair request before saving its next phase. Empty passes and closing sweeps do not publish. Publication failures leave the pass active so retries repeat the handoff after ensuring issue state and history are durable. The repair message identity combines the build and persisted pass notification IDs: retries share an identity, while later passes can trigger another evaluation. The fix controller selects an eligible issue and submits at most one repair per build.

Once the entire build-issue range is exhausted:

- An ordinary refresh becomes `refreshed`.
- A terminal pass with a task-wide failure, or a canceled build, becomes `finalized` without closing anything. Reported closed issues may still have reopened during refreshing; unreported closed issues remain closed.
- Any other terminal pass becomes `finalizing`, resetting its cursor to the start of the issue-state range.

The `finalizing` phase traverses retained issue states in primary-key order. Each key is looked up in the build's issues: a reported issue remains open, while an absent issue closes. Actual openings and closings are retained in history. Exhausting the issue-state range marks the pass `finalized`, so late refreshes are ignored.

No closure occurs before all accumulated build issues have been processed. A task-wide failure in a later batch therefore protects existing issues across every task, not just keys encountered afterward. A later build makes its own decision; the restriction does not carry over. Both walks consume one ordered range at a time; no merge is required.

## Bounded passes

Every delivery processes at most 100 task/unit keys from one ordered range, including a revisited cursor and retained closed rows. Refreshing reads build issues; finalizing reads issue states. The cursor is always inclusive: revisit it to finish the issue and its history, then continue after it. A full batch retains its active phase and requests redelivery. Moving from refreshing to finalizing conditionally saves a reset cursor before any closing work begins.

An unfinished pass completes or is safely abandoned before another source is admitted. A notification that helps finish a different pass requests redelivery before handling its own source. Completed notifications and older sources are ignored, as are refreshes after the same Request has finalized or its terminal pass was abandoned. A terminal pass requires a terminal build chosen by its Request in both active phases.

Build IDs are opaque; source order comes from Request IDs. All scans are bounded reads under leading primary-key prefixes. There is no transaction spanning progress, issue state, and history.

A finalized checkpoint retains `issues_reconciled` in its Request's history before acknowledgement or replacement by a newer pass. The build ID identifies the occurrence. Metadata distinguishes normal reconciliation from cancellation or task-wide suppression of absent-issue closure. A completed nonterminal refresh adds no request-history entry. If history fails after checkpoint persistence, the next primary or recovery delivery repairs that entry before replacing the checkpoint.

## Recovery and activation

An error returns to normal consumer retry. The next delivery resumes the cursor before advancing. Once that retry policy dead-letters a notification, the recovery controller abandons the failed pass rather than rerunning its remaining batches:

1. Conditionally enter `abandoning`, retaining the source, notification, and cursor. Normal deliveries hold for 30 seconds while this phase is present; they cannot start another pass or resume issue application.
2. If there is a cursor, read its issue and conditionally advance progress after that read. Advance the issue's version without changing its status or transition metadata. This invalidates a paused worker's earlier row guard. If the row has not been created yet, finish that one opening instead: the normal worker can only have saved such a cursor after finding a reported build issue. Creating it prevents the delayed create from succeeding. Closed issue rows are never deleted.
3. Repair the cursor's opening or closing history from the retained row. Record an `issue_tracking_abandoned` Request-history event keyed by the failed notification, with its build ID.
4. Conditionally enter `abandoned`. Keep the source-order boundary and notification identity, so the same delivery and older builds cannot restart the abandoned work.

Accepted opens and closes remain in place. Recovery does not scan either issue range, close the remaining issues, change Request or Build outcomes, or release build slots. An abandoned ordinary refresh still permits a new refresh or terminal notification for that Request; an abandoned terminal pass permits only newer Requests. A later successful build can reconcile issues left unchanged by the failed pass.

Recovery must finish fencing the current issue and retaining its history before admitting newer work. If a normal write wins the issue CAS first, recovery retries against that accepted row. If a replacement recovery worker advances first, its issue-version change rejects the old write. Merely marking the pass abandoned without touching the cursor row would not provide this protection.

A notification can fail while helping an older pass. Recovery abandons that saved pass first, then requests immediate redelivery to abandon the failed notification's own work if it has not already been superseded. A delayed DLQ delivery never abandons a newer build or a different notification from the same Request.

Storage and history failures return for retry; hosts must pair recovery with an always-retryable, unlimited-retry DLQ consumer. Only recovery work repeats. Malformed message identities, unresolvable queues, and missing source records are logged and acknowledged.

The reference service registers queue-partitioned `issuetrack` and `issuetrack_dlq` subscriptions. Normal processing uses backend error classification and the extended bounded retry policy, while recovery uses the existing always-retryable DLQ consumer. The DLQ consumer starts before normal consumption and stops after it. Deploy recovery-aware controllers and provision the issue tables before enabling consumption; older controllers do not recognize the abandonment phases. Completed opening passes publish repair notifications; recovery does not. The reference host also consumes repair notifications with a no-op executor; fix dead-letter recovery retains submission outcomes without calling the executor.

## Analysis handoff

Build analysis publishes refresh or final notifications after persisting build issues. Final notification requires successful terminal detection and the Request's recorded terminal build. Terminal detection returns the complete cumulative issue set, including issues returned by earlier calls, so finalization does not depend on earlier workers finishing their writes. Nonterminal detection may omit earlier issues without removing them from the build's accumulated results. The reference host uses a no-op detector; deployments supply their own detector and register the same normal and recovery paths.
