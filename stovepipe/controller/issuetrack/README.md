# Issue tracking

Issue tracking maintains current issue state and opening/closing history from persisted build issues. Each delivery applies a bounded batch of issues and requests immediate redelivery when more work remains. No consumer is registered yet.

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

An unfinished pass completes before another source is admitted. A notification that helps finish a different pass requests redelivery before handling its own source. Completed notifications and older sources are ignored, as are refreshes after the same Request has finalized. A terminal pass requires a terminal build chosen by its Request in both active phases.

Build IDs are opaque; source order comes from Request IDs. All scans are bounded reads under leading primary-key prefixes. There is no transaction spanning progress, issue state, and history.

## Recovery and activation

An error returns to normal consumer retry. The next delivery resumes the cursor before advancing. Permanently failing cursor work blocks issue tracking for that queue; terminal recovery and consumer registration remain follow-up work. This PR does not enable consumption. The host must provide a fix publication route and a repair executor when registering the fix consumer.

## Analysis handoff

Build analysis publishes refresh or final notifications after persisting build issues. Final notification requires successful terminal detection and the Request's recorded terminal build. Terminal detection returns the complete cumulative issue set, including issues returned by earlier calls, so finalization does not depend on earlier workers finishing their writes. Nonterminal detection may omit earlier issues without removing them from the build's accumulated results. The reference host registers the publication route but does not enable issue-tracking consumption or recovery.
