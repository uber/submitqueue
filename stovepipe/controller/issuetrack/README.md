# Issue tracking

Issue tracking maintains current issue state and opening/closing history from persisted build issues. This layer applies one issue at a time; notification handling and bounded passes follow separately. No consumer is registered yet.

## Applying one issue

1. Read whether the build reports the issue and load its current state.
2. Reject a source older than the issue's last processed build, using Request order rather than opaque Build IDs.
3. Before changing the issue, conditionally save its task/unit key as the current queue cursor **after** reading the issue row. Repeat this conditional write even when resuming the same cursor.
4. Conditionally create or update the issue row. Record the processed build even when its open/closed status stays the same; update the changing build and timestamp only on an actual transition.
5. Record opening or closing history from the issue row's changing build and timestamp. On retry, the inclusive cursor revisits that row and repairs its history before proceeding.

Closed issue rows are retained, so their versions cannot reset on reopening. A repeated result from the same build needs no new issue write. Its history is repaired when revisiting the current cursor.

## Ordering requirements

The caller must finish the current cursor and its history before moving on or admitting another build. If a replacement worker advances, completing that cursor changes the issue version first. A paused worker's earlier issue write then fails its row guard. A worker that reads later is rejected by source ordering or the conditional cursor write.

The post-read cursor write and processed-build updates, including unchanged statuses, are required for this ordering. A progress version alone is not a lock around issue writes. These operations use individual K-V reads, conditional writes, and immutable history creates; no cross-record transaction or stored future-change payload is needed.
