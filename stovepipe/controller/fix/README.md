# Fix submission

The controller consumes a `FixRequest` containing a queue name and build ID. It pages through a build's durable issue records and skips submission when any record has a fix ID, including recovered issues. Otherwise, it selects one currently open issue whose first-seen build matches this build and calls `RepairExecutor.Submit`.

A returned fix ID is associated with the issue through `BuildIssueStore.Update`, using the version read during selection. A concurrent association is preserved. A submission error without an ID leaves the issue eligible for normal queue retries; an ID returned alongside an error is retained.

Issue tracking calls [`fixrequest.Publish`](../../core/fixrequest/publish.go) after persisting both the build observation and open issue. Publications for the same build share a message identity and partition.

The one-per-build limit is controller policy. Submission precedes persistence, so overlapping deliveries, crashes, and failed writes can still produce duplicate executor calls. Issue recovery remains separate from repair association.

After an association is durable, the controller retains `repair_submitted` in the owning request history with the build, task, unit, and fix IDs. An existing association repairs missing history without calling the executor again. The event describes the persisted association, including the winner of a concurrent update, and does not imply repair completion.
