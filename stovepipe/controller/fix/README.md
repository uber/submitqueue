# Fix submission

The controller consumes a `FixRequest` containing a queue name and build ID. It pages through a build's durable issue records and skips submission when any record has a fix ID, including recovered issues. Otherwise, it selects one currently open issue whose first-seen build matches this build and calls `RepairExecutor.Submit`.

A returned fix ID is associated with the issue through `BuildIssueStore.Update`, using the version read during selection. A concurrent association is preserved. A submission error without an ID leaves the issue eligible for normal queue retries; an ID returned alongside an error is retained.

Issue tracking calls [`fixrequest.Publish`](../../core/fixrequest/publish.go) after completing a nonempty opening pass and persisting its issue state and history. Publications share a build partition, while the message identity includes the pass notification ID so a later pass can trigger another evaluation. A pass with no eligible issue completes without submitting a repair; a recorded fix ID prevents later passes from submitting another repair for the build.

The one-per-build limit is controller policy. Submission precedes persistence, so overlapping deliveries, crashes, and failed writes can still produce duplicate executor calls. Issue recovery remains separate from repair association.
