# Fix submission

The controller consumes a `FixRequest` containing a queue name and build ID. It pages through a build's durable issue records and skips submission when any record has a fix ID, including recovered issues. Otherwise, it selects one currently open issue whose first-seen build matches this build and calls `RepairExecutor.Submit`.

A returned fix ID is associated with the issue through `BuildIssueStore.Update`, using the version read during selection. A concurrent association is preserved. A submission error without an ID leaves the issue eligible for normal queue retries; an ID returned alongside an error is retained.

Issue tracking calls [`fixrequest.Publish`](../../core/fixrequest/publish.go) after completing a nonempty opening pass and persisting its issue state and history. Publications share a build partition, while the message identity includes the pass notification ID so a later pass can trigger another evaluation. A pass with no eligible issue completes without submitting a repair; a recorded fix ID prevents later passes from submitting another repair for the build.

The one-per-build limit is controller policy. Submission precedes persistence, so overlapping deliveries, crashes, and failed writes can still produce duplicate executor calls. Issue recovery remains separate from repair association.

A durable association retains `fix_submitted` in the owning Request's history, with build, task, unit, and fix IDs. A digest of the fix ID identifies the occurrence and fits the history key limit; metadata retains the verbatim fix ID. An existing association repairs missing history before skipping submission, including when the issue has since recovered. Without an eligible issue, the controller retains `fix_submission_skipped` once per build with reason `no_eligible_issue`. These entries do not change Request outcome or establish issue recovery.

The DLQ controller reads the same durable decision without calling the executor. It repairs submitted or skipped history when possible; otherwise, an eligible issue without a fix ID retains `fix_submission_abandoned` once per build with reason `acceptance_unknown`. An executor error without an ID can follow acceptance, so this event does not claim rejection. Source and history errors retry. Unusable payloads, unresolvable queues, and missing or mismatched sources are logged and acknowledged. The reference host registers recovery with unlimited, always-retryable DLQ processing to preserve history repair through a storage outage.
