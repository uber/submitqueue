# Build issue detection

Build analysis runs alongside status polling and recording. Hosts supply a queue-resolved detector factory. The reference service uses a no-op detector so the analysis stage is always wired without inspecting provider output; deployments replace the factory with their integration. No-op results do not establish build success. The reference host registers an issue-tracking publication route but does not yet consume those notifications.

## Start and completion notices

After persisting a Build and its trigger occurrence, build publishes to buildsignal and buildanalysis. On completion, buildsignal persists status, releases the build slot, records the Request outcome, and publishes to record and buildanalysis. Each pair of publications attempts both destinations even if one fails. Publication is not atomic: one destination can receive its message while the other publication fails.

Start and completion notices use distinct deterministic message IDs and the same build-ID partition. Retries of one notice deduplicate without suppressing the other. A held start delivery can delay the completion notice in that partition, but it also reloads Build status on every pass and can perform terminal analysis itself. Both notices may analyze the terminal Build; duplicate observations are safe.

Publication errors are returned through the producing stage's retry/DLQ policy. Analysis processing does not gate record, but notification publication is still part of the producing stage: a failed start publication can send the build-stage message to its existing fail-closed recovery path.

## Build issues and polling

The [analysis controller](../../../../stovepipe/controller/buildanalysis/README.md) owns detection, per-build issue persistence, polling, and the stop condition for Requests that end without a recorded build result. It accumulates per-build issues and publishes queue-partitioned issue-tracking notifications after persistence; it does not maintain open issues itself. Successful terminal analysis requests final reconciliation only after the Request records that Build as its winning outcome. Final reconciliation uses the accumulated build issues, including issues omitted from the last detector response. See the controller documentation for handoff and deduplication rules.

Status-polling DLQ recovery does not publish a build-completion notice. The analysis polling loop observes the ended Request through storage instead.

## Analysis errors and dead letters

Analysis errors are returned to the normal consumer whether the Build is running or terminal; errors do not use the two-minute polling hold. The reference service uses backend error classification and the extended retry configuration: a maximum of 10 attempts, with retry delays of 5, 10, 20, 40, then 60 seconds, capped at 60 seconds. A successful hold resets failure accounting. Non-retryable errors and exhausted retries send the message to the analysis DLQ, so final analysis is attempted rather than guaranteed to succeed.

The analysis DLQ uses the existing DLQ consumer, with its always-retryable processor and unlimited-retry subscription. The recovery controller records incomplete analysis in Request history without rerunning detection or changing the validation outcome. Only state reads and history persistence retry; unusable messages are logged and acknowledged. See [DLQ recovery](../../../../stovepipe/controller/buildanalysis/README.md#dlq-recovery) for the history identity and error-handling rules.
