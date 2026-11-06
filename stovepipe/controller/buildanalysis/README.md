# Build analysis

## Build issues

The controller reloads the queue-scoped Build, invokes the queue's issue detector, and persists each returned build issue individually. Build issues already stored for the same build, task, and unit within a queue are not persisted again, so repeated polls and retries after partial writes are safe. Detector results accompanying an error are discarded.

Once the Request records the terminal Build as its winning outcome, successful detection and issue persistence retain `build_analysis_completed` to authorize final reconciliation. The event snapshots the build ID and issue count, including zero, from the first retained completion. Retries still detect and persist issues, but reuse the retained count and timestamp; a changed count is logged as a detector discrepancy and does not block completion. Concurrent deliveries and uncertain writes converge on the first stored snapshot, while other identity conflicts remain errors. Nonterminal polls and terminal passes waiting for the winning outcome add no completion entry.

Admission registers a queue-partitioned tracking job for each validation before build dispatch. After persistence, analysis retains terminal completion in request history; the tracking job reads that event independently of queue delivery. Issue tracking maintains open issues and lifecycle history; analysis never writes them directly. An empty detection result does not delete earlier build issues.

## Issue-tracking handoff

The request-scoped tracking job reads persisted build issues during ordinary refreshes without closing other issues. Once analysis records completion, tracking can also close issues absent from the accumulated build issues, unless any task-wide failure was recorded or the build was canceled. Analysis does not publish tracking messages.

A successful terminal detection must return the complete cumulative issue set for the build, including issues returned by earlier calls. Persisting that result before recording completion makes finalization independent of whether an earlier worker finished saving its results. An empty terminal result is valid only if the build has no reportable issues; inability to determine the complete set must return an error instead. See the [detector contract](../../extension/buildissue/README.md).

All analysis deliveries for a build share its build-ID partition and consumer group. Completion requires a successful pass that began with a terminal Build and the Request's recorded winning build ID. If status becomes terminal before that outcome is recorded, analysis persists the issues and holds until the outcome is available. Known losing builds stop without detection. A Request that ends without a winning build cannot authorize closing issues based on absence; the tracking job goes to its DLQ for cleanup.

## Polling and completion

Every delivery reloads the Build and its Request before detection. After a successful pass, a nonterminal Build holds the delivery for two minutes; a terminal Build completes after persistence and completion history once its Request outcome is known, or the Request has ended without one. A pass that begins with a nonterminal Build always holds, even if the Build finishes during detection, so another delivery analyzes it after observing terminal status. More than one delivery may analyze the same terminal Build; build-issue persistence remains idempotent.

If the Request is terminal but the loaded Build is nonterminal, analysis reloads the Build before deciding to stop. The Build may have finished between the initial Build read and the Request read; reloading avoids skipping its final analysis. If it is still nonterminal, analysis retains abandonment and stops without detection or a hold. This can occur when the status-polling DLQ handler fails the Request without learning the actual build result, leaving the Build at its last observed status.

Status polling and updates remain the responsibility of buildsignal. Analysis never changes Request state or releases build slots.

## Errors

Detection, persistence, configuration, state-loading, and message-validation errors are returned to the consumer for its normal retry or DLQ policy, whether the Build is running or terminal. Analysis errors do not use the two-minute polling hold. The consumer classifies errors and applies its configured retry limits and backoff; a final analysis can still fail and dead-letter.

## DLQ recovery

The recovery controller records a `build_analysis_abandoned` event in the Request's history, with the build ID, to indicate that analysis could not complete and the recorded build issues may be incomplete. It does not rerun detection, delete earlier build issues, change Request or Build state, or release a build slot. Original failure details are logged rather than included in the immutable history event.

History identity is stable per build, so repeated recovery deliveries and separate analysis notices for the same build retain one event. Different builds retain different events, even for the same Request. A later successful analysis does not erase the historical occurrence.

State-read and history-write errors are returned for retry. Hosts must run recovery with the DLQ consumer's always-retryable processor and unlimited-retry subscription; only recovery work is retried, not analysis. Unusable payloads, invalid or unresolvable queues, missing Build or Request records, and queue mismatches are logged and acknowledged rather than retried indefinitely.

## Host integration

The reference service supplies a no-op detector, registers tracking at admission, and publishes analysis notices at build start and completion. It registers both analysis and issue tracking with bounded normal retries and the existing DLQ consumer for recovery. See [issue tracking](../issuetrack/README.md) for reconciliation and recovery behavior and [build issue detection](../../../doc/rfc/stovepipe/steps/buildanalysis.md) for the publication rules and retry configuration.
