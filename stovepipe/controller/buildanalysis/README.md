# Build analysis

## Build issues

The controller reloads the queue-scoped Build, invokes the queue's issue detector, and persists each returned build issue individually. Build issues already stored for the same build, task, and unit within a queue are not persisted again, so repeated polls and retries after partial writes are safe. Detector results accompanying an error are discarded.

After persistence, the controller publishes an issue-tracking notification partitioned by queue. Issue tracking maintains open issues and lifecycle history; analysis never writes them directly. An empty detection result does not delete earlier build issues.

## Issue-tracking handoff

Refresh notifications let issue tracking open issues reported by the build without closing other open issues. Final notifications let it also close open issues absent from the build's accumulated issues, unless any task-wide failure was recorded or the build was canceled. A successful terminal detection must return the complete cumulative issue set for the build, including issues returned by earlier calls. Persisting that complete result before publication makes finalization independent of whether an earlier worker finished saving its results. An empty terminal result is valid only if the build has no reportable issues; inability to determine the complete set must return an error instead. See the [detector contract](../../extension/buildissue/README.md).

All analysis deliveries for a build must share its build-ID partition and consumer group. The controller validates that partition key. Finalization requires a successful pass that began with a terminal Build and the Request's recorded winning build ID. If status becomes terminal before that outcome is recorded, analysis publishes a refresh and holds until the outcome is available. Known losing builds do not publish. A terminal Request without a recorded winning build permits a refresh but not closing issues based on absence.

Every refresh gets a fresh notification ID because held polling deliveries do not identify individual passes. A fixed ID would suppress later refreshes after earlier notifications were consumed. Retries can therefore publish duplicate refreshes; issue tracking is idempotent. Final notifications use one deterministic ID per build, so duplicate terminal passes deduplicate. Publication errors return through the normal analysis retry/DLQ policy; no hold or successful completion occurs before publication succeeds.

## Polling and completion

Every delivery reloads the Build and its Request before detection. After a successful pass, a nonterminal Build holds the delivery for two minutes; a terminal Build completes after persistence and publication once its Request outcome is known, or the Request has ended without one. A pass that begins with a nonterminal Build always holds, even if the Build finishes during detection, so another delivery analyzes it after observing terminal status. More than one delivery may analyze the same terminal Build; build-issue persistence remains idempotent.

If the Request is terminal but the loaded Build is nonterminal, analysis reloads the Build before deciding to stop. The Build may have finished between the initial Build read and the Request read; reloading avoids skipping its final analysis. If it is still nonterminal, analysis stops without detection or a hold. This can occur when the status-polling DLQ handler fails the Request without learning the actual build result, leaving the Build at its last observed status.

Status polling and updates remain the responsibility of buildsignal. Analysis never changes Request state or releases build slots.

## Errors

Detection, persistence, publication, configuration, state-loading, and message-validation errors are returned to the consumer for its normal retry or DLQ policy, whether the Build is running or terminal. Analysis errors do not use the two-minute polling hold. The consumer classifies errors and applies its configured retry limits and backoff; a final analysis can still fail and dead-letter.

## DLQ recovery

The recovery controller records a `build_analysis_abandoned` event in the Request's history, with the build ID, to indicate that analysis could not complete and the recorded build issues may be incomplete. It does not rerun detection, delete earlier build issues, change Request or Build state, or release a build slot. Original failure details are logged rather than included in the immutable history event.

History identity is stable per build, so repeated recovery deliveries and separate analysis notices for the same build retain one event. Different builds retain different events, even for the same Request. A later successful analysis does not erase the historical occurrence.

State-read and history-write errors are returned for retry. Hosts must run recovery with the DLQ consumer's always-retryable processor and unlimited-retry subscription; only recovery work is retried, not analysis. Unusable payloads, invalid or unresolvable queues, missing Build or Request records, and queue mismatches are logged and acknowledged rather than retried indefinitely.

## Host integration

The reference service supplies a no-op detector and publishes analysis notices at build start and completion. It registers both analysis and issue tracking with bounded normal retries and the existing DLQ consumer for recovery. See [issue tracking](../issuetrack/README.md) for reconciliation and recovery behavior and [build issue detection](../../../doc/rfc/stovepipe/steps/buildanalysis.md) for the publication rules and retry configuration.
