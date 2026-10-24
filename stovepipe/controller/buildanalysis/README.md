# Build analysis

## Observations

The controller reloads the queue-scoped Build, invokes the queue's issue detector, and persists each returned observation individually. Observations already stored for the same build, task, and unit within a queue are not persisted again, so repeated polls and retries after partial writes are safe. Detector results accompanying an error are discarded.

After persistence, the controller publishes an issue-tracking notification partitioned by queue. Issue tracking maintains open issues and lifecycle history; analysis never writes them directly. An empty detection result does not delete earlier observations.

## Issue-tracking handoff

Refresh notifications let issue tracking open observed issues without resolving absent ones. Final notifications let it also resolve open issues absent from the build's accumulated observations. A successful terminal detection must discover all remaining reportable issues; it need not repeat earlier observations. An empty terminal response still triggers finalization, but does not retract earlier observations from the same build. See the [detector contract](../../extension/buildissue/README.md).

All analysis deliveries for a build must share its build-ID partition and consumer group. The controller validates that partition key. Finalization requires a successful pass that began with a terminal Build and the Request's recorded winning build ID. If status becomes terminal before that outcome is recorded, analysis publishes a refresh and holds until the outcome is available. Known losing builds do not publish. A terminal Request without a recorded winning build permits a refresh but not absence-based resolution.

Every refresh gets a fresh notification ID because held polling deliveries do not identify individual passes. A fixed ID would suppress later refreshes after earlier notifications were consumed. Retries can therefore publish duplicate refreshes; issue tracking is idempotent. Final notifications use one deterministic ID per build, so duplicate terminal passes deduplicate. Publication errors return through the normal analysis retry/DLQ policy; no hold or successful completion occurs before publication succeeds.

## Polling and completion

Every delivery reloads the Build and its Request before detection. After a successful pass, a nonterminal Build holds the delivery for two minutes; a terminal Build completes after persistence and publication once its Request outcome is known, or the Request has ended without one. A pass that begins with a nonterminal Build always holds, even if the Build finishes during detection, so another delivery analyzes it after observing terminal status. More than one delivery may analyze the same terminal Build; observation persistence remains idempotent.

If the Request is terminal but the loaded Build is nonterminal, analysis reloads the Build before deciding to stop. The Build may have finished between the initial Build read and the Request read; reloading avoids skipping its final analysis. If it is still nonterminal, analysis stops without detection or a hold. This can occur when the status-polling DLQ handler fails the Request without learning the actual build result, leaving the Build at its last observed status.

Status polling and updates remain the responsibility of buildsignal. Analysis never changes Request state or releases build slots.

## Errors

Detection, persistence, publication, configuration, state-loading, and message-validation errors are returned to the consumer for its normal retry or DLQ policy, whether the Build is running or terminal. Analysis errors do not use the two-minute polling hold. The consumer classifies errors and applies its configured retry limits and backoff; a final analysis can still fail and dead-letter.

## DLQ recovery

The recovery controller records a `build_analysis_abandoned` event in the Request's history, with the build ID, to indicate that analysis could not complete and issue observations may be incomplete. It does not rerun detection, delete earlier observations, change Request or Build state, or release a build slot. Original failure details are logged rather than included in the immutable history event.

History identity is stable per build, so repeated recovery deliveries and separate analysis notices for the same build retain one event. Different builds retain different events, even for the same Request. A later successful analysis does not erase the historical occurrence.

State-read and history-write errors are returned for retry. Hosts must run recovery with the DLQ consumer's always-retryable processor and unlimited-retry subscription; only recovery work is retried, not analysis. Unusable payloads, invalid or unresolvable queues, missing Build or Request records, and queue mismatches are logged and acknowledged rather than retried indefinitely.

## Host integration

The reference service supplies a no-op detector and publishes analysis notices at build start and completion. It also registers a publish-only issue-tracking topic; issue-tracking consumption and recovery are not enabled yet. It registers the normal analysis controller with bounded retries and the recovery controller with the existing DLQ consumer. See [build issue observation](../../../doc/rfc/stovepipe/steps/buildanalysis.md) for the publication rules and retry configuration.
