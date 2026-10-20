# First-failure repair submission

Build analysis records confirmed failures. The `fix` queue step selects the first eligible failed unit for each build and submits one repair through a Go extension. Submission can happen while the build is running. See the [workflow](workflow.md#workflow).

## 1. Publish a confirmed failure

[`buildanalysis.Controller.Process`](../../../stovepipe/controller/buildanalysis/buildanalysis.go) obtains confirmed [`BuildIssue`](../../../stovepipe/entity/build_issue.go) observations from [`buildissue.Detector`](../../../stovepipe/extension/buildissue/buildissue.go). The detector owns provider confirmation and retry rules.

The build-analysis integration persists the per-build observation through [`BuildIssueStore`](../../../stovepipe/extension/storage/build_issue_store.go) and reconciles its [`OpenIssue`](../../../stovepipe/entity/open_issue.go). Each continuous opening retains its immutable `FirstSeenBuildID`. A recurrence after recovery has a new opening build. Detecting builds and opening builds are separate identities.

Publish [`FixRequest`](../../../stovepipe/core/messagequeue/proto/fix.proto) with the queue, detecting BuildID, TaskID, UnitID, and FirstSeenBuildID. Partition by detecting BuildID. Replay publication after duplicate observation writes so interrupted handoffs can finish.

## 2. Select one issue per build

[`fix.Controller.Process`](../../../stovepipe/controller/fix/fix.go) reloads the Build and Request and checks queue identity. For a new attempt, it verifies the persisted failure and current issue opening.

1. Create a queued [`Fix`](../../../stovepipe/entity/fix.go) using the detecting BuildID as its queue-scoped ID. Create-if-absent selects the first eligible request processed, including when deliveries race.
2. If that key already exists, reload its saved selection. Preparation retries use the saved issue, even when another target caused the delivery.
3. Before dispatch, recheck the selected opening. If it recovered or reopened, mark the queued attempt skipped and retain the build's selection.

Build analysis continues collecting all failures. Later deliveries reconcile the saved submission and its history. Each build has its own initial attempt; cross-build deduplication and decisions about additional root causes are follow-up work.

## 3. Submit once

[`RepairExecutor.Submit`](../../../stovepipe/extension/fixer/fixer.go) accepts `SubmitRequest{BuildID, Issue}` and returns a nonempty opaque provider reference or an error. The executor is bound to its queue by its factory. Resolve it before claiming dispatch.

Conditionally persist `FixSubmitting` before calling Submit. Only the delivery whose conditional write succeeds calls the executor. Redelivery after that marker reconciles history and finishes without another trigger.

On acceptance, persist `FixSubmitted` and `ExecutionRef`. On an error or empty reference, persist `FixSubmissionUnknown` and the diagnostic in metadata. A trigger error can follow acceptance, so it leaves execution unknown. A failed result write leaves the submitting marker.

This is at-most-once dispatch. A crash between the durable marker and the outbound call can leave the repair unlaunched. A lost response leaves acceptance unknown. These cases require explicit follow-up. Execution monitoring can be added separately.

## 4. Record request history and provenance

Record `fix_submission_started` for the durable dispatch claim and `fix_submission_recorded` for a retained accepted or uncertain trigger result. Stable occurrence IDs derive from the fix ID. Replay reconstructs missing history from the saved Fix, including when a later target supplies the delivery. History failures never repeat submission. An attempt left submitting has only a started event.

[`FixStore`](../../../stovepipe/extension/storage/fix_store.go) adds one table with primary-key create, get, and conditional update. It retains the detecting BuildID, issue-opening references, submission state, optional provider reference and metadata, timestamps, and version. Build and OpenIssue keep their existing schemas.

Navigate `Fix.BuildID → Build.RequestID → Request.URI` for the initiating request and change. `Fix.Issues` retains opening identities after recovery. Build analysis owns failure and recovery evidence.

## Integration

[`core/fixrequest.Publish`](../../../stovepipe/core/fixrequest/publish.go) is the handoff after observation and positive open-issue persistence. The build-analysis owner supplies reconciliation and calls the helper, including on replay. The reference host retains its no-op detector until that integration lands.

The initial flow ends after submission and request-history recording. Execution monitoring, recovery correlation, and decisions about related failures can build on these records later.
