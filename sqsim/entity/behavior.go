// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entity

// Behavior groups the external operations encountered by a Land.
type Behavior struct {
	// BuildRunner describes build triggering and status polling.
	BuildRunner BuildRunnerBehavior `json:"build_runner"`
	// MergeConflictCheck describes Runway dry-run mergeability checks.
	MergeConflictCheck MergeConflictCheckBehavior `json:"merge_conflict_check"`
	// Merge describes Runway committing merge calls.
	Merge MergeBehavior `json:"merge"`
}

// Invocation describes one call made to an external system.
type Invocation[T any] struct {
	// DelayMs is synchronous provider latency before the call returns.
	DelayMs int64 `json:"delay_ms"`
	// Outcome is the result applied by the external system.
	Outcome T `json:"outcome"`
	// Fault optionally changes how the result is returned to the caller.
	Fault Fault `json:"fault"`
}

// Fault describes an error returned by a modeled external operation.
type Fault struct {
	// Kind identifies the classification of the returned error.
	Kind FaultKind `json:"kind"`
	// Phase states whether the outcome is applied before the error.
	Phase FaultPhase `json:"phase"`
}

// FaultKind identifies a modeled error classification.
type FaultKind string

const (
	// FaultNone indicates that an invocation returns normally.
	FaultNone FaultKind = ""
	// FaultRetryable indicates a transient infrastructure error.
	FaultRetryable FaultKind = "retryable"
	// FaultNonRetryable indicates a permanent infrastructure error.
	FaultNonRetryable FaultKind = "non_retryable"
)

// FaultPhase identifies when an error occurs relative to an operation's side effect.
type FaultPhase string

const (
	// FaultBeforeSideEffect indicates that the operation did not apply its outcome.
	FaultBeforeSideEffect FaultPhase = "before_side_effect"
	// FaultAfterSideEffect indicates that the outcome was applied before the response failed.
	FaultAfterSideEffect FaultPhase = "after_side_effect"
)

// BuildRunnerBehavior describes BuildRunner Trigger and Status behavior.
type BuildRunnerBehavior struct {
	// Triggers are consumed by logical Trigger operations in declaration order.
	Triggers []Invocation[BuildTriggerOutcome] `json:"triggers"`
}

// BuildTriggerOutcome describes the external build created by Trigger.
type BuildTriggerOutcome struct {
	// Build is the build created by a successful Trigger side effect.
	Build BuildExecution `json:"build"`
}

// BuildExecution describes status over elapsed wall time for one build.
type BuildExecution struct {
	// Timeline describes status as elapsed wall time from build creation.
	Timeline []BuildStatusPoint `json:"timeline"`
	// StatusFaults inject errors into selected Status calls.
	StatusFaults []FaultOnCall `json:"status_faults"`
}

// BuildStatusPoint is the status visible at and after one elapsed offset.
type BuildStatusPoint struct {
	// AfterMs is elapsed wall time since build creation in milliseconds.
	AfterMs int64 `json:"after_ms"`
	// Status is the status visible at and after AfterMs.
	Status BuildStatus `json:"status"`
}

// FaultOnCall injects a fault into one numbered method call.
type FaultOnCall struct {
	// Call is the one-based invocation number.
	Call int `json:"call"`
	// Fault is the error returned by that invocation.
	Fault Fault `json:"fault"`
}

// BuildStatus is a provider-neutral modeled build status.
type BuildStatus string

const (
	// BuildAccepted indicates that the runner accepted the build.
	BuildAccepted BuildStatus = "accepted"
	// BuildRunning indicates that the build is executing.
	BuildRunning BuildStatus = "running"
	// BuildSucceeded indicates terminal build success.
	BuildSucceeded BuildStatus = "succeeded"
	// BuildFailed indicates terminal build failure.
	BuildFailed BuildStatus = "failed"
	// BuildCancelled indicates terminal cancellation.
	BuildCancelled BuildStatus = "cancelled"
)

// IsTerminal reports whether the modeled build status is terminal.
func (s BuildStatus) IsTerminal() bool {
	return s == BuildSucceeded || s == BuildFailed || s == BuildCancelled
}

// MergeConflictCheckBehavior describes CheckMergeability calls.
type MergeConflictCheckBehavior struct {
	// Invocations are consumed by CheckMergeability calls in declaration order.
	Invocations []Invocation[MergeConflictCheckOutcome] `json:"invocations"`
}

// MergeConflictCheckOutcome is the business result of a mergeability check.
type MergeConflictCheckOutcome string

const (
	// Mergeable indicates that the changes apply cleanly.
	Mergeable MergeConflictCheckOutcome = "mergeable"
	// MergeConflict indicates a terminal merge conflict.
	MergeConflict MergeConflictCheckOutcome = "conflict"
)

// MergeBehavior describes committing Merge calls.
type MergeBehavior struct {
	// Invocations are consumed by committing Merge calls in declaration order.
	Invocations []Invocation[MergeOutcome] `json:"invocations"`
}

// MergeOutcome describes the business result of a committing merge.
type MergeOutcome struct {
	// Result identifies the merge result.
	Result MergeResult `json:"result"`
}

// MergeResult is the modeled business result of a committing merge.
type MergeResult string

const (
	// MergeSucceeded indicates that the merge committed successfully.
	MergeSucceeded MergeResult = "succeeded"
)
