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

// Package sqsim provides the author-facing SQSim scenario DSL.
package sqsim

import "github.com/uber/submitqueue/sqsim/entity"

// Public aliases keep scenario source concise while immutable data remains in entity.
type (
	Scenario                   = entity.Scenario
	Land                       = entity.Land
	Expectation                = entity.Expectation
	ExpectedRequestStatus      = entity.ExpectedRequestStatus
	Behavior                   = entity.Behavior
	Fault                      = entity.Fault
	FaultKind                  = entity.FaultKind
	FaultPhase                 = entity.FaultPhase
	BuildRunnerBehavior        = entity.BuildRunnerBehavior
	BuildTriggerOutcome        = entity.BuildTriggerOutcome
	BuildExecution             = entity.BuildExecution
	BuildStatusPoint           = entity.BuildStatusPoint
	FaultOnCall                = entity.FaultOnCall
	BuildStatus                = entity.BuildStatus
	MergeConflictCheckBehavior = entity.MergeConflictCheckBehavior
	MergeConflictCheckOutcome  = entity.MergeConflictCheckOutcome
	MergeBehavior              = entity.MergeBehavior
	MergeOutcome               = entity.MergeOutcome
	MergeResult                = entity.MergeResult
)

const (
	RequestLanded    = entity.RequestLanded
	RequestError     = entity.RequestError
	RequestCancelled = entity.RequestCancelled

	FaultNone         = entity.FaultNone
	FaultRetryable    = entity.FaultRetryable
	FaultNonRetryable = entity.FaultNonRetryable

	FaultBeforeSideEffect = entity.FaultBeforeSideEffect
	FaultAfterSideEffect  = entity.FaultAfterSideEffect

	BuildAccepted  = entity.BuildAccepted
	BuildRunning   = entity.BuildRunning
	BuildSucceeded = entity.BuildSucceeded
	BuildFailed    = entity.BuildFailed
	BuildCancelled = entity.BuildCancelled

	Mergeable      = entity.Mergeable
	MergeConflict  = entity.MergeConflict
	MergeSucceeded = entity.MergeSucceeded
)
