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

package sqsim

import (
	"time"

	"github.com/uber/submitqueue/sqsim/entity"
)

// BuildRunnerBehaviorBuilder constructs BuildRunner behavior.
type BuildRunnerBehaviorBuilder struct {
	triggers []entity.Invocation[entity.BuildTriggerOutcome]
}

// NewBuildRunnerBehavior returns an empty BuildRunner behavior builder.
func NewBuildRunnerBehavior() *BuildRunnerBehaviorBuilder {
	return &BuildRunnerBehaviorBuilder{}
}

// Trigger appends Trigger invocations.
func (b *BuildRunnerBehaviorBuilder) Trigger(invocations ...entity.Invocation[entity.BuildTriggerOutcome]) *BuildRunnerBehaviorBuilder {
	b.triggers = append(b.triggers, invocations...)
	return b
}

// StatusFaultOnCall adds a Status fault to the most recent successful Trigger outcome.
func (b *BuildRunnerBehaviorBuilder) StatusFaultOnCall(call int, fault Fault) *BuildRunnerBehaviorBuilder {
	if len(b.triggers) == 0 {
		return b
	}
	last := &b.triggers[len(b.triggers)-1]
	last.Outcome.Build.StatusFaults = append(last.Outcome.Build.StatusFaults, entity.FaultOnCall{
		Call:  call,
		Fault: fault,
	})
	return b
}

func (b *BuildRunnerBehaviorBuilder) build() (entity.BuildRunnerBehavior, error) {
	triggers := append([]entity.Invocation[entity.BuildTriggerOutcome](nil), b.triggers...)
	for i := range triggers {
		triggers[i].Outcome.Build.Timeline = append([]entity.BuildStatusPoint(nil), triggers[i].Outcome.Build.Timeline...)
		triggers[i].Outcome.Build.StatusFaults = append([]entity.FaultOnCall(nil), triggers[i].Outcome.Build.StatusFaults...)
	}
	return entity.BuildRunnerBehavior{Triggers: triggers}, nil
}

// StatusAt returns a build status timeline point.
func StatusAt(after time.Duration, status BuildStatus) BuildStatusPoint {
	return entity.BuildStatusPoint{AfterMs: after.Milliseconds(), Status: status}
}

// BuildCreated returns a successful Trigger invocation with a build timeline.
func BuildCreated(timeline ...BuildStatusPoint) entity.Invocation[entity.BuildTriggerOutcome] {
	return entity.Invocation[entity.BuildTriggerOutcome]{
		Outcome: entity.BuildTriggerOutcome{
			Build: entity.BuildExecution{Timeline: append([]entity.BuildStatusPoint(nil), timeline...)},
		},
	}
}

// BuildCreatedWithFault returns a Trigger invocation that creates a build and then faults.
func BuildCreatedWithFault(fault Fault, timeline ...BuildStatusPoint) entity.Invocation[entity.BuildTriggerOutcome] {
	invocation := BuildCreated(timeline...)
	invocation.Fault = fault
	return invocation
}

// BuildTriggerFault returns a Trigger invocation that fails without creating a build.
func BuildTriggerFault(fault Fault) entity.Invocation[entity.BuildTriggerOutcome] {
	return entity.Invocation[entity.BuildTriggerOutcome]{Fault: fault}
}

// SuccessfulBuildRunner returns a build behavior that succeeds immediately.
func SuccessfulBuildRunner() *BuildRunnerBehaviorBuilder {
	return NewBuildRunnerBehavior().Trigger(BuildCreated(StatusAt(0, BuildSucceeded)))
}

// BuildSucceededAfter returns a build behavior that succeeds after the given duration.
func BuildSucceededAfter(after time.Duration) *BuildRunnerBehaviorBuilder {
	timeline := []BuildStatusPoint{StatusAt(0, BuildAccepted)}
	if after > 0 {
		timeline = append(timeline, StatusAt(after, BuildSucceeded))
	} else {
		timeline[0] = StatusAt(0, BuildSucceeded)
	}
	return NewBuildRunnerBehavior().Trigger(BuildCreated(timeline...))
}

// BuildFailedAfter returns a build behavior that fails after the given duration.
func BuildFailedAfter(after time.Duration) *BuildRunnerBehaviorBuilder {
	timeline := []BuildStatusPoint{StatusAt(0, BuildAccepted)}
	if after > 0 {
		timeline = append(timeline, StatusAt(after, BuildFailed))
	} else {
		timeline[0] = StatusAt(0, BuildFailed)
	}
	return NewBuildRunnerBehavior().Trigger(BuildCreated(timeline...))
}
