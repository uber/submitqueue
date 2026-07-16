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
	"fmt"

	"github.com/uber/submitqueue/sqsim/entity"
)

// BehaviorBuilder constructs the external behavior attached to one or more Lands.
type BehaviorBuilder struct {
	buildRunner        *BuildRunnerBehaviorBuilder
	mergeConflictCheck *MergeConflictCheckBehaviorBuilder
	merge              *MergeBehaviorBuilder
}

// NewBehavior returns an empty behavior builder.
func NewBehavior() *BehaviorBuilder {
	return &BehaviorBuilder{}
}

// BuildRunner sets the build behavior.
func (b *BehaviorBuilder) BuildRunner(behavior *BuildRunnerBehaviorBuilder) *BehaviorBuilder {
	b.buildRunner = behavior
	return b
}

// MergeConflictCheck sets the dry-run mergeability behavior.
func (b *BehaviorBuilder) MergeConflictCheck(behavior *MergeConflictCheckBehaviorBuilder) *BehaviorBuilder {
	b.mergeConflictCheck = behavior
	return b
}

// Merge sets the committing merge behavior.
func (b *BehaviorBuilder) Merge(behavior *MergeBehaviorBuilder) *BehaviorBuilder {
	b.merge = behavior
	return b
}

func (b *BehaviorBuilder) build() (entity.Behavior, error) {
	if b == nil {
		return entity.Behavior{}, fmt.Errorf("behavior builder is nil")
	}
	if b.buildRunner == nil {
		return entity.Behavior{}, fmt.Errorf("build runner behavior is required")
	}
	if b.mergeConflictCheck == nil {
		return entity.Behavior{}, fmt.Errorf("merge conflict check behavior is required")
	}
	if b.merge == nil {
		return entity.Behavior{}, fmt.Errorf("merge behavior is required")
	}

	buildRunner, err := b.buildRunner.build()
	if err != nil {
		return entity.Behavior{}, fmt.Errorf("build runner: %w", err)
	}
	mergeConflictCheck, err := b.mergeConflictCheck.build()
	if err != nil {
		return entity.Behavior{}, fmt.Errorf("merge conflict check: %w", err)
	}
	merge, err := b.merge.build()
	if err != nil {
		return entity.Behavior{}, fmt.Errorf("merge: %w", err)
	}
	return entity.Behavior{
		BuildRunner:        buildRunner,
		MergeConflictCheck: mergeConflictCheck,
		Merge:              merge,
	}, nil
}

func cloneBehavior(behavior entity.Behavior) entity.Behavior {
	triggers := make([]entity.Invocation[entity.BuildTriggerOutcome], len(behavior.BuildRunner.Triggers))
	for i, trigger := range behavior.BuildRunner.Triggers {
		trigger.Outcome.Build.Timeline = append([]entity.BuildStatusPoint(nil), trigger.Outcome.Build.Timeline...)
		trigger.Outcome.Build.StatusFaults = append([]entity.FaultOnCall(nil), trigger.Outcome.Build.StatusFaults...)
		triggers[i] = trigger
	}
	behavior.BuildRunner.Triggers = triggers
	behavior.MergeConflictCheck.Invocations = append([]entity.Invocation[entity.MergeConflictCheckOutcome](nil), behavior.MergeConflictCheck.Invocations...)
	behavior.Merge.Invocations = append([]entity.Invocation[entity.MergeOutcome](nil), behavior.Merge.Invocations...)
	return behavior
}
