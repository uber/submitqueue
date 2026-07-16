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

// MergeConflictCheckBehaviorBuilder constructs CheckMergeability behavior.
type MergeConflictCheckBehaviorBuilder struct {
	invocations []entity.Invocation[entity.MergeConflictCheckOutcome]
}

// NewMergeConflictCheckBehavior returns an empty behavior builder.
func NewMergeConflictCheckBehavior() *MergeConflictCheckBehaviorBuilder {
	return &MergeConflictCheckBehaviorBuilder{}
}

// Invoke appends CheckMergeability invocations.
func (b *MergeConflictCheckBehaviorBuilder) Invoke(invocations ...entity.Invocation[entity.MergeConflictCheckOutcome]) *MergeConflictCheckBehaviorBuilder {
	b.invocations = append(b.invocations, invocations...)
	return b
}

func (b *MergeConflictCheckBehaviorBuilder) build() (entity.MergeConflictCheckBehavior, error) {
	return entity.MergeConflictCheckBehavior{
		Invocations: append([]entity.Invocation[entity.MergeConflictCheckOutcome](nil), b.invocations...),
	}, nil
}

// SuccessfulMergeConflictCheck returns an immediately mergeable check.
func SuccessfulMergeConflictCheck() *MergeConflictCheckBehaviorBuilder {
	return NewMergeConflictCheckBehavior().Invoke(MergeConflictCheckSucceeded())
}

// ConflictingMergeConflictCheck returns an immediate terminal conflict.
func ConflictingMergeConflictCheck() *MergeConflictCheckBehaviorBuilder {
	return NewMergeConflictCheckBehavior().Invoke(MergeConflictCheckConflicted())
}

// MergeConflictCheckSucceeded returns an immediately mergeable invocation.
func MergeConflictCheckSucceeded() entity.Invocation[entity.MergeConflictCheckOutcome] {
	return entity.Invocation[entity.MergeConflictCheckOutcome]{Outcome: entity.Mergeable}
}

// MergeConflictCheckConflicted returns an immediate terminal-conflict invocation.
func MergeConflictCheckConflicted() entity.Invocation[entity.MergeConflictCheckOutcome] {
	return entity.Invocation[entity.MergeConflictCheckOutcome]{Outcome: entity.MergeConflict}
}

// MergeConflictCheckFault returns a failed CheckMergeability invocation.
func MergeConflictCheckFault(fault Fault) entity.Invocation[entity.MergeConflictCheckOutcome] {
	return entity.Invocation[entity.MergeConflictCheckOutcome]{Fault: fault}
}

// MergeInvocationBuilder constructs one Merge invocation.
type MergeInvocationBuilder struct {
	invocation entity.Invocation[entity.MergeOutcome]
}

// Fault applies a fault to the invocation.
func (b *MergeInvocationBuilder) Fault(fault Fault) *MergeInvocationBuilder {
	b.invocation.Fault = fault
	return b
}

// MergeBehaviorBuilder constructs committing Merge behavior.
type MergeBehaviorBuilder struct {
	invocations []entity.Invocation[entity.MergeOutcome]
}

// NewMergeBehavior returns an empty Merge behavior builder.
func NewMergeBehavior() *MergeBehaviorBuilder {
	return &MergeBehaviorBuilder{}
}

// Invoke appends committing Merge invocations.
func (b *MergeBehaviorBuilder) Invoke(invocations ...*MergeInvocationBuilder) *MergeBehaviorBuilder {
	for _, invocation := range invocations {
		if invocation == nil {
			b.invocations = append(b.invocations, entity.Invocation[entity.MergeOutcome]{})
			continue
		}
		b.invocations = append(b.invocations, invocation.invocation)
	}
	return b
}

func (b *MergeBehaviorBuilder) build() (entity.MergeBehavior, error) {
	return entity.MergeBehavior{
		Invocations: append([]entity.Invocation[entity.MergeOutcome](nil), b.invocations...),
	}, nil
}

// MergeSucceededAfter returns a successful Merge invocation with provider latency.
func MergeSucceededAfter(delay time.Duration) *MergeInvocationBuilder {
	return &MergeInvocationBuilder{
		invocation: entity.Invocation[entity.MergeOutcome]{
			DelayMs: delay.Milliseconds(),
			Outcome: entity.MergeOutcome{Result: entity.MergeSucceeded},
		},
	}
}

// SuccessfulMerge returns an immediately successful committing merge.
func SuccessfulMerge() *MergeBehaviorBuilder {
	return NewMergeBehavior().Invoke(MergeSucceededAfter(0))
}
