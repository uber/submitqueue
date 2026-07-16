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
	"time"

	"github.com/uber/submitqueue/sqsim/entity"
)

// LandBuilder constructs one immutable Land.
type LandBuilder struct {
	name        string
	queue       string
	submitAfter time.Duration
	behavior    *BehaviorBuilder
	expectation entity.ExpectedRequestStatus
}

// NewLand returns a builder for a named Land.
func NewLand(name string) *LandBuilder {
	return &LandBuilder{name: name}
}

// Queue sets the SubmitQueue queue receiving the request.
func (b *LandBuilder) Queue(queue string) *LandBuilder {
	b.queue = queue
	return b
}

// SubmitAfter sets the delay from run start before submission.
func (b *LandBuilder) SubmitAfter(delay time.Duration) *LandBuilder {
	b.submitAfter = delay
	return b
}

// Behavior sets the external-system behavior for this Land.
func (b *LandBuilder) Behavior(behavior *BehaviorBuilder) *LandBuilder {
	b.behavior = behavior
	return b
}

// Expect sets the required public terminal status.
func (b *LandBuilder) Expect(status ExpectedRequestStatus) *LandBuilder {
	b.expectation = status
	return b
}

func (b *LandBuilder) build() (entity.Land, error) {
	if b == nil {
		return entity.Land{}, fmt.Errorf("land builder is nil")
	}
	if b.behavior == nil {
		return entity.Land{}, fmt.Errorf("behavior is required")
	}
	behavior, err := b.behavior.build()
	if err != nil {
		return entity.Land{}, fmt.Errorf("behavior: %w", err)
	}
	return entity.Land{
		Name:          b.name,
		Queue:         b.queue,
		SubmitAfterMs: b.submitAfter.Milliseconds(),
		Behavior:      behavior,
		Expectation:   entity.Expectation{Status: b.expectation},
	}, nil
}

func cloneLand(land entity.Land) entity.Land {
	land.Behavior = cloneBehavior(land.Behavior)
	return land
}
