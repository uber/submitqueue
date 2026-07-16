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

// ScenarioBuilder constructs an immutable Scenario.
type ScenarioBuilder struct {
	timeout time.Duration
	lands   []*LandBuilder
}

// NewScenario returns an empty scenario builder.
func NewScenario() *ScenarioBuilder {
	return &ScenarioBuilder{}
}

// Timeout sets the maximum wall-clock duration of the run.
func (b *ScenarioBuilder) Timeout(timeout time.Duration) *ScenarioBuilder {
	b.timeout = timeout
	return b
}

// Land appends requests in declaration order.
func (b *ScenarioBuilder) Land(lands ...*LandBuilder) *ScenarioBuilder {
	b.lands = append(b.lands, lands...)
	return b
}

// Build validates and returns an immutable Scenario.
func (b *ScenarioBuilder) Build() (Scenario, error) {
	if b == nil {
		return Scenario{}, fmt.Errorf("scenario builder is nil")
	}

	lands := make([]entity.Land, 0, len(b.lands))
	for i, landBuilder := range b.lands {
		if landBuilder == nil {
			return Scenario{}, fmt.Errorf("land %d is nil", i)
		}
		land, err := landBuilder.build()
		if err != nil {
			return Scenario{}, fmt.Errorf("land %d: %w", i, err)
		}
		lands = append(lands, cloneLand(land))
	}

	scenario := entity.Scenario{
		TimeoutMs: b.timeout.Milliseconds(),
		Lands:     lands,
	}
	if err := Validate(scenario); err != nil {
		return Scenario{}, err
	}
	return cloneScenario(scenario), nil
}

func cloneScenario(scenario entity.Scenario) entity.Scenario {
	lands := make([]entity.Land, len(scenario.Lands))
	for i, land := range scenario.Lands {
		lands[i] = cloneLand(land)
	}
	scenario.Lands = lands
	return scenario
}
