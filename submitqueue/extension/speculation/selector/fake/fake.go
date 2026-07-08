// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fake provides a programmable selector.Selector for tests and examples.
// It returns the decisions seeded via SetDecisions (none by default, i.e. leave
// every path as-is). FailWith injects an error on every call. It is intended for
// examples and tests only, never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/selector"
)

// Selector is a programmable selector.Selector.
type Selector struct {
	decisions []entity.SpeculationPathDecision
	err       error
}

// New returns a fake Selector that decides nothing (leaves every path as-is).
// Seed decisions with SetDecisions.
func New() *Selector {
	return &Selector{}
}

// SetDecisions seeds the decisions returned by Select.
func (s *Selector) SetDecisions(decisions ...entity.SpeculationPathDecision) *Selector {
	s.decisions = decisions
	return s
}

// FailWith makes every Select call return err.
func (s *Selector) FailWith(err error) *Selector {
	s.err = err
	return s
}

// Select returns the seeded decisions. The batch argument is ignored.
func (s *Selector) Select(_ context.Context, _ entity.Batch) ([]entity.SpeculationPathDecision, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.decisions, nil
}

// ensure the fake satisfies the interface.
var _ selector.Selector = (*Selector)(nil)
