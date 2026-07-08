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

// Package fake provides a programmable prioritizer.Prioritizer for tests and
// examples. It returns the decisions seeded via SetDecisions (none by default,
// i.e. leave every candidate as-is). FailWith injects an error on every call. It
// is intended for examples and tests only, never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/prioritizer"
)

// Prioritizer is a programmable prioritizer.Prioritizer.
type Prioritizer struct {
	decisions []entity.PathDecision
	err       error
}

// New returns a fake Prioritizer that decides nothing (leaves every candidate
// as-is). Seed decisions with SetDecisions.
func New() *Prioritizer {
	return &Prioritizer{}
}

// SetDecisions seeds the decisions returned by Prioritize, in the order given.
func (p *Prioritizer) SetDecisions(decisions ...entity.PathDecision) *Prioritizer {
	p.decisions = decisions
	return p
}

// FailWith makes every Prioritize call return err.
func (p *Prioritizer) FailWith(err error) *Prioritizer {
	p.err = err
	return p
}

// Prioritize returns the seeded decisions. The candidates argument is ignored.
func (p *Prioritizer) Prioritize(_ context.Context, _ []entity.SpeculationPathInfo) ([]entity.PathDecision, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.decisions, nil
}

// ensure the fake satisfies the interface.
var _ prioritizer.Prioritizer = (*Prioritizer)(nil)
