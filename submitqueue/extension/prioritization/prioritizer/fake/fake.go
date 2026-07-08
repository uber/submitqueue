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
// examples. It returns the builds seeded via SetAdmitted (none by default).
// FailWith injects an error on every call. It stands in for a real prioritizer's
// storage reads so tests need no store. It is intended for examples and tests
// only, never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/prioritization/prioritizer"
)

// Prioritizer is a programmable prioritizer.Prioritizer.
type Prioritizer struct {
	admitted []entity.Build
	err      error
}

// New returns a fake Prioritizer that admits nothing. Seed the admitted builds
// with SetAdmitted.
func New() *Prioritizer {
	return &Prioritizer{}
}

// SetAdmitted seeds the builds returned by Prioritize, in the order given.
func (p *Prioritizer) SetAdmitted(builds ...entity.Build) *Prioritizer {
	p.admitted = builds
	return p
}

// FailWith makes every Prioritize call return err.
func (p *Prioritizer) FailWith(err error) *Prioritizer {
	p.err = err
	return p
}

// Prioritize returns the seeded admitted builds.
func (p *Prioritizer) Prioritize(_ context.Context) ([]entity.Build, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.admitted, nil
}

// ensure the fake satisfies the interface.
var _ prioritizer.Prioritizer = (*Prioritizer)(nil)
