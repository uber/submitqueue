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

// Package fake provides a programmable limit.PrioritizationLimit for tests and
// examples. New sets the value returned by Limit; FailWith injects an error on
// every call. It is intended for examples and tests only, never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/extension/prioritization/limit"
)

// PrioritizationLimit is a programmable limit.PrioritizationLimit.
type PrioritizationLimit struct {
	limit int
	err   error
}

// New returns a fake PrioritizationLimit whose Limit returns the given value.
func New(value int) *PrioritizationLimit {
	return &PrioritizationLimit{limit: value}
}

// FailWith makes every Limit call return err.
func (l *PrioritizationLimit) FailWith(err error) *PrioritizationLimit {
	l.err = err
	return l
}

// Limit returns the configured value, or the injected error if FailWith was set.
func (l *PrioritizationLimit) Limit(_ context.Context) (int, error) {
	if l.err != nil {
		return 0, l.err
	}
	return l.limit, nil
}

// ensure the fake satisfies the interface.
var _ limit.PrioritizationLimit = (*PrioritizationLimit)(nil)
