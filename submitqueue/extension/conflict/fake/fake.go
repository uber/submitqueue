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

// Package fake provides a conflict.Analyzer that decorates an existing analyzer:
// it delegates to the wrapped implementation for the happy path, but injects an
// error when a caller-supplied predicate matches.
//
// Unlike the change-facing fakes, Analyze operates on batches — it never sees
// change URIs — so error injection is predicate-driven rather than marker-driven.
// To exercise the analyzer's error path in e2e, route a queue to an analyzer
// built with a failing predicate (e.g. FailAlways) via the queue wiring. It is
// intended for examples and tests only, never production.
package fake

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
)

// FailOn decides whether Analyze should inject an error for the given inputs.
type FailOn func(batch entity.Batch, inFlight []entity.Batch) bool

// FailAlways is a FailOn that injects an error on every Analyze call.
func FailAlways(entity.Batch, []entity.Batch) bool { return true }

// analyzerFake decorates a delegate Analyzer, injecting an error when failOn
// reports true.
type analyzerFake struct {
	delegate conflict.Analyzer
	failOn   FailOn
}

// New returns a conflict.Analyzer that delegates to the given analyzer but
// returns an error when failOn reports true for the call's inputs. The delegate
// is the existing analyzer implementation to wrap (e.g. all or none). A nil
// failOn never injects an error (pure passthrough).
func New(delegate conflict.Analyzer, failOn FailOn) conflict.Analyzer {
	return analyzerFake{delegate: delegate, failOn: failOn}
}

// Analyze returns an error when failOn reports true; otherwise it delegates to
// the wrapped analyzer.
func (a analyzerFake) Analyze(ctx context.Context, batch entity.Batch, inFlight []entity.Batch) ([]conflict.Conflict, error) {
	if a.failOn != nil && a.failOn(batch, inFlight) {
		return nil, fmt.Errorf("fake: injected analyze error for batch %q", batch.ID)
	}
	return a.delegate.Analyze(ctx, batch, inFlight)
}
