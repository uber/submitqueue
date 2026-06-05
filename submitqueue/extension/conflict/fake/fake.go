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
// error when a candidate change URI carries a failure marker of the form
// "sq-fake=<token>":
//
//	sq-fake=conflict-error -> non-nil error (the delegate is not called)
//
// Because the analyzer now receives the candidate's changes (entity.BatchChanges
// with URIs), the same URI-marker convention used by the other fakes works here —
// a land request can drive the conflict-analysis error path end-to-end. It is
// intended for examples and tests only, never production.
package fake

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/core/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
)

// Recognized marker token. See the package doc for the convention.
const tokenError = "conflict-error"

// analyzerFake decorates a delegate Analyzer, injecting an error when a
// candidate change URI carries the failure marker.
type analyzerFake struct {
	delegate conflict.Analyzer
}

// New returns a conflict.Analyzer that delegates to the given analyzer but
// returns an error when a candidate change URI carries the
// "sq-fake=conflict-error" marker. The delegate is the existing analyzer
// implementation to wrap (e.g. all or none).
func New(delegate conflict.Analyzer) conflict.Analyzer {
	return analyzerFake{delegate: delegate}
}

// Analyze returns an error when a candidate change URI carries the failure
// marker; otherwise it delegates to the wrapped analyzer.
func (a analyzerFake) Analyze(ctx context.Context, candidate entity.BatchChanges, inFlight []entity.BatchChanges) ([]conflict.Conflict, error) {
	if markerToken(candidate) == tokenError {
		return nil, fmt.Errorf("fake: marked conflict-analysis error for batch %q", candidate.BatchID)
	}
	return a.delegate.Analyze(ctx, candidate, inFlight)
}

// markerToken returns the marker token embedded in the first candidate change
// URI that carries one, or "" if none do.
func markerToken(changes entity.BatchChanges) string {
	uris := make([]string, 0, len(changes.Changes))
	for _, c := range changes.Changes {
		uris = append(uris, c.URI)
	}
	return fakemarker.Token(uris)
}
