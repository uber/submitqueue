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

// Package fake provides a scorer.Scorer that decorates an existing scorer: it
// delegates to the wrapped implementation for the happy path, but injects an
// error when a change URI carries a failure marker of the form "sq-fake=<token>":
//
//	sq-fake=score-error -> non-nil error (the delegate is not called)
//
// This lets tests exercise scorer error paths end-to-end (driven from a land
// request) while preserving real scoring behavior for unmarked changes. It is
// intended for examples and tests only, never production.
package fake

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/core/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
)

// Recognized marker token. See the package doc for the convention.
const tokenError = "score-error"

// scorerFake decorates a delegate Scorer, injecting an error when a change URI
// carries the failure marker. It resolves the batch itself to inspect URIs.
type scorerFake struct {
	resolver changeset.Resolver
	delegate scorer.Scorer
}

// New returns a scorer.Scorer that delegates to the given scorer but returns an
// error when a change URI carries the "sq-fake=score-error" marker. The resolver
// resolves the batch's changes so the marker can be inspected; the delegate is the
// existing scorer implementation to wrap (e.g. heuristic or composite).
func New(resolver changeset.Resolver, delegate scorer.Scorer) scorer.Scorer {
	return scorerFake{resolver: resolver, delegate: delegate}
}

// Score returns an error when a change URI carries the failure marker; otherwise
// it delegates to the wrapped scorer.
func (s scorerFake) Score(ctx context.Context, batch entity.Batch) (float64, error) {
	changes, err := s.resolver.DetailedForBatch(ctx, batch)
	if err != nil {
		return 0, err
	}
	if markerToken(changes) == tokenError {
		return 0, fmt.Errorf("fake: marked score error")
	}
	return s.delegate.Score(ctx, batch)
}

// markerToken returns the marker token embedded in the first change URI that
// carries one, or "" if none do.
func markerToken(changes entity.BatchChanges) string {
	uris := make([]string, 0, len(changes.Changes))
	for _, c := range changes.Changes {
		uris = append(uris, c.URI)
	}
	return fakemarker.Token(uris)
}
