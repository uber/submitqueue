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

// Package fake provides a pusher.Pusher whose outcome is driven by the input
// changes. With no marker every change is reported as committed with a synthetic
// commit SHA, behaving as a best-case stub for wiring and baselines. A failure
// can be injected end-to-end (e.g. from an e2e land request) by embedding a
// marker token in a change URI of the form "sq-fake=<token>":
//
//	sq-fake=conflict   -> pusher.ErrConflict
//	sq-fake=push-error -> non-nil error
//
// Both failure markers honor the atomicity contract: on error nothing is
// "pushed". This lets a single running stack exercise negative paths purely by
// varying request payloads. It is intended for examples and tests only, never
// production.
package fake

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/core/fakemarker"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
)

// Recognized marker tokens. See the package doc for the convention.
const (
	tokenConflict = "conflict"
	tokenError    = "push-error"
)

// fakePusher is a pusher.Pusher that reports every change as committed unless a
// marker token in a change URI requests a failure. The atomic counter hands out
// unique synthetic commit SHAs and makes the type safe for concurrent use. It
// resolves each batch's changes through the injected resolver.
type fakePusher struct {
	resolver changeset.Resolver
	counter  atomic.Uint64
}

// New returns a pusher.Pusher that defaults to committing every change and
// honors marker tokens embedded in change URIs. The resolver resolves each
// batch's changes.
func New(resolver changeset.Resolver) pusher.Pusher {
	return &fakePusher{resolver: resolver}
}

// Push resolves each batch's changes and reports every change as committed with
// a synthetic commit SHA, grouped per batch, unless a recognized marker token in
// one of the changes requests a failure.
func (p *fakePusher) Push(ctx context.Context, batches []entity.Batch) (pusher.Result, error) {
	perBatch := make([][]entity.Change, len(batches))
	var all []entity.Change
	for i, b := range batches {
		cs, err := p.resolver.ChangesForBatch(ctx, b)
		if err != nil {
			return pusher.Result{}, fmt.Errorf("fake: resolve batch %s: %w", b.ID, err)
		}
		perBatch[i] = cs
		all = append(all, cs...)
	}

	switch fakemarker.TokenInChanges(all) {
	case tokenConflict:
		return pusher.Result{}, pusher.ErrConflict
	case tokenError:
		return pusher.Result{}, fmt.Errorf("fake: marked push error")
	}

	result := make([]pusher.BatchOutcome, len(batches))
	for i, b := range batches {
		outcomes := make([]pusher.ChangeOutcome, 0, len(perBatch[i]))
		for _, change := range perBatch[i] {
			sha := fmt.Sprintf("fake-%d", p.counter.Add(1))
			outcomes = append(outcomes, pusher.ChangeOutcome{
				Change:     change,
				Status:     pusher.OutcomeStatusCommitted,
				CommitSHAs: []string{sha},
			})
		}
		result[i] = pusher.BatchOutcome{BatchID: b.ID, Outcomes: outcomes}
	}
	return pusher.Result{Batches: result}, nil
}
