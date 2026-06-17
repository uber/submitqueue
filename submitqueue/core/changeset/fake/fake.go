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

// Package fake provides an in-memory changeset.Resolver for tests and examples.
// Seed per-batch results with Set (raw changes) and SetDetailed (detailed view),
// keyed by batch ID; the resolver serves what was seeded. A batch with no seeded
// entry resolves to empty rather than an error, matching a batch whose requests
// carry no changes. FailWith injects an error on every call to exercise the error
// path without a real store. It is intended for examples and tests only, never
// production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// Resolver is a programmable in-memory changeset.Resolver.
type Resolver struct {
	changes  map[string][]change.Change
	detailed map[string]entity.BatchChanges
	err      error
}

// New returns an empty fake Resolver. Seed it with Set / SetDetailed.
func New() *Resolver {
	return &Resolver{
		changes:  map[string][]change.Change{},
		detailed: map[string]entity.BatchChanges{},
	}
}

// Set seeds the raw changes returned by Changes for the given batch ID.
func (r *Resolver) Set(batchID string, changes ...change.Change) *Resolver {
	r.changes[batchID] = changes
	return r
}

// SetDetailed seeds the detailed view returned by Detailed for the given batch ID.
func (r *Resolver) SetDetailed(batchID string, detailed entity.BatchChanges) *Resolver {
	r.detailed[batchID] = detailed
	return r
}

// FailWith makes every Changes and Detailed call return err.
func (r *Resolver) FailWith(err error) *Resolver {
	r.err = err
	return r
}

// ChangesForBatch returns the seeded raw changes for the batch, in seeded order.
// An unseeded batch resolves to a nil slice.
func (r *Resolver) ChangesForBatch(_ context.Context, batch entity.Batch) ([]change.Change, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.changes[batch.ID], nil
}

// DetailedForBatch returns the seeded detailed view for the batch. An unseeded
// batch resolves to an empty entity.BatchChanges carrying the batch's identity.
func (r *Resolver) DetailedForBatch(_ context.Context, batch entity.Batch) (entity.BatchChanges, error) {
	if r.err != nil {
		return entity.BatchChanges{}, r.err
	}
	if detailed, ok := r.detailed[batch.ID]; ok {
		return detailed, nil
	}
	return entity.BatchChanges{BatchID: batch.ID, Queue: batch.Queue}, nil
}

// ensure the fake satisfies the interface.
var _ changeset.Resolver = (*Resolver)(nil)
