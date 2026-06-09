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

// Package fake provides a programmable in-memory enumerator.Enumerator for tests
// and examples. Seed the tree returned for a batch with Set, keyed by batch ID;
// an unseeded batch enumerates to an empty tree carrying the batch's identity.
// FailWith injects an error on every call to exercise the error path. It is
// intended for examples and tests only, never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/enumerator"
)

// Enumerator is a programmable in-memory enumerator.Enumerator.
type Enumerator struct {
	trees map[string]entity.SpeculationTree
	err   error
}

// New returns an empty fake Enumerator. Seed it with Set.
func New() *Enumerator {
	return &Enumerator{trees: map[string]entity.SpeculationTree{}}
}

// Set seeds the tree returned by Enumerate for the given batch ID.
func (e *Enumerator) Set(batchID string, tree entity.SpeculationTree) *Enumerator {
	e.trees[batchID] = tree
	return e
}

// FailWith makes every Enumerate call return err.
func (e *Enumerator) FailWith(err error) *Enumerator {
	e.err = err
	return e
}

// Enumerate returns the seeded tree for the batch. An unseeded batch returns an
// empty tree carrying the batch's identity. The deps argument is ignored.
func (e *Enumerator) Enumerate(_ context.Context, batchID string, _ []entity.Batch) (entity.SpeculationTree, error) {
	if e.err != nil {
		return entity.SpeculationTree{}, e.err
	}
	if tree, ok := e.trees[batchID]; ok {
		return tree, nil
	}
	return entity.SpeculationTree{BatchID: batchID}, nil
}

// ensure the fake satisfies the interface.
var _ enumerator.Enumerator = (*Enumerator)(nil)
