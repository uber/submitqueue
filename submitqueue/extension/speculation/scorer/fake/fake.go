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

// Package fake provides a programmable scorer.Scorer for tests and examples.
// Seed the scored tree returned for a batch with Set, keyed by batch ID; an
// unseeded batch returns an empty tree carrying the batch's identity. FailWith
// injects an error on every call. It stands in for a real scorer's storage reads
// so tests need no store. It is intended for examples and tests only, never
// production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/scorer"
)

// Scorer is a programmable scorer.Scorer.
type Scorer struct {
	trees map[string]entity.SpeculationTree
	err   error
}

// New returns an empty fake Scorer. Seed it with Set.
func New() *Scorer {
	return &Scorer{trees: map[string]entity.SpeculationTree{}}
}

// Set seeds the scored tree returned by Score for the given batch ID.
func (s *Scorer) Set(batchID string, tree entity.SpeculationTree) *Scorer {
	s.trees[batchID] = tree
	return s
}

// FailWith makes every Score call return err.
func (s *Scorer) FailWith(err error) *Scorer {
	s.err = err
	return s
}

// Score returns the seeded tree for the batch. An unseeded batch returns an
// empty tree carrying the batch's identity.
func (s *Scorer) Score(_ context.Context, batch entity.Batch) (entity.SpeculationTree, error) {
	if s.err != nil {
		return entity.SpeculationTree{}, s.err
	}
	if tree, ok := s.trees[batch.ID]; ok {
		return tree, nil
	}
	return entity.SpeculationTree{BatchID: batch.ID}, nil
}

// ensure the fake satisfies the interface.
var _ scorer.Scorer = (*Scorer)(nil)
