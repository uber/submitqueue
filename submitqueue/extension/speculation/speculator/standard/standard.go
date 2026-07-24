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

// Package standard provides the standard speculator.Speculator: it composes a
// Generator and an Allocator so a queue's candidate scoring and its budget
// rationing policy can vary independently without changing the Speculator. The
// Speculator itself decides nothing — it opens the Generator's candidate stream
// and hands it to the Allocator; all of its behavior comes from those two parts.
package standard

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/allocator"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
)

// spec is a speculator.Speculator that opens the generator's candidate stream
// and hands it to the allocator to spend the build budget.
type spec struct {
	gen   generator.Generator
	alloc allocator.Allocator
}

// New returns the standard speculator.Speculator, composing a Generator and an
// Allocator.
func New(gen generator.Generator, alloc allocator.Allocator) speculator.Speculator {
	return spec{gen: gen, alloc: alloc}
}

// Speculate opens the generator over the queue's batches, then lets the
// allocator spend the budget over the resulting candidate iterator, reconciling
// it against the path sets.
func (s spec) Speculate(ctx context.Context, batches []entity.Batch, pathSets []entity.SpeculationPathSet) ([]entity.Speculation, error) {
	iter, err := s.gen.Generate(ctx, batches)
	if err != nil {
		return nil, err
	}
	return s.alloc.Allocate(ctx, pathSets, iter)
}
