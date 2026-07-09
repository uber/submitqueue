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

// Package chain provides an enumerator.Enumerator that enumerates a batch
// as exactly one path, built directly on top of the full ordered chain of
// its active dependencies. It is the single-chain baseline; a multi-path
// enumerator (e.g. one adding build-alone fallback paths) can replace or
// supplement it without changing the contract.
package chain

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/enumerator"
)

// chainEnumerator is an enumerator.Enumerator that always enumerates a single
// path per batch.
type chainEnumerator struct{}

// New returns an enumerator.Enumerator that enumerates exactly one path per
// batch: the path's Base is deps in the given order and its Head is the
// batch itself.
func New() enumerator.Enumerator {
	return chainEnumerator{}
}

// Enumerate returns exactly one path whose Base preserves deps' order and
// whose Head is the batch — including when deps is empty, in which case the
// single path has an empty Base (the batch builds directly on the target
// branch). The controller owns everything beyond structure: it assigns the
// path its identity, stamps its status, and has the scorer fill its score.
func (chainEnumerator) Enumerate(_ context.Context, batch entity.Batch, deps []entity.Batch) ([]entity.SpeculationPath, error) {
	var base []string
	for _, dep := range deps {
		base = append(base, dep.ID)
	}
	return []entity.SpeculationPath{{Base: base, Head: batch.ID}}, nil
}
