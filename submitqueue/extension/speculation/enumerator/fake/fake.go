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
// and examples. Seed the paths returned for a batch with Set, keyed by batch ID;
// an unseeded batch enumerates to no paths. FailWith injects an error on every
// call to exercise the error path. It is intended for examples and tests only,
// never production.
package fake

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/enumerator"
)

// Enumerator is a programmable in-memory enumerator.Enumerator.
type Enumerator struct {
	paths map[string][]entity.SpeculationPath
	err   error
}

// New returns an empty fake Enumerator. Seed it with Set.
func New() *Enumerator {
	return &Enumerator{paths: map[string][]entity.SpeculationPath{}}
}

// Set seeds the paths returned by Enumerate for the given batch ID.
func (e *Enumerator) Set(batchID string, paths []entity.SpeculationPath) *Enumerator {
	e.paths[batchID] = paths
	return e
}

// FailWith makes every Enumerate call return err.
func (e *Enumerator) FailWith(err error) *Enumerator {
	e.err = err
	return e
}

// Enumerate returns the seeded paths for the batch. An unseeded batch returns
// no paths. The deps argument is ignored.
func (e *Enumerator) Enumerate(_ context.Context, batch entity.Batch, _ []entity.Batch) ([]entity.SpeculationPath, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.paths[batch.ID], nil
}

// ensure the fake satisfies the interface.
var _ enumerator.Enumerator = (*Enumerator)(nil)
