// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package generator defines the candidate-path stream used by the default
// speculation implementation.
package generator

//go:generate mockgen -source=generator.go -destination=mock/generator_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Generator opens a best-first stream of coherent speculation candidates over
// one queue snapshot.
type Generator interface {
	// Generate creates an Iterator over candidates for batches in
	// BatchStateSpeculating. The input must include every batch referenced by a
	// candidate head's Dependencies field, including terminal dependencies whose
	// outcomes constrain the generated assumptions.
	//
	// The returned Iterator owns its state and is not safe for concurrent use.
	// An empty snapshot or a snapshot without Speculating heads returns an empty
	// Iterator rather than an error.
	Generate(ctx context.Context, batches []entity.Batch) (Iterator, error)
}

// Iterator is a pull-based, probability-ordered stream of candidate paths.
type Iterator interface {
	// Next returns the next candidate. ok is false when the stream is exhausted;
	// exhaustion is an expected result, not an error. Implementations must leave
	// the stream unconsumed when ctx is already cancelled.
	Next(ctx context.Context) (candidate entity.CandidatePath, ok bool, err error)
}
