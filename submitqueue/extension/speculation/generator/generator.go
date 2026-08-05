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

// Package generator defines the candidate-stream composition point inside the
// standard Speculator. A Generator turns one snapshot of a queue's batches into
// a pull-based stream of candidate speculation paths, ranked by a policy of its
// choosing. It is not a controller-facing extension: the speculate controller
// depends only on the Speculator contract, and an alternate Speculator need not
// use or expose this seam.
package generator

//go:generate mockgen -source=generator.go -destination=mock/generator_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Generator produces the queue's candidate paths as a pull-based stream over
// one snapshot of the queue's batches. Which candidates come first, and what
// their ranking scores mean, is the implementation's own policy.
type Generator interface {
	// Open starts the candidate stream. The consumer pulls candidates lazily
	// from the returned iterator. A cancelled or expired ctx aborts with its
	// error and no iterator.
	//
	// batches is treated as a snapshot: a generator may hold it, and the
	// dependency slices within it, for as long as the iterator lives rather
	// than copying them. Callers must not write to either while pulling. A
	// queue that has moved on is a new snapshot and a new Open, which is how
	// batches are revised anyway — they are replaced, not edited in place.
	Open(ctx context.Context, batches []entity.Batch) (PathIterator, error)
}

// PathIterator is a pull-based stream of candidate paths. Beyond what ranking
// requires up front, the producer computes only what is pulled.
type PathIterator interface {
	// Next yields the next candidate in the generator's order. ok is false once
	// the generator has no more candidates to offer. Candidates never repeat and
	// never contradict a resolved fact — a dependency that already succeeded is
	// never assumed to fail, and one that already failed or was cancelled is
	// never assumed to succeed.
	//
	// A cancelled or expired ctx ends the stream with its error, ok false, and
	// no candidate.
	Next(ctx context.Context) (c entity.CandidatePath, ok bool, err error)
}
