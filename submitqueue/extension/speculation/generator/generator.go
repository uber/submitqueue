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

// Package generator defines the candidate-stream composition point used by the
// standard Speculator. A Generator yields candidate paths through a pull
// iterator, in an order it chooses; it is not a controller-facing extension —
// an alternate Speculator need not use or expose this split.
package generator

//go:generate mockgen -source=generator.go -destination=mock/generator_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Generator produces the queue's candidate paths as a pull-based stream over the
// queue's live batches. The order candidates are yielded in, and how they are
// ranked, is the generator's own policy.
type Generator interface {
	// Generate starts the candidate stream. The consumer pulls candidates lazily
	// from the returned iterator. A cancelled or expired ctx aborts with its
	// error and no iterator.
	//
	// batches is treated as a snapshot: a generator may hold it, and the
	// dependency slices within it, for as long as the iterator lives rather
	// than copying them. Callers must not write to either while pulling. A
	// queue that has moved on is a new snapshot and a new Generate, which is how
	// batches are revised anyway — they are replaced, not edited in place.
	//
	// The snapshot must include every batch a head's direct dependencies
	// reference. A snapshot that breaks that — or carries empty or duplicate
	// batch IDs, or a head with an empty, duplicate, or self dependency — is
	// malformed input and aborts with an error rather than a stream.
	Generate(ctx context.Context, batches []entity.Batch) (Iterator, error)
}

// Iterator is a pull-based stream of candidate paths. Beyond what ranking
// requires up front, the producer computes only what is pulled.
type Iterator interface {
	// Next yields the next candidate in the generator's order. ok is false once
	// the generator has no more candidates to offer. Candidates never repeat and
	// never contradict a resolved fact — a dependency that already succeeded is
	// never assumed to fail, and one that already failed is never assumed to
	// succeed.
	//
	// A cancelled or expired ctx ends the stream with its error, ok false, and
	// no candidate.
	Next(ctx context.Context) (c entity.CandidatePath, ok bool, err error)
}
