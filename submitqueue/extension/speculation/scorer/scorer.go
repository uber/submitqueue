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

package scorer

//go:generate mockgen -source=scorer.go -destination=mock/scorer_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// Scorer computes the predicted-success score of every path in a batch's
// speculation tree.
//
// A path's score is a prediction — "how likely is this bet to pay off?" — and
// predictions must move as evidence arrives. The scorer answers "how good is
// each path right now" from the current state: the per-batch success
// probabilities of a path's base batches (entity.Batch.Score, set by the score
// stage), which of those dependencies have already landed or had their build
// pass (resolved assumptions raise confidence), and optionally other signals
// (how long the batch has waited, historical pass rates).
//
// The controller re-runs the scorer on every respeculate, right after it
// reconciles path status — so when a dependency lands, its build passes, or a
// sibling path fails, the surviving paths' scores are recomputed against the new
// reality before anything is selected or prioritized. The controller drives
// *when* to rescore and persists the result; the scorer owns the *formula*.
//
// This is the per-*path* scorer, distinct from the per-*batch* score stage
// (extension/scorer), which sets entity.Batch.Score. The path scorer consumes
// those batch scores to score whole paths.
//
// The store is the source of truth. The scorer is handed only the batch identity
// and loads what it needs from storage — the batch's speculation tree (already
// carrying the controller-reconciled statuses) and the dependency batches
// (carrying their Batch.Score and current state) — through read access injected
// at its Factory. It never writes: it returns the scored tree and the controller
// persists the scores, keeping the controller the single writer of tree state.
type Scorer interface {
	// Score loads the batch's speculation tree and dependency batches from
	// storage and returns the tree with each path's Score set to its freshly
	// computed predicted-success value. Path structure and controller-stamped
	// Status are carried through unchanged; only Score is (re)computed. The
	// combination formula is the implementation's concern.
	Score(ctx context.Context, batch entity.Batch) (entity.SpeculationTree, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (read access to the
// tree and batch stores, scoring knobs, extra signals) is injected at
// construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Scorer serves.
	QueueName string
}

// Factory builds the Scorer for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Scorer for the given queue.
	For(cfg Config) (Scorer, error)
}
