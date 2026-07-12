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

package pathscorer

//go:generate mockgen -source=pathscorer.go -destination=mock/pathscorer_mock.go -package=mock

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
// The controller hands the scorer the batch's speculation tree directly — the
// subject it scores. Any richer signal an implementation needs (the dependency
// batches' scores, historical pass rates) is injected at its Factory, not passed
// here. It never writes: it returns per-path scores and the controller merges
// them into the tree and persists, keeping the controller the single writer of
// tree state.
type Scorer interface {
	// Score returns the freshly computed predicted-success score of each path in
	// the tree as entity.PathScore values, keyed by path ID
	// (entity.SpeculationPathInfo.ID). Paths omitted from the result keep their
	// last persisted score. The combination formula is the implementation's
	// concern; the returned values must be probabilities in [0, 1] — the
	// controller enforces the range when it consumes them.
	Score(ctx context.Context, tree entity.SpeculationTree) ([]entity.PathScore, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (scoring knobs, and
// read access to any extra signals such as the dependency batches' scores) is
// injected at construction by the integrator.
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
