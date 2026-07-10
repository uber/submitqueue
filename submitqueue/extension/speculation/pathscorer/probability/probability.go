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

// Package probability provides a pathscorer.Scorer that scores each path as
// the probability that exactly that path's assumption holds: every base
// dependency lands and every non-base dependency of the head does not. Each
// dependency's probability is resolved from its batch's state — certainty
// (1 or 0) once the batch is terminal, its predicted build-success score
// while it is in flight — so a resolved dependency automatically kills the
// paths that bet against the outcome and boosts the paths consistent with
// it. Dependency outcomes are treated as independent; there is no adjustment
// for wait time, historical pass rate, or correlation between outcomes.
//
// # Worked example
//
// Batch C has active dependencies A and B, with predicted success
// probabilities p(C) = 0.8, p(A) = 0.9, p(B) = 0.6, and a three-path tree:
//
//	path    base    formula                      score
//	chain   [A B]   0.8 * 0.9 * 0.6              0.432
//	drop-B  [A]     0.8 * 0.9 * (1 - 0.6)        0.288
//	alone   []      0.8 * (1 - 0.9) * (1 - 0.6)  0.032
//
// The chain path leads while everything looks healthy. When B's build fails,
// p(B) resolves to 0:
//
//	path    base    formula                      score
//	chain   [A B]   0.8 * 0.9 * 0                0
//	drop-B  [A]     0.8 * 0.9 * 1                0.72
//	alone   []      0.8 * 0.1 * 1                0.08
//
// The chain path — a bet on B landing — dies, and the drop-B path inherits
// its score mass. If A then lands, p(A) resolves to 1: drop-B rises to 0.8
// and alone falls to 0. The selector and prioritizer rank on these scores,
// so builds follow the paths still consistent with reality.
package probability

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/pathscorer"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// probabilityScorer computes each path's Score as the probability that
// exactly its assumption holds: every base dependency lands and every other
// active dependency of the head fails to land — so this exact path, and no
// sibling, is the one that materializes. Batch probabilities come from
// batchProbability, which prefers a terminal outcome over the prediction.
// Folding resolved outcomes into path Status (dead-pathing, cancellation)
// remains the controller's reconcile concern; this scorer only recomputes
// Score.
type probabilityScorer struct {
	// batches resolves a batch ID to its current state and predicted-success
	// probability (entity.Batch.State / entity.Batch.Score) and, for the
	// tree's head batch, its full active-dependency set
	// (entity.Batch.Dependencies).
	batches storage.BatchStore
}

// New returns a pathscorer.Scorer implementing the path-probability
// heuristic, reading batch states and success probabilities from batches.
func New(batches storage.BatchStore) pathscorer.Scorer {
	return &probabilityScorer{batches: batches}
}

// batchProbability resolves the probability that b lands. A terminal state
// is certainty — 1 for succeeded, 0 for failed or cancelled — and overrides
// any prediction. Otherwise it is b.Score, the predicted build-success
// probability: the score stage sets it before a batch ever reaches
// speculation, so every batch the scorer sees carries a real prediction.
// Cancelling is deliberately not treated as terminal: cancellation is
// best-effort and the batch may still succeed, so the prediction stands
// until the state settles.
func batchProbability(b entity.Batch) float64 {
	switch b.State {
	case entity.BatchStateSucceeded:
		return 1
	case entity.BatchStateFailed, entity.BatchStateCancelled:
		return 0
	}
	return b.Score
}

// Score returns one PathScore per path in tree, regardless of Status, each
// naming its path by ID. A path's score is:
//
//	p(head) * Π p(d) for d in path.Base * Π (1 - p(d)) for d in head.Dependencies but not in path.Base
//
// where p(b) is 1 when b has succeeded, 0 when b has failed or been
// cancelled, and otherwise b.Score — see batchProbability. Each batch
// referenced by the tree is loaded from the store at most once. Any store
// error (including not-found) is returned wrapped, unclassified — extensions
// never classify errors as user or infra.
func (s *probabilityScorer) Score(ctx context.Context, tree entity.SpeculationTree) ([]entity.PathScore, error) {
	if len(tree.Paths) == 0 {
		return nil, nil
	}

	head, err := s.batches.Get(ctx, tree.BatchID)
	if err != nil {
		return nil, fmt.Errorf("probability: get head batch %q: %w", tree.BatchID, err)
	}

	probabilities := map[string]float64{}
	probabilityOf := func(batchID string) (float64, error) {
		if p, ok := probabilities[batchID]; ok {
			return p, nil
		}
		b, err := s.batches.Get(ctx, batchID)
		if err != nil {
			return 0, fmt.Errorf("probability: get dependency batch %q: %w", batchID, err)
		}
		p := batchProbability(b)
		probabilities[batchID] = p
		return p, nil
	}

	scores := make([]entity.PathScore, len(tree.Paths))
	for i, path := range tree.Paths {
		inBase := make(map[string]bool, len(path.Path.Base))
		score := batchProbability(head)
		for _, dep := range path.Path.Base {
			inBase[dep] = true
			p, err := probabilityOf(dep)
			if err != nil {
				return nil, err
			}
			score *= p
		}
		for _, dep := range head.Dependencies {
			if inBase[dep] {
				continue
			}
			p, err := probabilityOf(dep)
			if err != nil {
				return nil, err
			}
			score *= 1 - p
		}
		scores[i] = entity.PathScore{PathID: path.ID, Score: float32(score)}
	}

	return scores, nil
}
