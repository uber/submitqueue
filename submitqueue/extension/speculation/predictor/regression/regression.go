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

// Package regression revises a Scorer's price by multiplying its odds by one
// factor per piece of evidence about the batch's progress.
//
// Odds rather than the probability itself, because a factor then means the same
// thing wherever it applies and the result cannot leave [0, 1]. Written as logs
// and summed, the same arithmetic is a logistic regression, which is what lets
// hand-written factors later be replaced by fitted ones without changing the
// form. See doc/rfc/submitqueue/outcome-predictor.md.
package regression

import (
	"fmt"
	"math"

	"context"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/predictor"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/scorer"
)

// Factors are the odds multipliers, one per piece of evidence. A factor of 1
// leaves the price alone. Named fields rather than a keyed map, so an evidence
// name that does not exist fails to compile instead of being ignored.
type Factors struct {
	// PathPassed applies once when a build has passed on the batch's
	// all-succeed path.
	PathPassed float64
	// PathFailed applies once per failed path, compounding.
	PathFailed float64
	// Merging applies while the batch is merging.
	Merging float64
	// Cancelling applies while the batch is cancelling.
	Cancelling float64
}

// AllOnes is the neutral set: the prediction is the scorer's price.
func AllOnes() Factors {
	return Factors{PathPassed: 1, PathFailed: 1, Merging: 1, Cancelling: 1}
}

// epsilon bounds the price away from 0 and 1, which have no finite odds.
// Without it a certain scorer could never be revised by any evidence — and
// certainty about an unfinished batch is the scorer overstating what it sees.
const epsilon = 1e-6

// regression is a predictor.OutcomePredictor that revises a scorer's price.
type regression struct {
	// cfg is the per-queue identity this predictor was built for.
	cfg predictor.Config
	// base prices the batch's change; its price is what the factors revise.
	base scorer.Scorer
	// factors are the odds multipliers applied to that price.
	factors Factors
	// scope is the tally scope for emitting metrics.
	scope tally.Scope
}

// New creates a regression predictor bound to the queue named in cfg, revising
// base's price by factors.
// Panics if base is nil or any factor is not positive.
func New(cfg predictor.Config, base scorer.Scorer, factors Factors, scope tally.Scope) predictor.OutcomePredictor {
	if base == nil {
		panic("regression.New: base must not be nil")
	}
	for name, factor := range map[string]float64{
		"PathPassed": factors.PathPassed,
		"PathFailed": factors.PathFailed,
		"Merging":    factors.Merging,
		"Cancelling": factors.Cancelling,
	} {
		// Zero would pin the prediction to 0 and negative has no meaning as a
		// multiplier on odds. Configuration rejects both, so reaching here is a
		// wiring bug rather than an operator's mistake.
		if !(factor > 0) {
			panic(fmt.Sprintf("regression.New: factor %s must be positive, got %v", name, factor))
		}
	}
	return &regression{cfg: cfg, base: base, factors: factors, scope: scope}
}

// Predict prices the batch's change through the base scorer, then multiplies
// the odds of that price by one factor per piece of evidence.
func (r *regression) Predict(ctx context.Context, batch entity.Batch, paths entity.SpeculationPathSet) (ret predictor.Probability, retErr error) {
	op := metrics.Begin(r.scope, "predict", metrics.FastLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	price, err := r.base.Score(ctx, batch)
	if err != nil {
		return 0, err
	}
	// A price that is not a probability is a broken scorer, not a low opinion of
	// the batch. Saying so leaves the caller to fall back on its own default,
	// where clamping would hand back a number that looks deliberate.
	if !(price >= 0 && price <= 1) {
		return 0, fmt.Errorf("base scorer returned %v, which is not a probability", price)
	}

	odds := oddsOf(math.Min(math.Max(price, epsilon), 1-epsilon))
	if hasPassedAllSucceedPath(paths) {
		odds *= r.factors.PathPassed
	}
	odds *= math.Pow(r.factors.PathFailed, float64(countFailed(paths)))
	switch batch.State {
	case entity.BatchStateMerging:
		odds *= r.factors.Merging
	case entity.BatchStateCancelling:
		odds *= r.factors.Cancelling
	}
	return probabilityOf(odds), nil
}

// oddsOf converts a probability to odds. p is bounded away from 1, so this is
// finite.
func oddsOf(p float64) float64 {
	return p / (1 - p)
}

// probabilityOf converts odds back to a probability. Overflowed odds read as
// certainty rather than the NaN the division would produce.
func probabilityOf(odds float64) predictor.Probability {
	if math.IsInf(odds, 1) {
		return 1
	}
	return predictor.Probability(odds / (1 + odds))
}

// hasPassedAllSucceedPath reports a passed build on the batch's all-succeed
// path. Only that path counts: one built without a dependency's changes says
// nothing about a candidate that assumes the dependency lands.
func hasPassedAllSucceedPath(paths entity.SpeculationPathSet) bool {
	for _, entry := range paths.Paths {
		if entry.Status != entity.SpeculationPathStatusPassed {
			continue
		}
		if assumesAllSucceed(entry.Path) {
			return true
		}
	}
	return false
}

// assumesAllSucceed reports whether every dependency is assumed to succeed.
func assumesAllSucceed(path entity.SpeculationPath) bool {
	for _, dep := range path.Dependencies {
		if dep.Assumption != entity.DependencyAssumptionSucceeds {
			return false
		}
	}
	return true
}

// countFailed counts the batch's failed paths; each one compounds.
func countFailed(paths entity.SpeculationPathSet) int {
	failed := 0
	for _, entry := range paths.Paths {
		if entry.Status == entity.SpeculationPathStatusFailed {
			failed++
		}
	}
	return failed
}
