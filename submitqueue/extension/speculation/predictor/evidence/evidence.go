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

// Package evidence revises a Scorer's price with factors for observed batch
// progress. See doc/rfc/submitqueue/outcome-predictor.md.
package evidence

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

// Factors revise the scorer's price, one per piece of evidence. A factor of 1
// leaves the price alone. Named fields make unknown evidence fail to compile.
type Factors struct {
	// PathPassed applies once when a build has passed on the batch's
	// all-succeed path.
	PathPassed float64
	// PathFailed applies once per failed all-succeed path, compounding.
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

// epsilon keeps exact certainty revisable while remaining close to the scorer.
const epsilon = 1e-6

// evidence is a predictor.Predictor that revises a scorer's price.
type evidence struct {
	// cfg is the per-queue identity this predictor was built for.
	cfg predictor.Config
	// base prices the batch's change; its price is what the factors revise.
	base scorer.Scorer
	// factors revise the scorer's price with observed evidence.
	factors Factors
	// scope is the tally scope for emitting metrics.
	scope tally.Scope
}

// New creates an evidence predictor bound to the queue named in cfg, revising
// base's price by factors.
//
// It rejects a nil base and non-positive factors.
func New(cfg predictor.Config, base scorer.Scorer, factors Factors, scope tally.Scope) (predictor.Predictor, error) {
	if base == nil {
		return nil, fmt.Errorf("evidence.New: base must not be nil")
	}
	for name, factor := range map[string]float64{
		"PathPassed": factors.PathPassed,
		"PathFailed": factors.PathFailed,
		"Merging":    factors.Merging,
		"Cancelling": factors.Cancelling,
	} {
		// Zero would permanently pin matching batches to 0; negatives cannot
		// represent either direction in the factor contract.
		if !(factor > 0) {
			return nil, fmt.Errorf("evidence.New: factor %s must be positive, got %v", name, factor)
		}
	}
	return &evidence{cfg: cfg, base: base, factors: factors, scope: scope}, nil
}

// Predict prices the batch's change, combines its evidence factors, and revises
// the scorer's price with the result.
func (r *evidence) Predict(ctx context.Context, batch entity.Batch, paths entity.SpeculationPathSet) (ret predictor.Probability, retErr error) {
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

	factor := math.Pow(r.factors.PathFailed, float64(countFailed(paths)))
	if hasPassedAllSucceedPath(paths) {
		factor *= r.factors.PathPassed
	}
	switch batch.State {
	case entity.BatchStateMerging:
		factor *= r.factors.Merging
	case entity.BatchStateCancelling:
		factor *= r.factors.Cancelling
	}
	return revise(math.Min(math.Max(price, epsilon), 1-epsilon), factor), nil
}

// revise applies the combined factor while keeping the result a probability.
func revise(price, factor float64) predictor.Probability {
	if math.IsInf(factor, 1) {
		return 1
	}
	return predictor.Probability(price * factor / (1 - price + price*factor))
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

// countFailed counts failed builds on the batch's all-succeed path; each one
// compounds. Flip-subset failures are ignored: they were built under different
// assumptions, the same filter PathPassed uses.
func countFailed(paths entity.SpeculationPathSet) int {
	failed := 0
	for _, entry := range paths.Paths {
		if entry.Status == entity.SpeculationPathStatusFailed && assumesAllSucceed(entry.Path) {
			failed++
		}
	}
	return failed
}
