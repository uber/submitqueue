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

package regression

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/predictor"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/scorer"
)

// testCfg is the per-queue identity used by every case in this file.
var testCfg = predictor.Config{QueueName: "test-queue"}

// fixedScorer always returns the same price.
type fixedScorer struct{ price float64 }

func (f fixedScorer) Score(_ context.Context, _ entity.Batch) (float64, error) {
	return f.price, nil
}

// errorScorer always fails.
type errorScorer struct{}

func (errorScorer) Score(_ context.Context, _ entity.Batch) (float64, error) {
	return 0, fmt.Errorf("scorer failed")
}

// pathSet builds a set whose entries carry the given statuses, every path
// assuming all of its dependencies succeed.
func pathSet(statuses ...entity.SpeculationPathStatus) entity.SpeculationPathSet {
	set := entity.SpeculationPathSet{Queue: "q", Head: "q/batch/1"}
	for i, status := range statuses {
		set.Paths = append(set.Paths, entity.SpeculationPathEntry{
			ID:     fmt.Sprintf("path-%d", i),
			Status: status,
			Path: entity.SpeculationPath{
				Head:         "q/batch/1",
				Dependencies: []entity.PathDependency{{Batch: "q/batch/0", Assumption: entity.DependencyAssumptionSucceeds}},
			},
		})
	}
	return set
}

// predict runs one prediction with all-neutral factors except those overridden.
func predict(t *testing.T, price float64, factors Factors, batch entity.Batch, paths entity.SpeculationPathSet) float64 {
	t.Helper()
	p := New(testCfg, fixedScorer{price: price}, factors, tally.NoopScope)
	got, err := p.Predict(context.Background(), batch, paths)
	require.NoError(t, err)
	return float64(got)
}

func TestPredict_NeutralFactorsReturnTheScorersPrice(t *testing.T) {
	for _, price := range []float64{0.01, 0.25, 0.5, 0.6, 0.9, 0.99} {
		t.Run(fmt.Sprintf("price %v", price), func(t *testing.T) {
			got := predict(t, price, AllOnes(), entity.Batch{}, pathSet(entity.SpeculationPathStatusPassed, entity.SpeculationPathStatusFailed))
			assert.InDelta(t, price, got, 1e-9)
		})
	}
}

func TestPredict_AppliesOneFactorPerEvidence(t *testing.T) {
	// 0.5 has odds of exactly 1, so the resulting odds are the factor itself and
	// the expected probability is factor/(1+factor).
	tests := []struct {
		name    string
		factors Factors
		batch   entity.Batch
		paths   entity.SpeculationPathSet
		want    float64
	}{
		{
			name:    "a passed path",
			factors: Factors{PathPassed: 9, PathFailed: 1, Merging: 1, Cancelling: 1},
			paths:   pathSet(entity.SpeculationPathStatusPassed),
			want:    0.9,
		},
		{
			name:    "no passed path leaves the price alone",
			factors: Factors{PathPassed: 9, PathFailed: 1, Merging: 1, Cancelling: 1},
			paths:   pathSet(entity.SpeculationPathStatusBuilding),
			want:    0.5,
		},
		{
			name:    "one failed path",
			factors: Factors{PathPassed: 1, PathFailed: 0.25, Merging: 1, Cancelling: 1},
			paths:   pathSet(entity.SpeculationPathStatusFailed),
			want:    0.2,
		},
		{
			name:    "failed paths compound",
			factors: Factors{PathPassed: 1, PathFailed: 0.5, Merging: 1, Cancelling: 1},
			paths:   pathSet(entity.SpeculationPathStatusFailed, entity.SpeculationPathStatusFailed),
			want:    0.2,
		},
		{
			name:    "merging",
			factors: Factors{PathPassed: 1, PathFailed: 1, Merging: 19, Cancelling: 1},
			batch:   entity.Batch{State: entity.BatchStateMerging},
			want:    0.95,
		},
		{
			name:    "cancelling",
			factors: Factors{PathPassed: 1, PathFailed: 1, Merging: 1, Cancelling: 0.25},
			batch:   entity.Batch{State: entity.BatchStateCancelling},
			want:    0.2,
		},
		{
			name:    "a state with no factor leaves the price alone",
			factors: Factors{PathPassed: 1, PathFailed: 1, Merging: 19, Cancelling: 0.25},
			batch:   entity.Batch{State: entity.BatchStateSpeculating},
			want:    0.5,
		},
		{
			name:    "evidence compounds across kinds",
			factors: Factors{PathPassed: 4, PathFailed: 1, Merging: 3, Cancelling: 1},
			batch:   entity.Batch{State: entity.BatchStateMerging},
			paths:   pathSet(entity.SpeculationPathStatusPassed),
			want:    0.923076923,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.InDelta(t, tt.want, predict(t, 0.5, tt.factors, tt.batch, tt.paths), 1e-9)
		})
	}
}

// A path built without one of its dependencies proves nothing about a candidate
// that assumes the dependency lands, which is what stacking on this batch means.
func TestPredict_IgnoresAPassedPathThatAssumesAFailure(t *testing.T) {
	paths := pathSet(entity.SpeculationPathStatusPassed)
	paths.Paths[0].Path.Dependencies[0].Assumption = entity.DependencyAssumptionFails

	factors := Factors{PathPassed: 9, PathFailed: 1, Merging: 1, Cancelling: 1}
	assert.InDelta(t, 0.5, predict(t, 0.5, factors, entity.Batch{}, paths), 1e-9)
}

func TestPredict_APathWithNoDependenciesCounts(t *testing.T) {
	paths := pathSet(entity.SpeculationPathStatusPassed)
	paths.Paths[0].Path.Dependencies = nil

	factors := Factors{PathPassed: 9, PathFailed: 1, Merging: 1, Cancelling: 1}
	assert.InDelta(t, 0.9, predict(t, 0.5, factors, entity.Batch{}, paths), 1e-9)
}

func TestPredict_AnEmptyPathSetIsNoEvidence(t *testing.T) {
	factors := Factors{PathPassed: 9, PathFailed: 0.1, Merging: 1, Cancelling: 1}
	assert.InDelta(t, 0.5, predict(t, 0.5, factors, entity.Batch{}, entity.SpeculationPathSet{}), 1e-9)
}

// A scorer certain either way still has to be movable, or no evidence could ever
// revise a price the scorer had no business being certain about.
func TestPredict_CertainPricesStayInRangeAndStillMove(t *testing.T) {
	tests := []struct {
		name      string
		price     float64
		factor    float64
		wantAbove float64
		wantBelow float64
	}{
		{name: "certain success, evidence against", price: 1, factor: 0.5, wantAbove: 0.99, wantBelow: 1},
		{name: "certain failure, evidence for", price: 0, factor: 2, wantAbove: 0, wantBelow: 0.01},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factors := AllOnes()
			factors.PathPassed = tt.factor
			got := predict(t, tt.price, factors, entity.Batch{}, pathSet(entity.SpeculationPathStatusPassed))
			assert.Greater(t, got, tt.wantAbove)
			assert.Less(t, got, tt.wantBelow)
		})
	}
}

func TestPredict_RejectsAPriceThatIsNotAProbability(t *testing.T) {
	for _, price := range []float64{-0.1, 1.5, math.NaN()} {
		t.Run(fmt.Sprintf("price %v", price), func(t *testing.T) {
			p := New(testCfg, fixedScorer{price: price}, AllOnes(), tally.NoopScope)
			_, err := p.Predict(context.Background(), entity.Batch{}, entity.SpeculationPathSet{})
			require.Error(t, err)
		})
	}
}

func TestPredict_PropagatesAScorerError(t *testing.T) {
	p := New(testCfg, errorScorer{}, AllOnes(), tally.NoopScope)
	_, err := p.Predict(context.Background(), entity.Batch{}, entity.SpeculationPathSet{})
	require.Error(t, err)
}

func TestNew_RejectsUnusableConstruction(t *testing.T) {
	zeroed := AllOnes()
	zeroed.Merging = 0
	negative := AllOnes()
	negative.PathFailed = -1

	tests := []struct {
		name    string
		base    scorer.Scorer
		factors Factors
	}{
		{name: "nil base", base: nil, factors: AllOnes()},
		{name: "zero factor", base: fixedScorer{price: 0.5}, factors: zeroed},
		{name: "negative factor", base: fixedScorer{price: 0.5}, factors: negative},
		{name: "unset factors", base: fixedScorer{price: 0.5}, factors: Factors{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Panics(t, func() { New(testCfg, tt.base, tt.factors, tally.NoopScope) })
		})
	}
}
