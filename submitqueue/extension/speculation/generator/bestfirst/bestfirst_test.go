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

package bestfirst

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

type scorerFunc func(context.Context, entity.Batch) (float64, error)

func (f scorerFunc) Score(ctx context.Context, batch entity.Batch) (float64, error) {
	return f(ctx, batch)
}

type recordingScorer struct {
	scores map[string]float64
	calls  map[string]int
}

func (s *recordingScorer) Score(_ context.Context, batch entity.Batch) (float64, error) {
	s.calls[batch.ID]++
	return s.scores[batch.ID], nil
}

func drain(t *testing.T, it interface {
	Next(context.Context) (entity.CandidatePath, bool, error)
}) []entity.CandidatePath {
	t.Helper()
	var candidates []entity.CandidatePath
	for {
		candidate, ok, err := it.Next(context.Background())
		require.NoError(t, err)
		if !ok {
			return candidates
		}
		candidates = append(candidates, candidate)
	}
}

func pathKey(path entity.SpeculationPath) string {
	parts := make([]string, 0, len(path.Dependencies)+1)
	parts = append(parts, path.Head)
	for _, dependency := range path.Dependencies {
		parts = append(parts, dependency.Batch+"="+string(dependency.Assumption))
	}
	return strings.Join(parts, "|")
}

func assumptionFor(t *testing.T, path entity.SpeculationPath, batchID string) entity.DependencyAssumption {
	t.Helper()
	for _, dependency := range path.Dependencies {
		if dependency.Batch == batchID {
			return dependency.Assumption
		}
	}
	t.Fatalf("dependency %q not found in path %+v", batchID, path)
	return entity.DependencyAssumptionUnknown
}

func TestNew_RequiresScorer(t *testing.T) {
	assert.Panics(t, func() {
		New(nil)
	})
}

func TestGenerate_ExampleOrdersCandidatesAcrossDisconnectedHeads(t *testing.T) {
	s := &recordingScorer{
		scores: map[string]float64{
			"A": 0.8,
			"B": 0.6,
			"E": 0.3,
		},
		calls: make(map[string]int),
	}
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "B", State: entity.BatchStateCreated},
		{ID: "C", State: entity.BatchStateSpeculating, Dependencies: []string{"A", "B"}},
		{ID: "E", State: entity.BatchStateCreated},
		{ID: "F", State: entity.BatchStateSpeculating, Dependencies: []string{"E"}},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)

	require.Len(t, got, 6)
	assert.Equal(t, []string{
		"F|E=fails",
		"C|A=succeeds|B=succeeds",
		"C|A=succeeds|B=fails",
		"F|E=succeeds",
		"C|A=fails|B=succeeds",
		"C|A=fails|B=fails",
	}, []string{
		pathKey(got[0].Path),
		pathKey(got[1].Path),
		pathKey(got[2].Path),
		pathKey(got[3].Path),
		pathKey(got[4].Path),
		pathKey(got[5].Path),
	})
	assert.InDelta(t, 0.70, got[0].RankingScore, 1e-12)
	assert.InDelta(t, 0.48, got[1].RankingScore, 1e-12)
	assert.InDelta(t, 0.32, got[2].RankingScore, 1e-12)
	assert.InDelta(t, 0.30, got[3].RankingScore, 1e-12)
	assert.InDelta(t, 0.12, got[4].RankingScore, 1e-12)
	assert.InDelta(t, 0.08, got[5].RankingScore, 1e-12)
	assert.Equal(t, map[string]int{"A": 1, "B": 1, "E": 1}, s.calls)
}

func TestGenerate_ResolvedDependenciesBecomeFixedAssumptions(t *testing.T) {
	s := &recordingScorer{
		scores: map[string]float64{"U": 0.7},
		calls:  make(map[string]int),
	}
	batches := []entity.Batch{
		{ID: "S", State: entity.BatchStateSucceeded},
		{ID: "F", State: entity.BatchStateFailed},
		{ID: "X", State: entity.BatchStateCancelled},
		{ID: "U", State: entity.BatchStateMerging},
		{
			ID:           "H",
			State:        entity.BatchStateSpeculating,
			Dependencies: []string{"S", "F", "X", "U"},
		},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)

	require.Len(t, got, 2)
	for _, candidate := range got {
		assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(t, candidate.Path, "S"))
		assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(t, candidate.Path, "F"))
		assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(t, candidate.Path, "X"))
	}
	assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(t, got[0].Path, "U"))
	assert.Equal(t, entity.DependencyAssumptionFails, assumptionFor(t, got[1].Path, "U"))
	assert.InDelta(t, 0.7, got[0].RankingScore, 1e-12)
	assert.InDelta(t, 0.3, got[1].RankingScore, 1e-12)
	assert.Equal(t, map[string]int{"U": 1}, s.calls)
}

func TestGenerate_AllResolvedDependenciesYieldOneCertainPath(t *testing.T) {
	s := scorerFunc(func(_ context.Context, batch entity.Batch) (float64, error) {
		t.Fatalf("unexpected score call for %q", batch.ID)
		return 0, nil
	})
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateSucceeded},
		{ID: "B", State: entity.BatchStateFailed},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A", "B"}},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)

	require.Len(t, got, 1)
	assert.Equal(t, "H|A=succeeds|B=fails", pathKey(got[0].Path))
	assert.Equal(t, 1.0, got[0].RankingScore)
}

func TestGenerate_OnlySpeculatingBatchesAreHeads(t *testing.T) {
	s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
		return 0.5, nil
	})
	batches := []entity.Batch{
		{ID: "created", State: entity.BatchStateCreated},
		{ID: "merging", State: entity.BatchStateMerging},
		{ID: "succeeded", State: entity.BatchStateSucceeded},
		{ID: "head", State: entity.BatchStateSpeculating},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)

	require.Len(t, got, 1)
	assert.Equal(t, "head", got[0].Path.Head)
	assert.Empty(t, got[0].Path.Dependencies)
	assert.Equal(t, 1.0, got[0].RankingScore)
}

func TestGenerate_SpeculatingBatchCanAlsoBeDependency(t *testing.T) {
	s := &recordingScorer{
		scores: map[string]float64{"A": 0.8},
		calls:  make(map[string]int),
	}
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateSpeculating},
		{ID: "B", State: entity.BatchStateSpeculating, Dependencies: []string{"A"}},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)

	require.Len(t, got, 3)
	assert.Equal(t, "A", got[0].Path.Head)
	assert.Equal(t, 1.0, got[0].RankingScore)
	assert.Equal(t, "B|A=succeeds", pathKey(got[1].Path))
	assert.Equal(t, "B|A=fails", pathKey(got[2].Path))
	assert.Equal(t, map[string]int{"A": 1}, s.calls)
}

func TestGenerate_ZeroAndOneProbabilitiesRemainExact(t *testing.T) {
	s := &recordingScorer{
		scores: map[string]float64{"A": 1, "B": 0},
		calls:  make(map[string]int),
	}
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "B", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A", "B"}},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)

	require.Len(t, got, 4)
	assert.Equal(t, "H|A=succeeds|B=fails", pathKey(got[0].Path))
	assert.Equal(t, 1.0, got[0].RankingScore)
	seen := make(map[string]struct{}, len(got))
	for i, candidate := range got {
		assert.False(t, math.IsNaN(candidate.RankingScore))
		if i > 0 {
			assert.Equal(t, 0.0, candidate.RankingScore)
		}
		seen[pathKey(candidate.Path)] = struct{}{}
	}
	assert.Len(t, seen, 4)
}

func TestGenerate_ExhaustiveResultsMatchBruteForce(t *testing.T) {
	probabilities := map[string]float64{
		"A": 0.9,
		"B": 0.7,
		"C": 0.6,
		"D": 0.2,
	}
	s := &recordingScorer{scores: probabilities, calls: make(map[string]int)}
	dependencies := []string{"A", "B", "C", "D"}
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "B", State: entity.BatchStateCreated},
		{ID: "C", State: entity.BatchStateCreated},
		{ID: "D", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: dependencies},
	}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it)
	require.Len(t, got, 1<<len(dependencies))

	expected := make(map[string]float64, len(got))
	for mask := 0; mask < 1<<len(dependencies); mask++ {
		path := entity.SpeculationPath{Head: "H"}
		probability := 1.0
		for i, id := range dependencies {
			assumption := entity.DependencyAssumptionFails
			factor := 1 - probabilities[id]
			if mask&(1<<i) != 0 {
				assumption = entity.DependencyAssumptionSucceeds
				factor = probabilities[id]
			}
			path.Dependencies = append(path.Dependencies, entity.PathDependency{
				Batch:      id,
				Assumption: assumption,
			})
			probability *= factor
		}
		expected[pathKey(path)] = probability
	}

	seen := make(map[string]struct{}, len(got))
	for i, candidate := range got {
		key := pathKey(candidate.Path)
		assert.InDelta(t, expected[key], candidate.RankingScore, 1e-12)
		seen[key] = struct{}{}
		if i > 0 {
			assert.GreaterOrEqual(t, got[i-1].RankingScore, candidate.RankingScore)
		}
	}
	assert.Len(t, seen, len(expected))
}

func TestGenerate_EqualProbabilitiesAreDeterministicAndExhaustive(t *testing.T) {
	s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
		return 0.5, nil
	})
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "B", State: entity.BatchStateCreated},
		{ID: "C", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A", "B", "C"}},
	}

	generateKeys := func() []string {
		it, err := New(s).Generate(context.Background(), batches)
		require.NoError(t, err)
		candidates := drain(t, it)
		keys := make([]string, len(candidates))
		for i, candidate := range candidates {
			keys[i] = pathKey(candidate.Path)
			assert.InDelta(t, 0.125, candidate.RankingScore, 1e-12)
		}
		return keys
	}

	first := generateKeys()
	second := generateKeys()
	assert.Equal(t, first, second)
	assert.Len(t, first, 8)
	assert.Len(t, mapFromStrings(first), 8)
}

func mapFromStrings(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func TestGenerate_PreservesDependencyOrderAndInput(t *testing.T) {
	s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
		return 0.8, nil
	})
	dependencies := []string{"B", "A"}
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "B", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: dependencies},
	}
	original := append([]string(nil), dependencies...)

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	candidate, ok, err := it.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)

	assert.Equal(t, original, dependencies)
	require.Len(t, candidate.Path.Dependencies, 2)
	assert.Equal(t, "B", candidate.Path.Dependencies[0].Batch)
	assert.Equal(t, "A", candidate.Path.Dependencies[1].Batch)
}

func TestGenerate_LargeDependencySetIsLazy(t *testing.T) {
	const dependencyCount = 50
	scores := make(map[string]float64, dependencyCount)
	batches := make([]entity.Batch, 0, dependencyCount+1)
	dependencies := make([]string, dependencyCount)
	for i := range dependencies {
		id := fmt.Sprintf("D%02d", i)
		dependencies[i] = id
		scores[id] = 0.8
		batches = append(batches, entity.Batch{ID: id, State: entity.BatchStateCreated})
	}
	batches = append(batches, entity.Batch{
		ID:           "H",
		State:        entity.BatchStateSpeculating,
		Dependencies: dependencies,
	})
	s := &recordingScorer{scores: scores, calls: make(map[string]int)}

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	var previous = math.Inf(1)
	for range 32 {
		candidate, ok, err := it.Next(context.Background())
		require.NoError(t, err)
		require.True(t, ok)
		assert.LessOrEqual(t, candidate.RankingScore, previous)
		previous = candidate.RankingScore
	}
	assert.Len(t, s.calls, dependencyCount)
}

func TestGenerate_EmptyIterator(t *testing.T) {
	s := scorerFunc(func(_ context.Context, batch entity.Batch) (float64, error) {
		t.Fatalf("unexpected score call for %q", batch.ID)
		return 0, nil
	})

	for _, batches := range [][]entity.Batch{
		nil,
		{{ID: "A", State: entity.BatchStateCreated}},
	} {
		it, err := New(s).Generate(context.Background(), batches)
		require.NoError(t, err)
		candidate, ok, err := it.Next(context.Background())
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, entity.CandidatePath{}, candidate)
	}
}

func TestGenerate_RejectsMalformedSnapshots(t *testing.T) {
	s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
		return 0.5, nil
	})
	tests := []struct {
		name    string
		batches []entity.Batch
	}{
		{
			name:    "empty batch ID",
			batches: []entity.Batch{{State: entity.BatchStateSpeculating}},
		},
		{
			name: "duplicate batch ID",
			batches: []entity.Batch{
				{ID: "A", State: entity.BatchStateCreated},
				{ID: "A", State: entity.BatchStateSpeculating},
			},
		},
		{
			name: "empty dependency ID",
			batches: []entity.Batch{
				{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{""}},
			},
		},
		{
			name: "self dependency",
			batches: []entity.Batch{
				{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"H"}},
			},
		},
		{
			name: "duplicate dependency",
			batches: []entity.Batch{
				{ID: "A", State: entity.BatchStateCreated},
				{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A", "A"}},
			},
		},
		{
			name: "missing dependency",
			batches: []entity.Batch{
				{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"missing"}},
			},
		},
		{
			name: "unknown dependency state",
			batches: []entity.Batch{
				{ID: "A", State: entity.BatchStateUnknown},
				{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(s).Generate(context.Background(), tt.batches)
			require.Error(t, err)
		})
	}
}

func TestGenerate_RejectsInvalidProbabilities(t *testing.T) {
	tests := []struct {
		name        string
		probability float64
	}{
		{name: "negative", probability: -0.1},
		{name: "above one", probability: 1.1},
		{name: "NaN", probability: math.NaN()},
		{name: "positive infinity", probability: math.Inf(1)},
		{name: "negative infinity", probability: math.Inf(-1)},
	}
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
				return tt.probability, nil
			})
			_, err := New(s).Generate(context.Background(), batches)
			require.Error(t, err)
		})
	}
}

func TestGenerate_PropagatesScorerError(t *testing.T) {
	sentinel := errors.New("score failed")
	s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
		return 0, sentinel
	})
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A"}},
	}

	_, err := New(s).Generate(context.Background(), batches)
	require.ErrorIs(t, err, sentinel)
}

func TestGenerate_AndNextHonorContextCancellation(t *testing.T) {
	s := scorerFunc(func(_ context.Context, _ entity.Batch) (float64, error) {
		return 0.8, nil
	})
	batches := []entity.Batch{
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"A"}},
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New(s).Generate(cancelled, batches)
	require.ErrorIs(t, err, context.Canceled)

	it, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	_, ok, err := it.Next(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, ok)

	candidate, ok, err := it.Next(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "H|A=succeeds", pathKey(candidate.Path))
}

func TestGenerate_ScoresDependenciesInStableOrder(t *testing.T) {
	var calls []string
	s := scorerFunc(func(_ context.Context, batch entity.Batch) (float64, error) {
		calls = append(calls, batch.ID)
		return 0.5, nil
	})
	batches := []entity.Batch{
		{ID: "C", State: entity.BatchStateCreated},
		{ID: "A", State: entity.BatchStateCreated},
		{ID: "B", State: entity.BatchStateCreated},
		{ID: "H", State: entity.BatchStateSpeculating, Dependencies: []string{"C", "A", "B"}},
	}

	_, err := New(s).Generate(context.Background(), batches)
	require.NoError(t, err)
	assert.True(t, sort.StringsAreSorted(calls))
	assert.Equal(t, []string{"A", "B", "C"}, calls)
}
