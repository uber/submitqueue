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

package bestfirst

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator"
)

// scoreFunc adapts a function to scorer.Scorer.
type scoreFunc func(ctx context.Context, batch entity.Batch) (float64, error)

func (f scoreFunc) Score(ctx context.Context, batch entity.Batch) (float64, error) {
	return f(ctx, batch)
}

// scoresByID serves fixed per-batch scores and fails the run on any batch it
// has no score for, so a test also asserts which batches get scored at all.
func scoresByID(scores map[string]float64) scorer.Scorer {
	return scoreFunc(func(_ context.Context, b entity.Batch) (float64, error) {
		s, ok := scores[b.ID]
		if !ok {
			return 0, fmt.Errorf("unexpected score request for %s", b.ID)
		}
		return s, nil
	})
}

func batch(id string, state entity.BatchState, deps ...string) entity.Batch {
	return entity.Batch{ID: id, Queue: "q", State: state, Dependencies: deps}
}

func succeeds(id string) entity.PathDependency {
	return entity.PathDependency{Batch: id, Assumption: entity.DependencyAssumptionSucceeds}
}

func fails(id string) entity.PathDependency {
	return entity.PathDependency{Batch: id, Assumption: entity.DependencyAssumptionFails}
}

func path(head string, deps ...entity.PathDependency) entity.SpeculationPath {
	return entity.SpeculationPath{Head: head, Dependencies: append([]entity.PathDependency{}, deps...)}
}

// drain pulls until the stream ends, requiring it to end before max pulls.
func drain(t *testing.T, it generator.PathIterator, max int) []entity.CandidatePath {
	t.Helper()
	var out []entity.CandidatePath
	for {
		c, ok, err := it.Next(context.Background())
		require.NoError(t, err)
		if !ok {
			return out
		}
		out = append(out, c)
		require.Less(t, len(out), max, "stream did not end")
	}
}

// requireStream asserts the exact candidate sequence: paths equal, scores close.
func requireStream(t *testing.T, want []entity.CandidatePath, got []entity.CandidatePath) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equalf(t, want[i].Path, got[i].Path, "candidate %d", i)
		assert.InDeltaf(t, want[i].RankingScore, got[i].RankingScore, 1e-9, "candidate %d score", i)
	}
}

func TestSingleHeadStreams(t *testing.T) {
	tests := []struct {
		name    string
		batches []entity.Batch
		scores  map[string]float64
		want    []entity.CandidatePath
	}{
		{
			name:    "no dependencies yields the sure path",
			batches: []entity.Batch{batch("q/batch/1", entity.BatchStateSpeculating)},
			want: []entity.CandidatePath{
				{Path: path("q/batch/1"), RankingScore: 1},
			},
		},
		{
			name: "resolved dependencies are pinned facts and only undecided ones swing",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateSucceeded),
				batch("q/batch/2", entity.BatchStateFailed),
				batch("q/batch/3", entity.BatchStateCancelled),
				batch("q/batch/4", entity.BatchStateCreated),
				batch("q/batch/9", entity.BatchStateSpeculating, "q/batch/1", "q/batch/2", "q/batch/3", "q/batch/4"),
			},
			scores: map[string]float64{"q/batch/4": 0.6},
			want: []entity.CandidatePath{
				{Path: path("q/batch/9", succeeds("q/batch/1"), fails("q/batch/2"), fails("q/batch/3"), succeeds("q/batch/4")), RankingScore: 0.6},
				{Path: path("q/batch/9", succeeds("q/batch/1"), fails("q/batch/2"), fails("q/batch/3"), fails("q/batch/4")), RankingScore: 0.4},
			},
		},
		{
			name: "merging and cancelling dependencies are priced as modal certainties",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateMerging),
				batch("q/batch/2", entity.BatchStateCancelling),
				batch("q/batch/9", entity.BatchStateSpeculating, "q/batch/1", "q/batch/2"),
			},
			want: []entity.CandidatePath{
				{Path: path("q/batch/9", succeeds("q/batch/1"), fails("q/batch/2")), RankingScore: 1},
			},
		},
		{
			name: "scores of exactly zero and one pin the assumption",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateCreated),
				batch("q/batch/2", entity.BatchStateCreated),
				batch("q/batch/9", entity.BatchStateSpeculating, "q/batch/1", "q/batch/2"),
			},
			scores: map[string]float64{"q/batch/1": 0, "q/batch/2": 1},
			want: []entity.CandidatePath{
				{Path: path("q/batch/9", fails("q/batch/1"), succeeds("q/batch/2")), RankingScore: 1},
			},
		},
		{
			name: "coin-flip dependency hedges both branches, modal first",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateSpeculating),
				batch("q/batch/9", entity.BatchStateSpeculating, "q/batch/1"),
			},
			scores: map[string]float64{"q/batch/1": 0.5},
			want: []entity.CandidatePath{
				{Path: path("q/batch/1"), RankingScore: 1},
				{Path: path("q/batch/9", succeeds("q/batch/1")), RankingScore: 0.5},
				{Path: path("q/batch/9", fails("q/batch/1")), RankingScore: 0.5},
			},
		},
		{
			name: "duplicate dependency entries collapse to one",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateCreated),
				batch("q/batch/9", entity.BatchStateSpeculating, "q/batch/1", "q/batch/1"),
			},
			scores: map[string]float64{"q/batch/1": 0.7},
			want: []entity.CandidatePath{
				{Path: path("q/batch/9", succeeds("q/batch/1")), RankingScore: 0.7},
				{Path: path("q/batch/9", fails("q/batch/1")), RankingScore: 0.3},
			},
		},
		{
			name: "unlikely dependency is assumed to fail on the modal path",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateCreated),
				batch("q/batch/9", entity.BatchStateSpeculating, "q/batch/1"),
			},
			scores: map[string]float64{"q/batch/1": 0.2},
			want: []entity.CandidatePath{
				{Path: path("q/batch/9", fails("q/batch/1")), RankingScore: 0.8},
				{Path: path("q/batch/9", succeeds("q/batch/1")), RankingScore: 0.2},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			it, err := New(scoresByID(tc.scores)).Open(context.Background(), tc.batches)
			require.NoError(t, err)
			requireStream(t, tc.want, drain(t, it, 100))
		})
	}
}

// TestBestFirstAcrossHeads walks the reference example from the design doc: a
// five-batch queue (two roots, a diamond, a leaf) whose full stream must come
// out in exact best-first order — sure paths first, hedges adjacent to their
// modal twins, deeper long shots later.
func TestBestFirstAcrossHeads(t *testing.T) {
	batches := []entity.Batch{
		batch("q/batch/1", entity.BatchStateSpeculating),
		batch("q/batch/2", entity.BatchStateSpeculating),
		batch("q/batch/3", entity.BatchStateSpeculating, "q/batch/1", "q/batch/2"),
		batch("q/batch/4", entity.BatchStateSpeculating, "q/batch/2"),
		batch("q/batch/5", entity.BatchStateSpeculating, "q/batch/1", "q/batch/2", "q/batch/3", "q/batch/4"),
	}
	scores := map[string]float64{
		"q/batch/1": 0.9,
		"q/batch/2": 0.5,
		"q/batch/3": 0.8,
		"q/batch/4": 0.7,
	}

	it, err := New(scoresByID(scores)).Open(context.Background(), batches)
	require.NoError(t, err)
	got := drain(t, it, 100)

	wantFirst := []entity.CandidatePath{
		{Path: path("q/batch/1"), RankingScore: 1},
		{Path: path("q/batch/2"), RankingScore: 1},
		{Path: path("q/batch/4", succeeds("q/batch/2")), RankingScore: 0.5},
		{Path: path("q/batch/4", fails("q/batch/2")), RankingScore: 0.5},
		{Path: path("q/batch/3", succeeds("q/batch/1"), succeeds("q/batch/2")), RankingScore: 0.45},
		{Path: path("q/batch/3", succeeds("q/batch/1"), fails("q/batch/2")), RankingScore: 0.45},
		{Path: path("q/batch/5", succeeds("q/batch/1"), succeeds("q/batch/2"), succeeds("q/batch/3"), succeeds("q/batch/4")), RankingScore: 0.252},
		{Path: path("q/batch/5", succeeds("q/batch/1"), fails("q/batch/2"), succeeds("q/batch/3"), succeeds("q/batch/4")), RankingScore: 0.252},
		{Path: path("q/batch/5", succeeds("q/batch/1"), succeeds("q/batch/2"), succeeds("q/batch/3"), fails("q/batch/4")), RankingScore: 0.108},
		{Path: path("q/batch/5", succeeds("q/batch/1"), fails("q/batch/2"), succeeds("q/batch/3"), fails("q/batch/4")), RankingScore: 0.108},
	}
	require.GreaterOrEqual(t, len(got), len(wantFirst))
	requireStream(t, wantFirst, got[:len(wantFirst)])

	// The whole space: 1 + 1 + 4 + 2 + 16 paths, each exactly once.
	assert.Len(t, got, 24)
	seen := make(map[string]bool, len(got))
	for _, c := range got {
		id := c.Path.ID()
		assert.False(t, seen[id], "path repeated: %+v", c.Path)
		seen[id] = true
	}

	// Best-first means globally non-increasing prices, all of them positive.
	for i := 1; i < len(got); i++ {
		assert.LessOrEqual(t, got[i].RankingScore, got[i-1].RankingScore, "candidate %d outranks its predecessor", i)
	}
	assert.Greater(t, got[len(got)-1].RankingScore, 0.0)
}

func TestOnlySpeculatingBatchesAreHeads(t *testing.T) {
	batches := []entity.Batch{
		batch("q/batch/1", entity.BatchStateCreated),
		batch("q/batch/2", entity.BatchStateMerging),
		batch("q/batch/3", entity.BatchStateSucceeded),
		batch("q/batch/4", entity.BatchStateCancelling),
		batch("q/batch/5", entity.BatchStateSpeculating),
	}
	it, err := New(scoresByID(nil)).Open(context.Background(), batches)
	require.NoError(t, err)
	requireStream(t, []entity.CandidatePath{{Path: path("q/batch/5"), RankingScore: 1}}, drain(t, it, 10))
}

func TestIncompleteSnapshotSkipsTheHead(t *testing.T) {
	tests := []struct {
		name    string
		batches []entity.Batch
		scores  map[string]float64
		want    []entity.CandidatePath
	}{
		{
			name: "dependency missing from the snapshot",
			batches: []entity.Batch{
				batch("q/batch/2", entity.BatchStateSpeculating, "q/batch/1"),
				batch("q/batch/3", entity.BatchStateSpeculating, "q/batch/2"),
			},
			scores: map[string]float64{"q/batch/2": 0.7},
			want: []entity.CandidatePath{
				{Path: path("q/batch/3", succeeds("q/batch/2")), RankingScore: 0.7},
				{Path: path("q/batch/3", fails("q/batch/2")), RankingScore: 0.3},
			},
		},
		{
			name: "dependency in a state the model does not cover",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateCreating),
				batch("q/batch/2", entity.BatchStateSpeculating, "q/batch/1"),
				batch("q/batch/3", entity.BatchStateSpeculating),
			},
			want: []entity.CandidatePath{
				{Path: path("q/batch/3"), RankingScore: 1},
			},
		},
		{
			name: "head listed as its own dependency",
			batches: []entity.Batch{
				batch("q/batch/1", entity.BatchStateSpeculating, "q/batch/1"),
				batch("q/batch/2", entity.BatchStateSpeculating),
			},
			want: []entity.CandidatePath{
				{Path: path("q/batch/2"), RankingScore: 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			it, err := New(scoresByID(tc.scores)).Open(context.Background(), tc.batches)
			require.NoError(t, err)
			requireStream(t, tc.want, drain(t, it, 10))
		})
	}
}

func TestScorerFailuresFailOpen(t *testing.T) {
	head := batch("q/batch/2", entity.BatchStateSpeculating, "q/batch/1")
	dep := batch("q/batch/1", entity.BatchStateCreated)

	t.Run("scorer error propagates", func(t *testing.T) {
		sentinel := errors.New("scorer down")
		sc := scoreFunc(func(context.Context, entity.Batch) (float64, error) { return 0, sentinel })
		_, err := New(sc).Open(context.Background(), []entity.Batch{dep, head})
		require.ErrorIs(t, err, sentinel)
	})

	for _, invalid := range []float64{math.NaN(), -0.1, 1.5} {
		t.Run(fmt.Sprintf("invalid probability %v", invalid), func(t *testing.T) {
			sc := scoreFunc(func(context.Context, entity.Batch) (float64, error) { return invalid, nil })
			_, err := New(sc).Open(context.Background(), []entity.Batch{dep, head})
			require.Error(t, err)
		})
	}
}

func TestContextCancellation(t *testing.T) {
	batches := []entity.Batch{batch("q/batch/1", entity.BatchStateSpeculating)}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("open aborts", func(t *testing.T) {
		_, err := New(scoresByID(nil)).Open(cancelled, batches)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("next ends the stream", func(t *testing.T) {
		it, err := New(scoresByID(nil)).Open(context.Background(), batches)
		require.NoError(t, err)
		_, ok, err := it.Next(cancelled)
		require.ErrorIs(t, err, context.Canceled)
		assert.False(t, ok)
	})
}

// TestCanonicalOrderAndDeterminism feeds the same queue twice with every input
// slice reversed. Both streams must be identical — dependencies normalized into
// queue order (numeric counter order, so batch/2 before batch/10), heads seeded
// in queue order — because path IDs hash the dependency order and must come out
// the same on every run.
func TestCanonicalOrderAndDeterminism(t *testing.T) {
	scores := map[string]float64{"q/batch/2": 0.9, "q/batch/10": 0.6}
	build := func(depOrder, batchOrder bool) []entity.Batch {
		deps := []string{"q/batch/10", "q/batch/2"}
		if depOrder {
			deps = []string{"q/batch/2", "q/batch/10"}
		}
		batches := []entity.Batch{
			batch("q/batch/1", entity.BatchStateSpeculating),
			batch("q/batch/2", entity.BatchStateSpeculating),
			batch("q/batch/10", entity.BatchStateSpeculating),
			batch("q/batch/11", entity.BatchStateSpeculating, deps...),
		}
		if batchOrder {
			for i, j := 0, len(batches)-1; i < j; i, j = i+1, j-1 {
				batches[i], batches[j] = batches[j], batches[i]
			}
		}
		return batches
	}

	first := build(false, false)
	inputDeps := first[3].Dependencies
	it, err := New(scoresByID(scores)).Open(context.Background(), first)
	require.NoError(t, err)
	got := drain(t, it, 20)

	want := []entity.CandidatePath{
		{Path: path("q/batch/1"), RankingScore: 1},
		{Path: path("q/batch/2"), RankingScore: 1},
		{Path: path("q/batch/10"), RankingScore: 1},
		{Path: path("q/batch/11", succeeds("q/batch/2"), succeeds("q/batch/10")), RankingScore: 0.54},
		{Path: path("q/batch/11", succeeds("q/batch/2"), fails("q/batch/10")), RankingScore: 0.36},
		{Path: path("q/batch/11", fails("q/batch/2"), succeeds("q/batch/10")), RankingScore: 0.06},
		{Path: path("q/batch/11", fails("q/batch/2"), fails("q/batch/10")), RankingScore: 0.04},
	}
	requireStream(t, want, got)

	// The caller's snapshot is read, never written: the head's dependency slice
	// keeps its original order.
	assert.Equal(t, []string{"q/batch/10", "q/batch/2"}, inputDeps)

	it2, err := New(scoresByID(scores)).Open(context.Background(), build(true, true))
	require.NoError(t, err)
	require.Equal(t, got, drain(t, it2, 20))
}
