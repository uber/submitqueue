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

package probability

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

const headID = "q/batch/5"

func TestScore_ProbabilityFormula(t *testing.T) {
	tests := []struct {
		name string
		head entity.Batch
		deps map[string]entity.Batch
		path entity.SpeculationPath
		want float32
	}{
		{
			name: "healthy dep in base multiplies straight through",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.9}},
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: headID},
			want: 0.72,
		},
		{
			name: "shaky dep not in base contributes its failure probability",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.9}},
			path: entity.SpeculationPath{Head: headID},
			want: 0.08, // 0.8 * (1 - 0.9)
		},
		{
			name: "succeeded dep in base is certainty, overriding its prediction",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.3, State: entity.BatchStateSucceeded}},
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: headID},
			want: 0.8, // 0.8 * 1
		},
		{
			name: "succeeded dep not in base zeroes the path betting against it",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.3, State: entity.BatchStateSucceeded}},
			path: entity.SpeculationPath{Head: headID},
			want: 0, // 0.8 * (1 - 1)
		},
		{
			name: "failed dep in base zeroes the path built on it",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.9, State: entity.BatchStateFailed}},
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: headID},
			want: 0, // 0.8 * 0
		},
		{
			name: "failed dep not in base boosts the path that excluded it",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.9, State: entity.BatchStateFailed}},
			path: entity.SpeculationPath{Head: headID},
			want: 0.8, // 0.8 * (1 - 0)
		},
		{
			name: "cancelled dep in base zeroes the path built on it",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.9, State: entity.BatchStateCancelled}},
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: headID},
			want: 0, // 0.8 * 0
		},
		{
			name: "cancelling dep keeps its prediction — cancellation is best-effort",
			head: entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", Score: 0.9, State: entity.BatchStateCancelling}},
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: headID},
			want: 0.72, // 0.8 * 0.9
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockBatchStore(ctrl)
			store.EXPECT().Get(gomock.Any(), headID).Return(tt.head, nil)
			for id, b := range tt.deps {
				store.EXPECT().Get(gomock.Any(), id).Return(b, nil)
			}

			tree := entity.SpeculationTree{
				BatchID: headID,
				Paths:   []entity.SpeculationPathInfo{{ID: "p/0", Path: tt.path, Status: entity.SpeculationPathStatusCandidate}},
			}
			got, err := New(store).Score(context.Background(), tree)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "p/0", got[0].PathID)
			assert.InDelta(t, tt.want, got[0].Score, 0.0001)
		})
	}
}

// TestScore_DependencyFailureShiftsMassToSurvivingPaths pins the respeculate
// story end-to-end: with three paths over deps A and B, a failure of B zeroes
// the chain path that built on it and boosts the drop-B path by the full
// (1 - p(B)) = 1 factor — the score mass shifts to the paths consistent with
// the resolved outcome, with no cross-path coupling in the scorer.
func TestScore_DependencyFailureShiftsMassToSurvivingPaths(t *testing.T) {
	depA := entity.Batch{ID: "q/batch/1", Score: 0.9}
	depB := entity.Batch{ID: "q/batch/2", Score: 0.6}
	head := entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{depA.ID, depB.ID}}

	tree := entity.SpeculationTree{
		BatchID: headID,
		Paths: []entity.SpeculationPathInfo{
			{ID: "p/chain", Path: entity.SpeculationPath{Base: []string{depA.ID, depB.ID}, Head: headID}}, // full chain
			{ID: "p/drop-b", Path: entity.SpeculationPath{Base: []string{depA.ID}, Head: headID}},         // drop B
			{ID: "p/alone", Path: entity.SpeculationPath{Head: headID}},                                   // alone
		},
	}

	score := func(t *testing.T, deps ...entity.Batch) []float32 {
		ctrl := gomock.NewController(t)
		store := storagemock.NewMockBatchStore(ctrl)
		store.EXPECT().Get(gomock.Any(), headID).Return(head, nil)
		for _, d := range deps {
			store.EXPECT().Get(gomock.Any(), d.ID).Return(d, nil)
		}
		got, err := New(store).Score(context.Background(), tree)
		require.NoError(t, err)
		require.Len(t, got, 3)
		scores := make([]float32, 3)
		for i, ps := range got {
			assert.Equal(t, tree.Paths[i].ID, ps.PathID)
			scores[i] = ps.Score
		}
		return scores
	}

	t.Run("before: both deps in flight, chain path leads", func(t *testing.T) {
		scores := score(t, depA, depB)
		assert.InDelta(t, 0.8*0.9*0.6, scores[0], 0.0001)         // chain
		assert.InDelta(t, 0.8*0.9*(1-0.6), scores[1], 0.0001)     // drop B
		assert.InDelta(t, 0.8*(1-0.9)*(1-0.6), scores[2], 0.0001) // alone
	})

	t.Run("after: B failed, drop-B path is boosted and chain dies", func(t *testing.T) {
		failedB := depB
		failedB.State = entity.BatchStateFailed
		scores := score(t, depA, failedB)
		assert.InDelta(t, 0, scores[0], 0.0001)             // chain: bets on B landing
		assert.InDelta(t, 0.8*0.9*1, scores[1], 0.0001)     // drop B: (1 - 0) boost
		assert.InDelta(t, 0.8*(1-0.9)*1, scores[2], 0.0001) // alone
	})
}

func TestScore_MultiplePathsScoredIndependentlyAndDepsCachedOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockBatchStore(ctrl)

	head := entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1", "q/batch/2"}}
	dep1 := entity.Batch{ID: "q/batch/1", Score: 0.9}
	dep2 := entity.Batch{ID: "q/batch/2", Score: 0.6}

	// Each batch is loaded at most once per Score call, even though both
	// paths below reference dep1 and dep2.
	store.EXPECT().Get(gomock.Any(), headID).Return(head, nil).Times(1)
	store.EXPECT().Get(gomock.Any(), "q/batch/1").Return(dep1, nil).Times(1)
	store.EXPECT().Get(gomock.Any(), "q/batch/2").Return(dep2, nil).Times(1)

	tree := entity.SpeculationTree{
		BatchID: headID,
		Paths: []entity.SpeculationPathInfo{
			{ID: "p/0", Path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: headID}},
			{ID: "p/1", Path: entity.SpeculationPath{Base: []string{"q/batch/2"}, Head: headID}},
		},
	}

	got, err := New(store).Score(context.Background(), tree)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "p/0", got[0].PathID)
	assert.InDelta(t, 0.8*0.9*(1-0.6), got[0].Score, 0.0001)
	assert.Equal(t, "p/1", got[1].PathID)
	assert.InDelta(t, 0.8*0.6*(1-0.9), got[1].Score, 0.0001)
}

func TestScore_HeadStoreErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockBatchStore(ctrl)
	sentinel := errors.New("boom")
	store.EXPECT().Get(gomock.Any(), headID).Return(entity.Batch{}, sentinel)

	tree := entity.SpeculationTree{
		BatchID: headID,
		Paths:   []entity.SpeculationPathInfo{{Path: entity.SpeculationPath{Head: headID}}},
	}
	_, err := New(store).Score(context.Background(), tree)
	require.ErrorIs(t, err, sentinel)
}

func TestScore_DependencyStoreErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockBatchStore(ctrl)
	sentinel := errors.New("boom")
	head := entity.Batch{ID: headID, Score: 0.8, Dependencies: []string{"q/batch/1"}}
	store.EXPECT().Get(gomock.Any(), headID).Return(head, nil)
	store.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{}, sentinel)

	tree := entity.SpeculationTree{
		BatchID: headID,
		Paths:   []entity.SpeculationPathInfo{{Path: entity.SpeculationPath{Head: headID}}},
	}
	_, err := New(store).Score(context.Background(), tree)
	require.ErrorIs(t, err, sentinel)
}

func TestScore_EmptyTreeSkipsStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockBatchStore(ctrl)
	// No EXPECT() calls set up: the store must not be touched for an empty tree.

	tree := entity.SpeculationTree{BatchID: headID}
	got, err := New(store).Score(context.Background(), tree)
	require.NoError(t, err)
	assert.Empty(t, got)
}
