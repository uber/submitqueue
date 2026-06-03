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

package composite

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/scorer"
)

// fixedScorer always returns a fixed score.
type fixedScorer struct {
	score float64
}

func (f *fixedScorer) Score(_ context.Context, _ entity.Change) (float64, error) {
	return f.score, nil
}

// errorScorer always returns an error.
type errorScorer struct{}

func (e *errorScorer) Score(_ context.Context, _ entity.Change) (float64, error) {
	return 0, fmt.Errorf("scorer failed")
}

func TestScorer_Score(t *testing.T) {
	tests := []struct {
		name    string
		scorers map[string]scorer.Scorer
		reduce  ReduceFunc
		want    float64
	}{
		{
			name: "min of two scorers",
			scorers: map[string]scorer.Scorer{
				"files": &fixedScorer{0.9},
				"deps":  &fixedScorer{0.6},
			},
			reduce: Min,
			want:   0.6,
		},
		{
			name: "max of two scorers",
			scorers: map[string]scorer.Scorer{
				"files": &fixedScorer{0.9},
				"deps":  &fixedScorer{0.6},
			},
			reduce: Max,
			want:   0.9,
		},
		{
			name: "avg of two scorers",
			scorers: map[string]scorer.Scorer{
				"files": &fixedScorer{0.9},
				"deps":  &fixedScorer{0.6},
			},
			reduce: Avg,
			want:   0.75,
		},
		{
			name: "single scorer passthrough",
			scorers: map[string]scorer.Scorer{
				"files": &fixedScorer{0.9},
			},
			reduce: Avg,
			want:   0.9,
		},
		{
			name: "avg of three scorers",
			scorers: map[string]scorer.Scorer{
				"files": &fixedScorer{0.9},
				"deps":  &fixedScorer{0.95},
				"lines": &fixedScorer{0.85},
			},
			reduce: Avg,
			want:   0.9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(tt.scorers, tt.reduce, tally.NoopScope)
			got, err := s.Score(context.Background(), entity.Change{})
			require.NoError(t, err)
			assert.InDelta(t, tt.want, got, 1e-9)
		})
	}
}

func TestScorer_Score_ChildError(t *testing.T) {
	s := New(map[string]scorer.Scorer{
		"error": &errorScorer{},
		"files": &fixedScorer{0.9},
	}, Min, tally.NoopScope)
	_, err := s.Score(context.Background(), entity.Change{})
	require.Error(t, err)
}

func TestNew_EmptyScorers(t *testing.T) {
	assert.Panics(t, func() {
		New(map[string]scorer.Scorer{}, Min, tally.NoopScope)
	})
}

func TestNew_NilReduce(t *testing.T) {
	assert.Panics(t, func() {
		New(map[string]scorer.Scorer{"files": &fixedScorer{0.9}}, nil, tally.NoopScope)
	})
}

func TestReduceFunc_ReceivesNames(t *testing.T) {
	var receivedNames []string
	custom := func(scores map[string]float64) float64 {
		for name := range scores {
			receivedNames = append(receivedNames, name)
		}
		return scores["files"]
	}

	s := New(map[string]scorer.Scorer{
		"files": &fixedScorer{0.9},
		"deps":  &fixedScorer{0.95},
	}, custom, tally.NoopScope)
	got, err := s.Score(context.Background(), entity.Change{})
	require.NoError(t, err)
	assert.Equal(t, 0.9, got)
	assert.ElementsMatch(t, []string{"files", "deps"}, receivedNames)
}
