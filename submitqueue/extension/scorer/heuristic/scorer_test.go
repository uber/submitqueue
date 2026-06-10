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

package heuristic

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// staticValue returns a ValueFunc that always returns the given value.
func staticValue(value int) ValueFunc {
	return func(_ context.Context, _ entity.BatchChanges) (int, error) {
		return value, nil
	}
}

func TestScorer_Score(t *testing.T) {
	tests := []struct {
		name      string
		buckets   []Bucket
		valueFunc ValueFunc
		want      float64
		wantErr   bool
	}{
		{
			name: "single bucket covering all values",
			buckets: []Bucket{
				{Min: 0, Max: 1000, Score: 0.9},
			},
			valueFunc: staticValue(5),
			want:      0.9,
		},
		{
			name: "multiple buckets with different ranges",
			buckets: []Bucket{
				{Min: 0, Max: 5, Score: 0.95},
				{Min: 6, Max: 20, Score: 0.75},
				{Min: 21, Max: 100, Score: 0.5},
			},
			valueFunc: staticValue(10),
			want:      0.75,
		},
		{
			name: "exact min boundary",
			buckets: []Bucket{
				{Min: 0, Max: 5, Score: 0.95},
				{Min: 6, Max: 20, Score: 0.75},
			},
			valueFunc: staticValue(6),
			want:      0.75,
		},
		{
			name: "exact max boundary",
			buckets: []Bucket{
				{Min: 0, Max: 5, Score: 0.95},
				{Min: 6, Max: 20, Score: 0.75},
			},
			valueFunc: staticValue(5),
			want:      0.95,
		},
		{
			name: "no matching bucket",
			buckets: []Bucket{
				{Min: 0, Max: 5, Score: 0.95},
				{Min: 10, Max: 20, Score: 0.75},
			},
			valueFunc: staticValue(7),
			wantErr:   true,
		},
		{
			name: "zero metric value",
			buckets: []Bucket{
				{Min: 0, Max: 0, Score: 1.0},
				{Min: 1, Max: 100, Score: 0.8},
			},
			valueFunc: staticValue(0),
			want:      1.0,
		},
		{
			name: "first matching bucket wins",
			buckets: []Bucket{
				{Min: 0, Max: 10, Score: 0.9},
				{Min: 5, Max: 15, Score: 0.7},
			},
			valueFunc: staticValue(7),
			want:      0.9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(changesetfake.New(), tt.buckets, tt.valueFunc, tally.NoopScope)
			got, err := s.Score(context.Background(), entity.Batch{})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestScorer_Score_ValueFuncError(t *testing.T) {
	failing := func(_ context.Context, _ entity.BatchChanges) (int, error) {
		return 0, assert.AnError
	}
	s := New(changesetfake.New(), []Bucket{{Min: 0, Max: 10, Score: 0.9}}, failing, tally.NoopScope)
	_, err := s.Score(context.Background(), entity.Batch{})
	require.Error(t, err)
}

func TestNew_NilValueFunc(t *testing.T) {
	assert.Panics(t, func() {
		New(changesetfake.New(), []Bucket{{Min: 0, Max: 10, Score: 0.85}}, nil, tally.NoopScope)
	})
}
