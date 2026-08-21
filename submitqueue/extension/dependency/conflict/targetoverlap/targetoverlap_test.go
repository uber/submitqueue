// Copyright (c) 2026 Uber Technologies, Inc.
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

package targetoverlap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/dependency/resolver"
)

// fakeResolver is an in-test TargetResolver that returns pre-configured target
// sets per batch ID.
type fakeResolver struct {
	targets map[string][]string
	err     error
}

func newFakeResolver() *fakeResolver {
	return &fakeResolver{targets: make(map[string][]string)}
}

func (f *fakeResolver) set(batchID string, targets ...string) *fakeResolver {
	f.targets[batchID] = targets
	return f
}

func (f *fakeResolver) failWith(err error) *fakeResolver {
	f.err = err
	return f
}

func (f *fakeResolver) ChangedTargets(_ context.Context, batch entity.Batch) ([]resolver.Target, error) {
	if f.err != nil {
		return nil, f.err
	}
	names := f.targets[batch.ID]
	targets := make([]resolver.Target, len(names))
	for i, n := range names {
		targets[i] = resolver.Target{Name: n}
	}
	return targets, nil
}

func cfg() conflict.Config {
	return conflict.Config{QueueName: "test-queue"}
}

func TestAnalyze(t *testing.T) {
	tests := []struct {
		name        string
		candidate   string
		candTargets []string
		inFlight    []struct {
			id      string
			targets []string
		}
		wantBatches []string
	}{
		{
			name:        "overlap on a shared target conflicts",
			candidate:   "cand",
			candTargets: []string{"//foo:lib", "//bar:lib"},
			inFlight: []struct {
				id      string
				targets []string
			}{
				{id: "x", targets: []string{"//bar:lib", "//baz:lib"}},
			},
			wantBatches: []string{"x"},
		},
		{
			name:        "disjoint targets do not conflict",
			candidate:   "cand",
			candTargets: []string{"//foo:lib"},
			inFlight: []struct {
				id      string
				targets []string
			}{
				{id: "x", targets: []string{"//bar:lib"}},
			},
			wantBatches: nil,
		},
		{
			name:        "only overlapping in-flight batches are reported, in order",
			candidate:   "cand",
			candTargets: []string{"//foo:lib"},
			inFlight: []struct {
				id      string
				targets []string
			}{
				{id: "x", targets: []string{"//foo:lib"}},
				{id: "y", targets: []string{"//bar:lib"}},
				{id: "z", targets: []string{"//foo:lib"}},
			},
			wantBatches: []string{"x", "z"},
		},
		{
			name:        "candidate with no targets conflicts with nothing",
			candidate:   "cand",
			candTargets: nil,
			inFlight: []struct {
				id      string
				targets []string
			}{
				{id: "x", targets: []string{"//foo:lib"}},
			},
			wantBatches: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newFakeResolver().set(tt.candidate, tt.candTargets...)
			inFlight := make([]entity.Batch, 0, len(tt.inFlight))
			for _, f := range tt.inFlight {
				r.set(f.id, f.targets...)
				inFlight = append(inFlight, entity.Batch{ID: f.id})
			}

			got, err := New(cfg(), r).Analyze(context.Background(), entity.Batch{ID: tt.candidate}, inFlight)
			require.NoError(t, err)

			var ids []string
			for _, c := range got {
				assert.Equal(t, entity.ConflictTypeTargetOverlap, c.Type)
				ids = append(ids, c.BatchID)
			}
			assert.Equal(t, tt.wantBatches, ids)
		})
	}
}

func TestAnalyze_EmptyInFlight(t *testing.T) {
	got, err := New(cfg(), newFakeResolver()).Analyze(context.Background(), entity.Batch{ID: "cand"}, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestAnalyze_ResolverError(t *testing.T) {
	sentinel := errors.New("resolver unavailable")

	t.Run("candidate resolution fails", func(t *testing.T) {
		r := newFakeResolver().failWith(sentinel)
		_, err := New(cfg(), r).Analyze(context.Background(), entity.Batch{ID: "cand"}, []entity.Batch{{ID: "x"}})
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("in-flight resolution fails", func(t *testing.T) {
		r := newFakeResolver().set("cand", "//foo:lib").failWith(sentinel)
		_, err := New(cfg(), r).Analyze(context.Background(), entity.Batch{ID: "cand"}, []entity.Batch{{ID: "x"}})
		require.ErrorIs(t, err, sentinel)
	})
}
