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

package sticky

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// fakeIter yields a fixed slice of candidates, optionally failing on the first pull.
type fakeIter struct {
	items []entity.CandidatePath
	idx   int
	fail  bool
}

func (f *fakeIter) Next(context.Context) (entity.CandidatePath, bool, error) {
	if f.fail {
		return entity.CandidatePath{}, false, errors.New("iterator boom")
	}
	if f.idx >= len(f.items) {
		return entity.CandidatePath{}, false, nil
	}
	c := f.items[f.idx]
	f.idx++
	return c, true, nil
}

// candidate builds a one-dependency candidate path for the given head and
// assumption.
func candidate(head string, assumption entity.DependencyAssumption, dep string) entity.CandidatePath {
	return entity.CandidatePath{
		Path: entity.SpeculationPath{
			Head:         head,
			Dependencies: []entity.PathDependency{{Batch: dep, Assumption: assumption}},
		},
	}
}

// fundedSet returns a path set holding one entry for head at the given status,
// whose ID matches candidate(head, assumption, dep) so it lines up with an
// iterator item.
func fundedSet(head string, assumption entity.DependencyAssumption, dep string, status entity.SpeculationPathStatus) entity.SpeculationPathSet {
	c := candidate(head, assumption, dep)
	return entity.SpeculationPathSet{
		Head:  head,
		Paths: []entity.SpeculationPathEntry{{ID: c.Path.ID(), Path: c.Path, Status: status}},
	}
}

func TestSticky_Allocate(t *testing.T) {
	tests := []struct {
		name     string
		budget   int
		pathSets []entity.SpeculationPathSet
		items    []entity.CandidatePath
		iterFail bool
		wantErr  bool
		// wantIdx are indices into items expected to be proposed as builds, in order.
		wantIdx []int
	}{
		{
			name:   "fills free budget in order",
			budget: 2,
			items: []entity.CandidatePath{
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"),
				candidate("q/2", entity.DependencyAssumptionFails, "q/1"),
				candidate("q/3", entity.DependencyAssumptionSucceeds, "q/1"),
			},
			wantIdx: []int{0, 1},
		},
		{
			name:     "keeps in-flight path funded and fills the free slot",
			budget:   2,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusBuilding)},
			items: []entity.CandidatePath{
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"), // already funded -> skipped
				candidate("q/2", entity.DependencyAssumptionFails, "q/1"),    // fills the one free slot
			},
			wantIdx: []int{1},
		},
		{
			name:     "terminal entry does not charge budget",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusPassed)},
			items:    []entity.CandidatePath{candidate("q/3", entity.DependencyAssumptionFails, "q/1")},
			wantIdx:  []int{0},
		},
		{
			// The generator enumerates the whole space, so a path whose build
			// already ran comes back round as a candidate — even as the
			// top-ranked one. It must be skipped without spending the slot, which
			// then goes to the next eligible candidate.
			name:     "skips a passed top candidate without spending the slot",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusPassed)},
			items: []entity.CandidatePath{
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"), // the passed path itself
				candidate("q/2", entity.DependencyAssumptionFails, "q/1"),
			},
			wantIdx: []int{1},
		},
		{
			name:     "skips a failed top candidate without spending the slot",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusFailed)},
			items: []entity.CandidatePath{
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"), // the failed path itself
				candidate("q/2", entity.DependencyAssumptionFails, "q/1"),
			},
			wantIdx: []int{1},
		},
		{
			name:     "skips a cancelled top candidate without spending the slot",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusCancelled)},
			items: []entity.CandidatePath{
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"), // the cancelled path itself
				candidate("q/2", entity.DependencyAssumptionFails, "q/1"),
			},
			wantIdx: []int{1},
		},
		{
			name:     "pending entry charges budget",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusPending)},
			items:    []entity.CandidatePath{candidate("q/3", entity.DependencyAssumptionSucceeds, "q/1")},
			wantIdx:  nil,
		},
		{
			// Cancelling is non-terminal, so it still charges the budget: sticky
			// must not spend a slot it only expects the cancel to free, which would
			// oversubscribe the hard cap on concurrent builds.
			name:     "cancelling entry charges budget",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusCancelling)},
			items:    []entity.CandidatePath{candidate("q/3", entity.DependencyAssumptionSucceeds, "q/1")},
			wantIdx:  nil,
		},
		{
			// A zero-valued status is a data defect: any reading of the record
			// could be wrong, so sticky fails fast rather than guessing.
			name:     "entry with no status fails fast",
			budget:   1,
			pathSets: []entity.SpeculationPathSet{fundedSet("q/2", entity.DependencyAssumptionSucceeds, "q/1", entity.SpeculationPathStatusUnknown)},
			items:    []entity.CandidatePath{candidate("q/3", entity.DependencyAssumptionSucceeds, "q/1")},
			wantErr:  true,
		},
		{
			name:   "deduplicates a repeated candidate",
			budget: 5,
			items: []entity.CandidatePath{
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"),
				candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"),
			},
			wantIdx: []int{0},
		},
		{
			name:     "propagates iterator error",
			budget:   2,
			iterFail: true,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := &fakeIter{items: tt.items, fail: tt.iterFail}
			got, err := New(tt.budget).Allocate(context.Background(), tt.pathSets, iter)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Len(t, got, len(tt.wantIdx))
			for i, idx := range tt.wantIdx {
				assert.Equal(t, entity.PathActionBuild, got[i].Action)
				assert.Equal(t, tt.items[idx].Path.ID(), got[i].Path.ID())
			}
		})
	}
}

func TestSticky_HonorsCancelledContext(t *testing.T) {
	// The run's output is all-or-nothing: a partial action list would fund an
	// arbitrary prefix of the ranking, so cancellation yields no actions at all.
	items := []entity.CandidatePath{
		candidate("q/2", entity.DependencyAssumptionSucceeds, "q/1"),
		candidate("q/2", entity.DependencyAssumptionFails, "q/1"),
	}

	t.Run("cancelled before the first pull", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got, err := New(5).Allocate(ctx, nil, &fakeIter{items: items})
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)
	})

	t.Run("expired deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
		defer cancel()

		got, err := New(5).Allocate(ctx, nil, &fakeIter{items: items})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, got)
	})

	t.Run("iterator that ignores cancellation cannot spin the loop", func(t *testing.T) {
		// endlessIter never reports cancellation and never runs out, so the
		// allocator's own check is the only thing that stops the pull loop.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got, err := New(5).Allocate(ctx, nil, endlessIter{item: items[0]})
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)
	})
}

// endlessIter yields the same candidate forever and ignores ctx entirely.
type endlessIter struct{ item entity.CandidatePath }

func (e endlessIter) Next(context.Context) (entity.CandidatePath, bool, error) {
	return e.item, true, nil
}
