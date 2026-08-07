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

package standard

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/submitqueue/submitqueue/entity"
	allocatormock "github.com/uber/submitqueue/submitqueue/extension/speculation/allocator/mock"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/allocator/sticky"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/generator/bestfirst"
	generatormock "github.com/uber/submitqueue/submitqueue/extension/speculation/generator/mock"
)

// assumptionFor returns what a path assumes about a given dependency batch.
func assumptionFor(p entity.SpeculationPath, dep string) entity.DependencyAssumption {
	for _, d := range p.Dependencies {
		if d.Batch == dep {
			return d.Assumption
		}
	}
	return entity.DependencyAssumptionUnknown
}

// constScorer is a minimal scorer.Scorer that scores every batch identically.
type constScorer struct{ v float64 }

func (c constScorer) Score(context.Context, entity.Batch) (float64, error) { return c.v, nil }

func TestComposed_EndToEnd_NaivePair(t *testing.T) {
	batches := []entity.Batch{
		{ID: "q/1", State: entity.BatchStateSpeculating},
		{ID: "q/2", State: entity.BatchStateSpeculating, Dependencies: []string{"q/1"}},
	}

	// bestfirst generator + sticky allocator with a 2-build budget.
	spec := New(bestfirst.New(constScorer{0.9}), sticky.New(2))
	got, err := spec.Speculate(context.Background(), batches, nil)
	require.NoError(t, err)

	// Two free budget slots -> two builds. Everything proposed is a build.
	require.Len(t, got, 2)
	var q2 entity.Speculation
	var found bool
	for _, s := range got {
		assert.Equal(t, entity.PathActionBuild, s.Action)
		if s.Path.Head == "q/2" {
			q2, found = s, true
		}
	}
	// q/2's funded path is the optimistic one: it assumes q/1 succeeds.
	require.True(t, found, "q/2 should have been funded")
	assert.Equal(t, entity.DependencyAssumptionSucceeds, assumptionFor(q2.Path, "q/1"))
}

func TestComposed_WiresGeneratorIntoAllocator(t *testing.T) {
	ctrl := gomock.NewController(t)

	batches := []entity.Batch{{ID: "q/1", State: entity.BatchStateSpeculating}}
	pathSets := []entity.SpeculationPathSet{{Head: "q/1"}}
	want := []entity.Speculation{{
		Path:   entity.SpeculationPath{Head: "q/1"},
		Action: entity.PathActionBuild,
	}}

	iter := generatormock.NewMockIterator(ctrl)
	gen := generatormock.NewMockGenerator(ctrl)
	alloc := allocatormock.NewMockAllocator(ctrl)

	gen.EXPECT().Generate(gomock.Any(), batches).Return(iter, nil)
	alloc.EXPECT().Allocate(gomock.Any(), pathSets, iter).Return(want, nil)

	got, err := New(gen, alloc).Speculate(context.Background(), batches, pathSets)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestComposed_PropagatesGeneratorError(t *testing.T) {
	ctrl := gomock.NewController(t)

	errGenerate := errors.New("generate boom")
	gen := generatormock.NewMockGenerator(ctrl)
	alloc := allocatormock.NewMockAllocator(ctrl)

	gen.EXPECT().Generate(gomock.Any(), gomock.Any()).Return(nil, errGenerate)
	// Allocate must not be called when Generate fails (no alloc.EXPECT()).

	_, err := New(gen, alloc).Speculate(context.Background(), nil, nil)
	require.ErrorIs(t, err, errGenerate)
}

func TestComposed_PropagatesContextCancellation(t *testing.T) {
	// standard adds no cancellation handling of its own — it inherits it from
	// the two parts. This pins that the seam actually propagates: a cancelled
	// run yields the context error and no actions.
	batches := []entity.Batch{
		{ID: "q/1", State: entity.BatchStateSpeculating},
		{ID: "q/2", State: entity.BatchStateSpeculating, Dependencies: []string{"q/1"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	spec := New(bestfirst.New(constScorer{0.9}), sticky.New(2))
	got, err := spec.Speculate(ctx, batches, nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, got)
}
