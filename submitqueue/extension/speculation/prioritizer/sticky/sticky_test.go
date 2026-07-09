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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	limitfake "github.com/uber/submitqueue/submitqueue/extension/speculation/prioritizationlimit/fake"
)

func path(head string, base ...string) entity.SpeculationPath {
	return entity.SpeculationPath{Base: base, Head: head}
}

func TestPrioritize_AdmitsTopScoredSelectedIntoFreeSlots(t *testing.T) {
	low := entity.SpeculationPathInfo{ID: "p/low", Path: path("q/batch/1"), Status: entity.SpeculationPathStatusSelected, Score: 0.4}
	high := entity.SpeculationPathInfo{ID: "p/high", Path: path("q/batch/2"), Status: entity.SpeculationPathStatusSelected, Score: 0.9}
	mid := entity.SpeculationPathInfo{ID: "p/mid", Path: path("q/batch/3"), Status: entity.SpeculationPathStatusSelected, Score: 0.6}

	p := New(limitfake.New(2))
	got, err := p.Prioritize(context.Background(), []entity.SpeculationPathInfo{low, high, mid})
	require.NoError(t, err)
	assert.Equal(t, []entity.PathDecision{
		{PathID: high.ID, Action: entity.SpeculationPathActionPromote},
		{PathID: mid.ID, Action: entity.SpeculationPathActionPromote},
	}, got)
}

func TestPrioritize_RunningPathsCountAgainstBudget(t *testing.T) {
	building := entity.SpeculationPathInfo{ID: "p/building", Path: path("q/batch/1"), Status: entity.SpeculationPathStatusBuilding, Score: 0.99}
	prioritized := entity.SpeculationPathInfo{ID: "p/prioritized", Path: path("q/batch/2"), Status: entity.SpeculationPathStatusPrioritized, Score: 0.99}
	pending := entity.SpeculationPathInfo{ID: "p/pending", Path: path("q/batch/3"), Status: entity.SpeculationPathStatusSelected, Score: 0.5}

	// limit 3, 2 already running -> exactly 1 free slot for the pending candidate.
	p := New(limitfake.New(3))
	got, err := p.Prioritize(context.Background(), []entity.SpeculationPathInfo{building, prioritized, pending})
	require.NoError(t, err)
	assert.Equal(t, []entity.PathDecision{
		{PathID: pending.ID, Action: entity.SpeculationPathActionPromote},
	}, got)
}

func TestPrioritize_ZeroFreeYieldsNoDecisions(t *testing.T) {
	building := entity.SpeculationPathInfo{ID: "p/building", Path: path("q/batch/1"), Status: entity.SpeculationPathStatusBuilding, Score: 0.9}
	pending := entity.SpeculationPathInfo{ID: "p/pending", Path: path("q/batch/2"), Status: entity.SpeculationPathStatusSelected, Score: 0.9}

	p := New(limitfake.New(1))
	got, err := p.Prioritize(context.Background(), []entity.SpeculationPathInfo{building, pending})
	require.NoError(t, err)
	assert.Empty(t, got)

	// Also verify no Selected candidates at all yields no decisions.
	got, err = p.Prioritize(context.Background(), []entity.SpeculationPathInfo{building})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestPrioritize_TieBreakIsDeterministic(t *testing.T) {
	a := entity.SpeculationPathInfo{ID: "p/a", Path: path("q/batch/2"), Status: entity.SpeculationPathStatusSelected, Score: 0.5}
	b := entity.SpeculationPathInfo{ID: "p/b", Path: path("q/batch/1"), Status: entity.SpeculationPathStatusSelected, Score: 0.5}
	c := entity.SpeculationPathInfo{ID: "p/c", Path: path("q/batch/1", "q/batch/0"), Status: entity.SpeculationPathStatusSelected, Score: 0.5}

	p := New(limitfake.New(3))

	// Same score across all three: ties break on Head, then on Base. Running
	// this repeatedly with inputs in different orders must always produce the
	// same output order.
	for _, in := range [][]entity.SpeculationPathInfo{
		{a, b, c},
		{c, b, a},
		{b, c, a},
	} {
		got, err := p.Prioritize(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, []entity.PathDecision{
			{PathID: b.ID, Action: entity.SpeculationPathActionPromote}, // q/batch/1, base []
			{PathID: c.ID, Action: entity.SpeculationPathActionPromote}, // q/batch/1, base [q/batch/0]
			{PathID: a.ID, Action: entity.SpeculationPathActionPromote}, // q/batch/2
		}, got)
	}
}

func TestPrioritize_LimitErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	p := New(limitfake.New(3).FailWith(sentinel))
	_, err := p.Prioritize(context.Background(), []entity.SpeculationPathInfo{
		{ID: "p/1", Path: path("q/batch/1"), Status: entity.SpeculationPathStatusSelected, Score: 0.5},
	})
	require.ErrorIs(t, err, sentinel)
}
