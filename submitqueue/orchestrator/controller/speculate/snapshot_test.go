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

package speculate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/submitqueue/entity"
)

const (
	head = "q/batch/head"
	dep1 = "q/batch/dep1"
	dep2 = "q/batch/dep2"
)

// pathOver builds a path over the given assumptions, in dep1, dep2 order.
func pathOver(assumptions ...entity.DependencyAssumption) entity.SpeculationPath {
	deps := []string{dep1, dep2}
	p := entity.SpeculationPath{Head: head}
	for i, a := range assumptions {
		p.Dependencies = append(p.Dependencies, entity.PathDependency{Batch: deps[i], Assumption: a})
	}
	return p
}

// snapWith builds a snapshot where dep1 and dep2 are in the given states.
func snapWith(dep1State, dep2State entity.BatchState) snapshot {
	return snapshot{
		batches: map[string]entity.Batch{
			head: {ID: head, State: entity.BatchStateSpeculating, Dependencies: []string{dep1, dep2}},
			dep1: {ID: dep1, State: dep1State},
			dep2: {ID: dep2, State: dep2State},
		},
		pathSets: map[string]entity.SpeculationPathSet{},
	}
}

func TestAssumptionBroken(t *testing.T) {
	const (
		running   = entity.BatchStateSpeculating
		succeeded = entity.BatchStateSucceeded
		failed    = entity.BatchStateFailed
		cancelled = entity.BatchStateCancelled
	)
	const (
		succeeds = entity.DependencyAssumptionSucceeds
		fails    = entity.DependencyAssumptionFails
	)

	tests := []struct {
		name       string
		assumption entity.DependencyAssumption
		depState   entity.BatchState
		want       bool
	}{
		{"succeeds holds while unresolved", succeeds, running, false},
		{"succeeds holds when it succeeds", succeeds, succeeded, false},
		{"succeeds broken when it fails", succeeds, failed, true},
		{"succeeds broken when it is cancelled", succeeds, cancelled, true},

		{"fails holds while unresolved", fails, running, false},
		{"fails holds when it fails", fails, failed, false},
		{"fails holds when it is cancelled", fails, cancelled, false},
		{"fails broken when it succeeds", fails, succeeded, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := pathOver(tt.assumption, entity.DependencyAssumptionFails)
			assert.Equal(t, tt.want, assumptionBroken(path, snapWith(tt.depState, entity.BatchStateSpeculating)))
		})
	}
}

// A dependency the run never read resolves nothing: the path is still a live
// guess rather than a broken one.
func TestAssumptionBroken_UnknownDependency(t *testing.T) {
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	snap := snapshot{batches: map[string]entity.Batch{}, pathSets: map[string]entity.SpeculationPathSet{}}

	assert.False(t, assumptionBroken(path, snap))
}
