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

package speculation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// PathMergeConfirmed and PathMergePossible differ in exactly one input: a
// base dependency in BatchStateMerging. Table-test the split and the shared
// rules (Passed required, base landed-or-cancelled, non-base ruled out,
// missing base entries tolerated).
func TestPathMergeConfirmedAndPossible(t *testing.T) {
	base := "q/batch/1"
	head := "q/batch/2"
	basePath := entity.SpeculationPath{Base: []string{base}, Head: head}
	alonePath := entity.SpeculationPath{Head: head}

	tests := []struct {
		name          string
		path          entity.SpeculationPath
		status        entity.SpeculationPathStatus
		deps          map[string]entity.Batch
		wantConfirmed bool
		wantPossible  bool
	}{
		{
			name:          "base_succeeded_confirmed",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateSucceeded}},
			wantConfirmed: true,
			wantPossible:  true,
		},
		{
			name:          "base_cancelled_confirmed",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateCancelled}},
			wantConfirmed: true,
			wantPossible:  true,
		},
		{
			name:          "base_merging_possible_not_confirmed",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateMerging}},
			wantConfirmed: false,
			wantPossible:  true,
		},
		{
			// Cancelling is transient: it settles to Cancelled or Succeeded
			// (both confirming) or Failed (refuting), so it is a wait, not
			// a verdict.
			name:          "base_cancelling_possible_not_confirmed",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateCancelling}},
			wantConfirmed: false,
			wantPossible:  true,
		},
		{
			name:          "base_failed_neither",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateFailed}},
			wantConfirmed: false,
			wantPossible:  false,
		},
		{
			name:          "base_speculating_neither",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateSpeculating}},
			wantConfirmed: false,
			wantPossible:  false,
		},
		{
			name:          "base_absent_from_deps_tolerated",
			path:          basePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{},
			wantConfirmed: true,
			wantPossible:  true,
		},
		{
			name:          "non_base_failed_ruled_out",
			path:          alonePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateFailed}},
			wantConfirmed: true,
			wantPossible:  true,
		},
		{
			name:   "non_base_merging_blocks_both",
			path:   alonePath,
			status: entity.SpeculationPathStatusPassed,
			// No Merging tolerance outside the base: the dependency may yet
			// land and invalidate the assumption set.
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateMerging}},
			wantConfirmed: false,
			wantPossible:  false,
		},
		{
			name:          "non_base_succeeded_blocks_both",
			path:          alonePath,
			status:        entity.SpeculationPathStatusPassed,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateSucceeded}},
			wantConfirmed: false,
			wantPossible:  false,
		},
		{
			name:          "unpassed_path_blocks_both",
			path:          basePath,
			status:        entity.SpeculationPathStatusBuilding,
			deps:          map[string]entity.Batch{base: {ID: base, State: entity.BatchStateSucceeded}},
			wantConfirmed: false,
			wantPossible:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := entity.SpeculationPathInfo{Path: tt.path, Status: tt.status}
			assert.Equal(t, tt.wantConfirmed, PathMergeConfirmed(info, tt.deps))
			assert.Equal(t, tt.wantPossible, PathMergePossible(info, tt.deps))
		})
	}
}
