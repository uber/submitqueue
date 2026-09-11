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
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// checkSnapshot builds a snapshot with a speculating head over dep1 and dep2,
// both unresolved, and the head's path set seeded with the given entries.
func checkSnapshot(headState entity.BatchState, entries ...entity.SpeculationPathEntry) snapshot {
	snap := snapshot{
		batches: map[string]entity.Batch{
			head: {ID: head, State: headState, Dependencies: []string{dep1, dep2}},
			dep1: {ID: dep1, State: entity.BatchStateSpeculating},
			dep2: {ID: dep2, State: entity.BatchStateSpeculating},
		},
		pathSets: map[string]entity.SpeculationPathSet{},
	}
	if len(entries) > 0 {
		snap.pathSets[head] = entity.SpeculationPathSet{Head: head, Paths: entries}
	}
	return snap
}

func entryFor(path entity.SpeculationPath, status entity.SpeculationPathStatus) entity.SpeculationPathEntry {
	return entity.SpeculationPathEntry{ID: path.ID(), Path: path, Status: status, Attempt: 1}
}

// A well-formed build proposal on a speculating head survives.
func TestFilterProposals_KeepsValidProposals(t *testing.T) {
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	snap := checkSnapshot(entity.BatchStateSpeculating)

	kept, rejected := filterProposals([]entity.Speculation{
		{Path: path, Action: entity.PathActionBuild},
	}, snap)

	assert.Empty(t, rejected)
	require.Len(t, kept, 1)
	assert.Equal(t, path.ID(), kept[0].Path.ID())
}

func TestFilterProposals_Rejects(t *testing.T) {
	valid := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)

	tests := []struct {
		name     string
		proposal entity.Speculation
		snap     snapshot
		want     rejection
	}{
		{
			name:     "zero-value action",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionUnknown},
			snap:     checkSnapshot(entity.BatchStateSpeculating),
			want:     rejectUnknownAction,
		},
		{
			name: "head this run never read",
			proposal: entity.Speculation{
				Path:   entity.SpeculationPath{Head: "q/batch/ghost"},
				Action: entity.PathActionBuild,
			},
			snap: checkSnapshot(entity.BatchStateSpeculating),
			want: rejectUnknownHead,
		},
		{
			name:     "head already landing",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionBuild},
			snap:     checkSnapshot(entity.BatchStateLanding),
			want:     rejectHeadNotSpeculating,
		},
		{
			name:     "head already cancelling",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionBuild},
			snap:     checkSnapshot(entity.BatchStateCancelling),
			want:     rejectHeadNotSpeculating,
		},
		{
			name: "path missing one of the head's dependencies",
			proposal: entity.Speculation{
				Path:   pathOver(entity.DependencyAssumptionSucceeds),
				Action: entity.PathActionBuild,
			},
			snap: checkSnapshot(entity.BatchStateSpeculating),
			want: rejectMalformedPath,
		},
		{
			name: "path with dependencies out of queue order",
			proposal: entity.Speculation{
				Path: entity.SpeculationPath{Head: head, Dependencies: []entity.PathDependency{
					{Batch: dep2, Assumption: entity.DependencyAssumptionFails},
					{Batch: dep1, Assumption: entity.DependencyAssumptionSucceeds},
				}},
				Action: entity.PathActionBuild,
			},
			snap: checkSnapshot(entity.BatchStateSpeculating),
			want: rejectMalformedPath,
		},
		{
			name:     "cancel on a path that is not stored",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionCancel},
			snap:     checkSnapshot(entity.BatchStateSpeculating),
			want:     rejectCancelNotInFlight,
		},
		{
			name:     "cancel on a path whose build already finished",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionCancel},
			snap: checkSnapshot(entity.BatchStateSpeculating,
				entryFor(valid, entity.SpeculationPathStatusFailed)),
			want: rejectCancelNotInFlight,
		},
		{
			name:     "cancel would discard a passed build",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionCancel},
			snap: checkSnapshot(entity.BatchStateSpeculating,
				entryFor(valid, entity.SpeculationPathStatusPassed)),
			want: rejectCancelPassed,
		},
		{
			name:     "build would resurrect a finished path",
			proposal: entity.Speculation{Path: valid, Action: entity.PathActionBuild},
			snap: checkSnapshot(entity.BatchStateSpeculating,
				entryFor(valid, entity.SpeculationPathStatusPassed)),
			want: rejectPathTerminal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, rejected := filterProposals([]entity.Speculation{tt.proposal}, tt.snap)
			assert.Empty(t, kept)
			require.Len(t, rejected, 1)
			assert.Equal(t, tt.want, rejected[0])
		})
	}
}

// A path a resolved dependency has already ruled out must not be funded, even
// if the Speculator proposes it.
func TestFilterProposals_RejectsBrokenPath(t *testing.T) {
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)

	snap := checkSnapshot(entity.BatchStateSpeculating)
	snap.batches[dep1] = entity.Batch{ID: dep1, State: entity.BatchStateFailed}

	kept, rejected := filterProposals([]entity.Speculation{
		{Path: path, Action: entity.PathActionBuild},
	}, snap)

	assert.Empty(t, kept)
	require.Len(t, rejected, 1)
	assert.Equal(t, rejectBrokenAssumption, rejected[0])
}

// Cancelling a path that is still running is the Speculator's one legitimate
// cancel, so it must survive.
func TestFilterProposals_KeepsCancelOfRunningPath(t *testing.T) {
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)

	for _, status := range []entity.SpeculationPathStatus{
		entity.SpeculationPathStatusPending,
		entity.SpeculationPathStatusBuilding,
	} {
		t.Run(string(status), func(t *testing.T) {
			snap := checkSnapshot(entity.BatchStateSpeculating, entryFor(path, status))

			kept, rejected := filterProposals([]entity.Speculation{
				{Path: path, Action: entity.PathActionCancel},
			}, snap)

			assert.Empty(t, rejected)
			assert.Len(t, kept, 1)
		})
	}
}

// One bad proposal must not discard the good ones alongside it.
func TestFilterProposals_FiltersIndependently(t *testing.T) {
	good := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	bad := pathOver(entity.DependencyAssumptionSucceeds)

	kept, rejected := filterProposals([]entity.Speculation{
		{Path: bad, Action: entity.PathActionBuild},
		{Path: good, Action: entity.PathActionBuild},
	}, checkSnapshot(entity.BatchStateSpeculating))

	require.Len(t, kept, 1)
	assert.Equal(t, good.ID(), kept[0].Path.ID())
	assert.Equal(t, []rejection{rejectMalformedPath}, rejected)
}
func TestIsWellFormed(t *testing.T) {
	headBatch := entity.Batch{ID: head, Dependencies: []string{dep1, dep2}}

	tests := []struct {
		name string
		path entity.SpeculationPath
		want bool
	}{
		{
			name: "one assumption per dependency",
			path: pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails),
			want: true,
		},
		{
			name: "dependencies out of queue order",
			path: entity.SpeculationPath{Head: head, Dependencies: []entity.PathDependency{
				{Batch: dep2, Assumption: entity.DependencyAssumptionSucceeds},
				{Batch: dep1, Assumption: entity.DependencyAssumptionFails},
			}},
			want: false,
		},
		{
			name: "missing a dependency",
			path: pathOver(entity.DependencyAssumptionSucceeds),
			want: false,
		},
		{
			name: "duplicate dependency in place of another",
			path: entity.SpeculationPath{Head: head, Dependencies: []entity.PathDependency{
				{Batch: dep1, Assumption: entity.DependencyAssumptionSucceeds},
				{Batch: dep1, Assumption: entity.DependencyAssumptionFails},
			}},
			want: false,
		},
		{
			name: "dependency the head does not have",
			path: entity.SpeculationPath{Head: head, Dependencies: []entity.PathDependency{
				{Batch: dep1, Assumption: entity.DependencyAssumptionSucceeds},
				{Batch: "q/batch/stranger", Assumption: entity.DependencyAssumptionFails},
			}},
			want: false,
		},
		{
			name: "unknown assumption value",
			path: pathOver(entity.DependencyAssumptionUnknown, entity.DependencyAssumptionFails),
			want: false,
		},
		{
			name: "wrong head",
			path: entity.SpeculationPath{Head: "q/batch/other", Dependencies: []entity.PathDependency{
				{Batch: dep1, Assumption: entity.DependencyAssumptionSucceeds},
				{Batch: dep2, Assumption: entity.DependencyAssumptionFails},
			}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isWellFormed(tt.path, headBatch))
		})
	}
}
