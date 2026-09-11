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

// passedPath returns a passed entry for a path over the given assumptions.
func passedPath(assumptions ...entity.DependencyAssumption) entity.SpeculationPathEntry {
	return entryFor(pathOver(assumptions...), entity.SpeculationPathStatusPassed)
}

func setOf(entries ...entity.SpeculationPathEntry) entity.SpeculationPathSet {
	return entity.SpeculationPathSet{Head: head, Paths: entries}
}

// The payoff case: a head lands as soon as the dependencies its passed build
// was stacked on have landed, without waiting for the ones it was built
// without or told to ignore.
func TestLandablePath(t *testing.T) {
	const (
		succeeds = entity.DependencyAssumptionSucceeds
		fails    = entity.DependencyAssumptionFails
	)

	tests := []struct {
		name       string
		assumption [2]entity.DependencyAssumption
		dep1State  entity.BatchState
		dep2State  entity.BatchState
		want       bool
	}{
		// dep2 is settled the way its assumption expects throughout, so dep1
		// is the only thing under test.
		{
			name:       "waits for an assumed-succeeding dependency to land",
			assumption: [2]entity.DependencyAssumption{succeeds, fails},
			dep1State:  entity.BatchStateSpeculating,
			dep2State:  entity.BatchStateFailed,
			want:       false,
		},
		{
			name:       "lands once it has",
			assumption: [2]entity.DependencyAssumption{succeeds, fails},
			dep1State:  entity.BatchStateSucceeded,
			dep2State:  entity.BatchStateFailed,
			want:       true,
		},
		{
			name:       "an assumed-succeeding dependency waits out its land",
			assumption: [2]entity.DependencyAssumption{succeeds, fails},
			dep1State:  entity.BatchStateLanding,
			dep2State:  entity.BatchStateFailed,
			want:       false,
		},
		{
			name:       "waits for an assumed-failing dependency to actually fail",
			assumption: [2]entity.DependencyAssumption{fails, fails},
			dep1State:  entity.BatchStateSpeculating,
			dep2State:  entity.BatchStateFailed,
			want:       false,
		},
		{
			name:       "still waits while that dependency is landing",
			assumption: [2]entity.DependencyAssumption{fails, fails},
			dep1State:  entity.BatchStateLanding,
			dep2State:  entity.BatchStateFailed,
			want:       false,
		},
		{
			name:       "still waits while that dependency is cancelling",
			assumption: [2]entity.DependencyAssumption{fails, fails},
			dep1State:  entity.BatchStateCancelling,
			dep2State:  entity.BatchStateFailed,
			want:       false,
		},
		{
			name:       "lands once it has failed",
			assumption: [2]entity.DependencyAssumption{fails, fails},
			dep1State:  entity.BatchStateFailed,
			dep2State:  entity.BatchStateFailed,
			want:       true,
		},
		{
			name:       "lands once it has been cancelled",
			assumption: [2]entity.DependencyAssumption{fails, fails},
			dep1State:  entity.BatchStateCancelled,
			dep2State:  entity.BatchStateFailed,
			want:       true,
		},
		{
			name:       "one unsettled dependency is enough to wait",
			assumption: [2]entity.DependencyAssumption{succeeds, succeeds},
			dep1State:  entity.BatchStateSucceeded,
			dep2State:  entity.BatchStateSpeculating,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dep2 := tt.dep2State
			if dep2 == "" {
				dep2 = entity.BatchStateSpeculating
			}
			set := setOf(passedPath(tt.assumption[0], tt.assumption[1]))
			_, ok := landablePath(set, snapWith(tt.dep1State, dep2))
			assert.Equal(t, tt.want, ok)
		})
	}
}

// A path that has not passed cannot carry the head out of the queue.
func TestLandablePath_IgnoresUnpassedPaths(t *testing.T) {
	for _, status := range []entity.SpeculationPathStatus{
		entity.SpeculationPathStatusPending,
		entity.SpeculationPathStatusBuilding,
		entity.SpeculationPathStatusFailed,
		entity.SpeculationPathStatusCancelled,
		entity.SpeculationPathStatusCancelling,
	} {
		t.Run(string(status), func(t *testing.T) {
			set := setOf(entryFor(
				pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds), status))
			_, ok := landablePath(set, snapWith(entity.BatchStateSucceeded, entity.BatchStateSucceeded))
			assert.False(t, ok)
		})
	}
}

// A passed build whose assumptions reality has since contradicted is not a
// licence to land — it verified a world that did not happen.
func TestLandablePath_ExcludesBrokenPassedPath(t *testing.T) {
	set := setOf(passedPath(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails))

	// The path was built without dep1, but dep1 landed after all.
	_, ok := landablePath(set, snapWith(entity.BatchStateSucceeded, entity.BatchStateSpeculating))
	assert.False(t, ok)
}

func TestBypassablePath(t *testing.T) {
	const (
		succeeds = entity.DependencyAssumptionSucceeds
		fails    = entity.DependencyAssumptionFails
	)
	allPaths := []entity.SpeculationPathEntry{
		passedPath(succeeds, succeeds),
		passedPath(succeeds, fails),
		passedPath(fails, succeeds),
		passedPath(fails, fails),
	}
	headBatch := entity.Batch{ID: head, Dependencies: []string{dep1, dep2}}

	tests := []struct {
		name      string
		head      entity.Batch
		set       entity.SpeculationPathSet
		dep1State entity.BatchState
		dep2State entity.BatchState
		want      bool
	}{
		{
			name:      "bypasses when every outcome of two unsettled dependencies passed",
			head:      headBatch,
			set:       setOf(allPaths...),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateLanding,
			want:      true,
		},
		{
			name:      "waits when one outcome is missing",
			head:      headBatch,
			set:       setOf(allPaths[:3]...),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSpeculating,
		},
		{
			name: "covers only the unsettled dependency when another has succeeded",
			head: headBatch,
			set: setOf(
				passedPath(succeeds, succeeds),
				passedPath(succeeds, fails),
			),
			dep1State: entity.BatchStateSucceeded,
			dep2State: entity.BatchStateSpeculating,
			want:      true,
		},
		{
			name: "does not count a path contradicted by a settled dependency",
			head: headBatch,
			set: setOf(
				passedPath(succeeds, succeeds),
				passedPath(fails, fails),
			),
			dep1State: entity.BatchStateSucceeded,
			dep2State: entity.BatchStateSpeculating,
		},
		{
			name: "does not count a duplicate outcome twice",
			head: headBatch,
			set: setOf(
				passedPath(succeeds, succeeds),
				passedPath(succeeds, succeeds),
				passedPath(fails, succeeds),
				passedPath(fails, fails),
			),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSpeculating,
		},
		{
			name: "does not count an unpassed outcome",
			head: headBatch,
			set: setOf(
				allPaths[0],
				allPaths[1],
				allPaths[2],
				entryFor(pathOver(fails, fails), entity.SpeculationPathStatusFailed),
			),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSpeculating,
		},
		{
			name: "does not count a malformed path",
			head: headBatch,
			set: setOf(
				allPaths[0],
				allPaths[1],
				allPaths[2],
				passedPath(fails),
			),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSpeculating,
		},
		{
			// A path stacked [dep2, dep1] built a different tree than [dep1, dep2]:
			// the base feeds the runner in path order (see build.loadBase). It
			// cannot stand in for the canonical combination its assumptions name.
			name: "does not count a reordered path",
			head: headBatch,
			set: setOf(
				passedPath(succeeds, succeeds),
				passedPath(succeeds, fails),
				passedPath(fails, succeeds),
				entryFor(entity.SpeculationPath{Head: head, Dependencies: []entity.PathDependency{
					{Batch: dep2, Assumption: fails},
					{Batch: dep1, Assumption: fails},
				}}, entity.SpeculationPathStatusPassed),
			),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSpeculating,
		},
		{
			name:      "leaves fully settled dependencies to strict land",
			head:      headBatch,
			set:       setOf(allPaths...),
			dep1State: entity.BatchStateSucceeded,
			dep2State: entity.BatchStateFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			winner, ok := bypassablePath(tt.head, tt.set, snapWith(tt.dep1State, tt.dep2State))
			assert.Equal(t, tt.want, ok)
			if tt.want {
				assert.NotEmpty(t, winner.ID)
			}
		})
	}
}

// livePassedPath is landablePath without the settled requirement, and the gap
// between the two is the head's waiting room: its own work is done and all that
// remains is other batches finishing. That window is reported to the members,
// so it has to be recognised while landablePath still says no.
func TestLivePassedPath(t *testing.T) {
	const (
		succeeds = entity.DependencyAssumptionSucceeds
		fails    = entity.DependencyAssumptionFails
	)

	tests := []struct {
		name      string
		set       entity.SpeculationPathSet
		dep1State entity.BatchState
		dep2State entity.BatchState
		want      bool
	}{
		{
			name:      "passed with an unsettled dependency is the waiting window",
			set:       setOf(passedPath(succeeds, succeeds)),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSucceeded,
			want:      true,
		},
		{
			name:      "passed and fully settled is still passed",
			set:       setOf(passedPath(succeeds, succeeds)),
			dep1State: entity.BatchStateSucceeded,
			dep2State: entity.BatchStateSucceeded,
			want:      true,
		},
		{
			name: "a build still running is not passed",
			set: setOf(entryFor(
				pathOver(succeeds, succeeds), entity.SpeculationPathStatusBuilding)),
			dep1State: entity.BatchStateSpeculating,
			dep2State: entity.BatchStateSucceeded,
			want:      false,
		},
		{
			// The head is back to building, which is why losing this is worth
			// reporting: the members would otherwise read "speculated" through
			// the whole rebuild.
			name:      "a contradicted assumption takes the path back out",
			set:       setOf(passedPath(fails, fails)),
			dep1State: entity.BatchStateSucceeded,
			dep2State: entity.BatchStateSpeculating,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := livePassedPath(tt.set, snapWith(tt.dep1State, tt.dep2State))
			assert.Equal(t, tt.want, ok)
		})
	}
}

func TestHasNoViableFuture(t *testing.T) {
	headBatch := entity.Batch{ID: head, Dependencies: []string{dep1, dep2}}
	// With every dependency resolved exactly one assumption pair is unbroken,
	// so every live path here has that shape and they differ by ID.
	failed := entryFor(
		pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds),
		entity.SpeculationPathStatusFailed)

	t.Run("waits while a dependency is unresolved", func(t *testing.T) {
		// An unresolved dependency means futures the queue has not tried yet.
		snap := snapWith(entity.BatchStateSucceeded, entity.BatchStateSpeculating)
		assert.False(t, hasNoViableFuture(headBatch, setOf(failed), snap))
	})

	t.Run("fails once everything is resolved and every live path failed", func(t *testing.T) {
		snap := snapWith(entity.BatchStateSucceeded, entity.BatchStateSucceeded)
		assert.True(t, hasNoViableFuture(headBatch, setOf(failed), snap))
	})

	t.Run("does not fail while a path is still running", func(t *testing.T) {
		running := entryFor(
			pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds),
			entity.SpeculationPathStatusBuilding)
		running.ID = "still-running"
		snap := snapWith(entity.BatchStateSucceeded, entity.BatchStateSucceeded)
		assert.False(t, hasNoViableFuture(headBatch, setOf(failed, running), snap))
	})

	t.Run("does not fail a head with nothing funded", func(t *testing.T) {
		snap := snapWith(entity.BatchStateSucceeded, entity.BatchStateSucceeded)
		assert.False(t, hasNoViableFuture(headBatch, setOf(), snap),
			"an unfunded head is waiting for the Speculator, not out of options")
	})

	t.Run("ignores broken paths when judging", func(t *testing.T) {
		// This path assumed dep1 would fail; it succeeded, so the failed build
		// tells us nothing about a future that can still happen.
		brokenFail := entryFor(
			pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionSucceeds),
			entity.SpeculationPathStatusFailed)
		snap := snapWith(entity.BatchStateSucceeded, entity.BatchStateSucceeded)
		assert.False(t, hasNoViableFuture(headBatch, setOf(brokenFail), snap))
	})
}

func TestDecide(t *testing.T) {
	headBatch := entity.Batch{ID: head, Dependencies: []string{dep1, dep2}}
	allResolved := snapWith(entity.BatchStateSucceeded, entity.BatchStateSucceeded)

	passed := passedPath(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds)
	// Only one assumption pair is unbroken here, so the live failed path has
	// the same shape as the passed one and the ID separates them in a set.
	failed := entryFor(
		pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds),
		entity.SpeculationPathStatusFailed)
	failed.ID = "failed-attempt"
	building := entryFor(
		pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails),
		entity.SpeculationPathStatusBuilding)

	assert.Equal(t, outcomeLand, decide(headBatch, setOf(passed), allResolved))
	assert.Equal(t, outcomeFail, decide(headBatch, setOf(failed), allResolved))
	assert.Equal(t, outcomeWait, decide(headBatch, setOf(building), allResolved))

	// A passed path wins over a failed sibling: one way through is enough.
	assert.Equal(t, outcomeLand, decide(headBatch, setOf(failed, passed), allResolved))

	allUnresolved := snapWith(entity.BatchStateSpeculating, entity.BatchStateSpeculating)
	fullCoverage := setOf(
		passedPath(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds),
		passedPath(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails),
		passedPath(entity.DependencyAssumptionFails, entity.DependencyAssumptionSucceeds),
		passedPath(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails),
	)
	assert.Equal(t, outcomeLand, decide(headBatch, fullCoverage, allUnresolved))
}

// Once a path has passed, its siblings cannot help the head but are still
// holding CI slots the rest of the queue could use.
func TestSupersede(t *testing.T) {
	winner := passedPath(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds)
	sibling := entryFor(
		pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionSucceeds),
		entity.SpeculationPathStatusBuilding)
	finished := entryFor(
		pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails),
		entity.SpeculationPathStatusFailed)

	set := setOf(winner, sibling, finished)
	assert.True(t, supersede(&set, winner.ID, 99))

	assert.Equal(t, entity.SpeculationPathStatusPassed, set.Paths[0].Status, "the winner is untouched")
	assert.Equal(t, entity.SpeculationPathStatusCancelling, set.Paths[1].Status)
	assert.Equal(t, int64(99), set.Paths[1].UpdatedAtMs)
	assert.Equal(t, entity.SpeculationPathStatusFailed, set.Paths[2].Status, "a finished path is left alone")

	assert.False(t, supersede(&set, winner.ID, 100), "a second pass changes nothing")
}

func TestAllPathsStopped(t *testing.T) {
	running := entryFor(pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds),
		entity.SpeculationPathStatusBuilding)
	cancelling := entryFor(pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails),
		entity.SpeculationPathStatusCancelling)
	done := entryFor(pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails),
		entity.SpeculationPathStatusCancelled)

	assert.True(t, allPathsStopped(setOf(done)))
	assert.True(t, allPathsStopped(setOf()))
	assert.False(t, allPathsStopped(setOf(running)))
	assert.False(t, allPathsStopped(setOf(cancelling)),
		"a cancelling build holds its CI slot until it actually stops")
}

func TestCancelAllPaths(t *testing.T) {
	running := entryFor(pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds),
		entity.SpeculationPathStatusBuilding)
	done := entryFor(pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails),
		entity.SpeculationPathStatusPassed)

	set := setOf(running, done)
	assert.True(t, cancelAllPaths(&set, 7))
	assert.Equal(t, entity.SpeculationPathStatusCancelling, set.Paths[0].Status)
	assert.Equal(t, entity.SpeculationPathStatusPassed, set.Paths[1].Status,
		"a finished build has no slot to release")

	assert.False(t, cancelAllPaths(&set, 8), "a second pass changes nothing")
}
