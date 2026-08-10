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

import "github.com/uber/submitqueue/submitqueue/entity"

// outcome is what a run has concluded about one batch. It is computed from
// the snapshot alone — decide is a pure function of the facts — which is what
// guarantees a swapped-in Speculator can change which paths run but never a
// batch's outcome.
type outcome string

const (
	// outcomeWait means the batch's outcome is not decided yet.
	outcomeWait outcome = "wait"
	// outcomeMerge means a passed path's assumptions have all come true, so
	// the head can be handed to the merge stage.
	outcomeMerge outcome = "merge"
	// outcomeFail means no future remains in which the head could pass.
	outcomeFail outcome = "fail"
	// outcomeCancel means a batch the user asked to cancel has had every path
	// stop, so nothing of it is still running.
	outcomeCancel outcome = "cancel"
)

// terminalState returns the batch state an outcome writes, and whether the
// outcome leaves the batch terminal. Merge is the odd one out: it hands the
// batch to the merge stage, which owns the terminal write that follows.
func (v outcome) terminalState() (entity.BatchState, bool) {
	switch v {
	case outcomeFail:
		return entity.BatchStateFailed, true
	case outcomeCancel:
		return entity.BatchStateCancelled, true
	default:
		return entity.BatchStateUnknown, false
	}
}

// decide returns the run's outcome on one open head, from the snapshot alone.
func decide(head entity.Batch, set entity.SpeculationPathSet, snap snapshot) outcome {
	if _, ok := mergeablePath(set, snap); ok {
		return outcomeMerge
	}
	if hasNoViableFuture(head, set, snap) {
		return outcomeFail
	}
	return outcomeWait
}

// mergeablePath returns a passed path whose merge preconditions are met: every
// guess it made about a dependency has been borne out by that dependency's
// actual state.
//
// This is what makes speculation pay — not by shortening the list the head
// waits on, but by having already done the work. The build ran against the
// guess while the dependencies were still resolving, so when they land the way
// the path assumed there is nothing left to run and the head merges at once.
//
// A guess that has not been settled yet is not a licence to merge, whichever
// way it points. A path that assumed a dependency would fail was built without
// that dependency's changes, so landing it while the dependency is still live
// puts a combination on the trunk that no build ever validated — which is the
// one thing the queue exists to prevent. The dependency merging is not enough
// either: a merge can fail, so "on its way in" is still an open question, and
// the head waits for the answer.
func mergeablePath(set entity.SpeculationPathSet, snap snapshot) (entity.SpeculationPathEntry, bool) {
	for _, entry := range set.Paths {
		if entry.Status != entity.SpeculationPathStatusPassed {
			continue
		}
		if assumptionBroken(entry.Path, snap) {
			continue
		}
		if allAssumptionsSettled(entry.Path, snap) {
			return entry, true
		}
	}
	return entity.SpeculationPathEntry{}, false
}

// allAssumptionsSettled reports whether every dependency has finished the way
// the path assumed: one it assumed would succeed has reached Succeeded, and one
// it assumed would fail has finished some other way.
func allAssumptionsSettled(path entity.SpeculationPath, snap snapshot) bool {
	for _, dep := range path.Dependencies {
		state := snap.batchState(dep.Batch)
		switch dep.Assumption {
		case entity.DependencyAssumptionSucceeds:
			if state != entity.BatchStateSucceeded {
				return false
			}
		case entity.DependencyAssumptionFails:
			if state != entity.BatchStateFailed && state != entity.BatchStateCancelled {
				return false
			}
		}
	}
	return true
}

// hasNoViableFuture reports whether the head can never pass: every dependency
// has resolved, and every path consistent with how they resolved has a failed
// build.
//
// Deliberately conservative — failing a batch that could still have landed is
// far worse than failing it a tick later, so this errs toward waiting:
//
//   - While any dependency is unresolved, an untried future may still exist,
//     so the answer is no.
//   - A head with no unbroken paths at all is not failed either: it simply
//     has nothing funded yet, and the next run's Speculator will propose
//     something.
func hasNoViableFuture(head entity.Batch, set entity.SpeculationPathSet, snap snapshot) bool {
	for _, depID := range head.Dependencies {
		if !snap.batchState(depID).IsTerminal() {
			return false
		}
	}

	live := 0
	for _, entry := range set.Paths {
		if assumptionBroken(entry.Path, snap) {
			continue
		}
		live++
		if entry.Status != entity.SpeculationPathStatusFailed {
			return false
		}
	}

	return live > 0
}
