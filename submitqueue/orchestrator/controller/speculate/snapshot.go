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

// snapshot is one run's working state: the queue as it was read, plus the
// changes the run has decided on so far. The store is read once — two
// decisions in the same run can never disagree about the state of the world —
// and everything the run concludes is folded back in here before anything
// else is derived from it.
type snapshot struct {
	// batches is every batch the run can reason about, by ID: the queue's
	// in-flight batches plus any finalized batch still named as a dependency
	// of one of them.
	batches map[string]entity.Batch
	// speculating is the queue's Speculating batches, in queue order. These are
	// the heads open to new work: what the Speculator is handed, and what the
	// dispatch step walks.
	speculating []entity.Batch
	// pathSets is each head's in-memory path set, by head batch ID. Statuses
	// already reflect what each path's build actually did — see
	// (*Controller).updatePathsFromBuilds.
	pathSets map[string]entity.SpeculationPathSet
	// dirty marks heads whose in-memory set differs from what is stored.
	// Always touch it through markDirty / isDirty so every step of the
	// handshake is greppable; see those methods for the contract.
	dirty map[string]bool
}

// markDirty records that the head's in-memory path set has diverged from the
// stored copy and must be persisted before the run ends. Call it after every
// mutation of snap.pathSets[id].
func (s *snapshot) markDirty(id string) {
	s.dirty[id] = true
}

// isDirty reports whether the head's set still needs persisting. A head with
// no entry is not dirty, so the unguarded map read is deliberate.
func (s snapshot) isDirty(id string) bool {
	return s.dirty[id]
}

// batchState returns a batch's state, or BatchStateUnknown for a batch the
// run never read — which resolves no assumption either way.
func (s snapshot) batchState(id string) entity.BatchState {
	return s.batches[id].State
}

// assumptionBroken reports whether a finished dependency has already proven
// one of the path's assumptions wrong: a dependency the path assumed would
// succeed ended some other way, or one it assumed would fail succeeded. A
// dependency still in flight proves nothing either way, and an ignored one
// never does — the path made no claim about it.
func assumptionBroken(path entity.SpeculationPath, snap snapshot) bool {
	for _, dep := range path.Dependencies {
		state := snap.batchState(dep.Batch)
		if !state.IsTerminal() {
			continue
		}

		switch dep.Assumption {
		case entity.DependencyAssumptionSucceeds:
			if state != entity.BatchStateSucceeded {
				return true
			}
		case entity.DependencyAssumptionFails:
			if state == entity.BatchStateSucceeded {
				return true
			}
		}
	}
	return false
}
