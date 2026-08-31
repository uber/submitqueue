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

// bypassablePath returns a passed path once passed builds cover every possible
// outcome of the head's unsettled dependencies. Settled dependencies stay
// pinned to reality through assumptionBroken.
//
// A path's combination is read positionally — the i-th assumption belongs to
// the head's i-th dependency — which isWellFormed's order check is what
// licenses: two paths with the same assumptions in different orders are
// different stacks, not the same outcome.
func bypassablePath(head entity.Batch, set entity.SpeculationPathSet, snap snapshot) (entity.SpeculationPathEntry, bool) {
	unsettled := unsettledDependencyIndices(head, snap)
	if len(unsettled) == 0 {
		return entity.SpeculationPathEntry{}, false
	}

	required := 1
	for range unsettled {
		if required > len(set.Paths)/2 {
			return entity.SpeculationPathEntry{}, false
		}
		required *= 2
	}

	seen := make(map[string]struct{}, required)
	var winner entity.SpeculationPathEntry
	for _, entry := range set.Paths {
		if entry.Status != entity.SpeculationPathStatusPassed ||
			assumptionBroken(entry.Path, snap) ||
			!isWellFormed(entry.Path, head) {
			continue
		}

		signature := make([]byte, len(unsettled))
		for i, depIndex := range unsettled {
			if entry.Path.Dependencies[depIndex].Assumption == entity.DependencyAssumptionFails {
				signature[i] = 'f'
			} else {
				signature[i] = 's'
			}
		}
		key := string(signature)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if len(seen) == 1 {
			winner = entry
		}
	}

	return winner, len(seen) == required
}

func unsettledDependencyIndices(head entity.Batch, snap snapshot) []int {
	var indices []int
	for i, depID := range head.Dependencies {
		if !snap.batchState(depID).IsTerminal() {
			indices = append(indices, i)
		}
	}
	return indices
}
