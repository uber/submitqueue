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

// Package speculation holds pure speculation-domain rules that more than one
// orchestrator stage must evaluate identically. It interprets entity state but
// owns none: no storage, no queues, no pipeline step — just functions of
// entities in, verdicts out. Rules consumed by a single stage do not belong
// here; they stay package-private in that stage until a second consumer
// appears.
package speculation

import "github.com/uber/submitqueue/submitqueue/entity"

// PathMergeConfirmed reports whether p's merge assumptions are fully
// confirmed by dependency outcomes: p's own build Passed, every base
// dependency either landed (entity.BatchStateSucceeded) or was cancelled out
// of the way (entity.BatchStateCancelled), and every dependency of the head
// outside the base has been ruled out (entity.BatchStateFailed or
// entity.BatchStateCancelled). depByID holds the head's dependency batches
// keyed by batch ID; a base entry absent from the map is treated as out of
// the way.
func PathMergeConfirmed(p entity.SpeculationPathInfo, depByID map[string]entity.Batch) bool {
	return pathMergeReady(p, depByID, false)
}

// PathMergePossible reports whether p's merge assumptions are confirmed or
// may still confirm: identical to PathMergeConfirmed, except a base
// dependency still in entity.BatchStateMerging or entity.BatchStateCancelling
// is tolerated. Both are transient and always settle to a terminal state that
// either confirms the assumption (Succeeded or Cancelled) or refutes it
// (Failed), so neither is a verdict yet. A path that is not possible can
// never merge. A dependency outside the base gets no such tolerance in
// either predicate: a still-undecided non-base dependency may yet land and
// invalidate the assumption set, so it must be ruled out, not bet on.
func PathMergePossible(p entity.SpeculationPathInfo, depByID map[string]entity.Batch) bool {
	return pathMergeReady(p, depByID, true)
}

// pathMergeReady is the shared evaluation behind PathMergeConfirmed and
// PathMergePossible; tolerateUnsettledBase is the only difference between
// the two.
func pathMergeReady(p entity.SpeculationPathInfo, depByID map[string]entity.Batch, tolerateUnsettledBase bool) bool {
	if p.Status != entity.SpeculationPathStatusPassed {
		return false
	}

	inBase := make(map[string]bool, len(p.Path.Base))
	for _, id := range p.Path.Base {
		inBase[id] = true
		d, ok := depByID[id]
		if !ok {
			continue
		}
		switch d.State {
		case entity.BatchStateSucceeded, entity.BatchStateCancelled:
		case entity.BatchStateMerging, entity.BatchStateCancelling:
			if !tolerateUnsettledBase {
				return false
			}
		default:
			return false
		}
	}
	for id, d := range depByID {
		if inBase[id] {
			continue
		}
		if d.State != entity.BatchStateFailed && d.State != entity.BatchStateCancelled {
			return false
		}
	}
	return true
}
