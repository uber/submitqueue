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

// rejection is why one proposed action was dropped. The reasons are the metric
// dimension for a misbehaving Speculator, so each names a distinct fault.
type rejection string

const (
	// rejectUnknownAction is a zero-value or unrecognized action.
	rejectUnknownAction rejection = "unknown_action"
	// rejectUnknownHead names a batch this run never read.
	rejectUnknownHead rejection = "unknown_head"
	// rejectHeadNotSpeculating targets a batch that is not open to new work.
	rejectHeadNotSpeculating rejection = "head_not_speculating"
	// rejectMalformedPath is a path whose assumptions do not line up with its
	// head's dependency list: one missing or extra, a duplicate, a wrong head,
	// or a made-up assumption value.
	rejectMalformedPath rejection = "malformed_path"
	// rejectBrokenAssumption is a path with an assumption a finished
	// dependency has already proven wrong.
	rejectBrokenAssumption rejection = "broken_assumption"
	// rejectPathTerminal would rebuild a path whose build already finished.
	rejectPathTerminal rejection = "path_terminal"
	// rejectCancelNotInFlight cancels a path that is not running.
	rejectCancelNotInFlight rejection = "cancel_not_in_flight"
	// rejectCancelPassed would throw away a build that already passed.
	rejectCancelPassed rejection = "cancel_passed"
)

// filterProposals narrows a Speculator's proposals down to the ones the
// controller is willing to enact, returning the survivors and a reason for
// each drop.
//
// The Speculator is an extension, so its output is untrusted input: it decides
// which paths run, never whether a batch merges or fails. Every rule here
// protects an invariant the extension could otherwise break — acting on a batch
// that is finalizing, resurrecting a path a resolved dependency has ruled out,
// or discarding a passed build the queue is about to merge on. A proposal that
// trips one of these is a bug in the Speculator, not a normal outcome, which is
// why the caller counts them.
func filterProposals(proposals []entity.Speculation, snap snapshot) ([]entity.Speculation, []rejection) {
	var kept []entity.Speculation
	var rejected []rejection

	for _, proposal := range proposals {
		if reason, ok := rejectionReason(proposal, snap); ok {
			rejected = append(rejected, reason)
			continue
		}
		kept = append(kept, proposal)
	}

	return kept, rejected
}

// rejectionReason reports why a proposal cannot be enacted, or ok=false if
// it can.
func rejectionReason(proposal entity.Speculation, snap snapshot) (rejection, bool) {
	switch proposal.Action {
	case entity.PathActionBuild, entity.PathActionCancel:
	default:
		return rejectUnknownAction, true
	}

	head, ok := snap.batches[proposal.Path.Head]
	if !ok {
		return rejectUnknownHead, true
	}
	// Only a speculating head is open to new work. Batches in every other
	// state are facts the Speculator may reason from, never action targets.
	if head.State != entity.BatchStateSpeculating {
		return rejectHeadNotSpeculating, true
	}
	if !isWellFormed(proposal.Path, head) {
		return rejectMalformedPath, true
	}
	if assumptionBroken(proposal.Path, snap) {
		return rejectBrokenAssumption, true
	}

	entry, stored := findPath(snap.pathSets[head.ID], proposal.Path.ID())

	if proposal.Action == entity.PathActionCancel {
		// Cancelling is only meaningful for a path that is actually running,
		// and never for one that passed: that build is the head's way out of
		// the queue, and the budget it holds is already spent.
		if !stored {
			return rejectCancelNotInFlight, true
		}
		if entry.Status == entity.SpeculationPathStatusPassed {
			return rejectCancelPassed, true
		}
		if entry.Status.IsTerminal() {
			return rejectCancelNotInFlight, true
		}
		return "", false
	}

	// A build proposal for a path whose build already finished would discard a
	// recorded result and start the same work again.
	if stored && entry.Status.IsTerminal() {
		return rejectPathTerminal, true
	}

	return "", false
}

// isWellFormed reports whether a path is a proper guess about its head:
// exactly one assumption for each of the head's dependencies, no more and no
// fewer, and every assumption a real value.
//
// A malformed path is not merely suboptimal, it is unmergeable — the merge
// preconditions are read off the path's assumptions (see mergeablePath), so a
// path missing a dependency would let its head merge without waiting for it.
func isWellFormed(path entity.SpeculationPath, head entity.Batch) bool {
	if path.Head != head.ID {
		return false
	}
	if len(path.Dependencies) != len(head.Dependencies) {
		return false
	}

	required := make(map[string]struct{}, len(head.Dependencies))
	for _, dep := range head.Dependencies {
		required[dep] = struct{}{}
	}

	for _, dep := range path.Dependencies {
		if _, ok := required[dep.Batch]; !ok {
			return false
		}
		delete(required, dep.Batch)

		switch dep.Assumption {
		case entity.DependencyAssumptionSucceeds,
			entity.DependencyAssumptionFails,
			entity.DependencyAssumptionIgnored:
		default:
			return false
		}
	}

	return len(required) == 0
}

// findPath returns the entry for a path ID in the set.
func findPath(set entity.SpeculationPathSet, pathID string) (entity.SpeculationPathEntry, bool) {
	for _, entry := range set.Paths {
		if entry.ID == pathID {
			return entry, true
		}
	}
	return entity.SpeculationPathEntry{}, false
}
