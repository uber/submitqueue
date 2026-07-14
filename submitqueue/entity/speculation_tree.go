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

package entity

import "slices"

// SpeculationPath is a single speculation path: an assumed-good prefix of
// predecessor batches (Base) on top of which the batch under verification
// (Head) is built and validated.
type SpeculationPath struct {
	// Base is the ordered list of predecessor batch IDs assumed to have passed.
	// Empty means the path builds the head batch directly on the target branch.
	Base []string
	// Head is the batch ID being verified by this path.
	Head string
}

// Equal reports whether p and other are structurally the same speculation
// path. It is true iff Head matches and Base has the same elements in the same
// order — Base order is the build order and is significant. Structural
// equality identifies a path that has no assigned ID yet; a persisted path is
// referenced by its ID (SpeculationPathInfo.ID).
func (p SpeculationPath) Equal(other SpeculationPath) bool {
	return p.Head == other.Head && slices.Equal(p.Base, other.Base)
}

// SpeculationPathStatus is the observed lifecycle state of a speculation path.
type SpeculationPathStatus string

const (
	// SpeculationPathStatusUnknown is the unreachable zero value, set by default
	// on init. A persisted path always carries a real status (candidate onward),
	// so this should never be seen in the store.
	SpeculationPathStatusUnknown SpeculationPathStatus = ""
	// SpeculationPathStatusCandidate is a freshly enumerated path, persisted but
	// not yet acted on.
	SpeculationPathStatusCandidate SpeculationPathStatus = "candidate"
	// SpeculationPathStatusSelected is a path promoted by per-batch selection —
	// a desire to build it — that queue-wide prioritization has not yet cleared.
	// It is not built while Selected: it waits on the build budget until it is
	// prioritized (or dropped).
	SpeculationPathStatusSelected SpeculationPathStatus = "selected"
	// SpeculationPathStatusPrioritized is a path admitted under the queue's
	// build budget: it is cleared to build.
	SpeculationPathStatusPrioritized SpeculationPathStatus = "prioritized"
	// SpeculationPathStatusBuilding is a path whose build is confirmed in
	// flight; its BuildID is known.
	SpeculationPathStatusBuilding SpeculationPathStatus = "building"
	// SpeculationPathStatusPassed is a path whose build succeeded.
	SpeculationPathStatusPassed SpeculationPathStatus = "passed"
	// SpeculationPathStatusFailed is a path whose build failed.
	SpeculationPathStatusFailed SpeculationPathStatus = "failed"
	// SpeculationPathStatusCancelling is a path whose in-flight build has been
	// asked to stop but whose cancellation is not yet confirmed. It mirrors the
	// batch-level cancelling intent (BatchStateCancelling). A path with no build
	// in flight is dropped straight to Cancelled instead.
	SpeculationPathStatusCancelling SpeculationPathStatus = "cancelling"
	// SpeculationPathStatusCancelled is the terminal state for a path that is no
	// longer pursued — its in-flight build was confirmed stopped, or the path was
	// abandoned before a build started.
	SpeculationPathStatusCancelled SpeculationPathStatus = "cancelled"
)

// SpeculationPathAction is a requested action for a single speculation path.
// It names the decision, not its effect — applying it yields the corresponding
// SpeculationPathStatus transition — and it is ephemeral: recomputed each time
// decisions are made, never persisted.
type SpeculationPathAction string

const (
	// SpeculationPathActionUnknown is the unreachable zero value. "Leave this
	// path as-is" is expressed by omitting the path from the decision set, not
	// by this value.
	SpeculationPathActionUnknown SpeculationPathAction = ""
	// SpeculationPathActionPromote advances the path one stage toward building:
	// to Selected when decided per batch (selection), to Prioritized — cleared
	// to build — when decided queue-wide (prioritization).
	SpeculationPathActionPromote SpeculationPathAction = "promote"
	// SpeculationPathActionCancel stops pursuing the path: it moves to
	// Cancelling if a build is in flight, or straight to Cancelled if no build
	// has started.
	SpeculationPathActionCancel SpeculationPathAction = "cancel"
)

// SpeculationPathInfo is the per-path entry in a speculation tree: a path, its
// latest predicted-success score, its status, and a link to the build
// dispatched for it (if any). ID and Path are immutable once the entry is
// persisted; Score, Status, and BuildID are updateable under the tree's
// Version optimistic lock.
type SpeculationPathInfo struct {
	// ID identifies this path. It is assigned when the path entry is first
	// persisted, immutable thereafter, and globally unique — not merely unique
	// within its tree, because other entities key rows by it alone
	// (SpeculationPathBuild.PathID is a primary key with no extra scoping
	// column). Its format carries no meaning — never parse it. Everything
	// outside the tree names a path by this ID (PathScore, PathDecision,
	// SpeculationPathBuild) rather than restating the Base/Head split.
	ID string
	// Path is the Base/Head split this entry covers. Immutable: it identifies
	// the entry and never changes after the path is first persisted.
	Path SpeculationPath
	// Score is the path's predicted-success score. Updateable: it is recomputed
	// as the world changes (dependencies land, dependency builds pass, sibling
	// paths fail), so it tracks the latest state rather than a figure frozen at
	// enumeration (0 until first scored).
	Score float32
	// Status is the observed lifecycle state of the path. Updateable.
	Status SpeculationPathStatus
	// BuildID holds the runner-minted build identifier (also the build store's
	// primary key) for this path. Updateable: it is empty until a build exists
	// for this path.
	BuildID string
}

// PathScore is a freshly computed predicted-success score for a single path,
// named by its ID. Like PathDecision, it is ephemeral and never persisted; the
// score it carries lands in the tree entry (SpeculationPathInfo.Score).
type PathScore struct {
	// PathID identifies the scored path (SpeculationPathInfo.ID).
	PathID string
	// Score is the path's predicted-success probability, in [0, 1].
	Score float32
}

// PathDecision is a requested action for a single speculation path, named by
// its ID. It is ephemeral and never persisted. A decision set covers only the
// paths to act on — omitted paths are left as-is — and carries at most one
// decision per path.
type PathDecision struct {
	// PathID identifies the speculation path the action applies to
	// (SpeculationPathInfo.ID).
	PathID string
	// Action is the requested action for the path.
	Action SpeculationPathAction
}

// SpeculationTree is the set of candidate speculation paths for a batch, built
// from its dependency graph. BatchID is immutable; Paths is updateable —
// overwritten wholesale on re-speculation — guarded by Version.
type SpeculationTree struct {
	// BatchID is the batch for which this speculation tree is constructed.
	// Immutable: it identifies the tree.
	BatchID string
	// Paths is the candidate speculation paths for this batch, derived from a
	// graph of its dependencies. Each entry's per-path dynamic state (Score,
	// Status, BuildID) is documented on SpeculationPathInfo.
	//
	// For e.g - Consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 depend on queueA/batch/1.
	// Each dependent batch gets two paths: build alone, or build on the
	// assumed-good predecessor. Just after enumeration every path is a candidate:
	//
	// Paths for queueA/batch/2 - [{Path: {Base: [], Head: "queueA/batch/2"}, Status: "candidate"}, {Path: {Base: ["queueA/batch/1"], Head: "queueA/batch/2"}, Status: "candidate"}]
	// Paths for queueA/batch/3 - [{Path: {Base: [], Head: "queueA/batch/3"}, Status: "candidate"}, {Path: {Base: ["queueA/batch/1"], Head: "queueA/batch/3"}, Status: "candidate"}]
	//
	Paths []SpeculationPathInfo
	// Version is the version of the object. It is used for optimistic locking:
	// updates are conditional on the persisted version matching the caller's
	// expected version. Versioning starts at 1 and is incremented for each
	// change to the object.
	Version int32
}

// PathIndex returns the index of the entry in t.Paths whose ID is id, or -1
// if none is.
func (t SpeculationTree) PathIndex(id string) int {
	for i, p := range t.Paths {
		if p.ID == id {
			return i
		}
	}
	return -1
}
