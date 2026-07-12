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

// SpeculationPath is a single speculation path: an assumed-good prefix of
// predecessor batches (Base) on top of which the batch under verification
// (Head) is built and validated.
//
// This is the unit the build stage consumes: Base maps to the build runner's
// base changes (an assumed-good prefix to apply) and Head maps to the changes
// being validated.
type SpeculationPath struct {
	// Base is the ordered list of predecessor batch IDs assumed to have passed.
	// Empty means the path builds the head batch directly on the target branch.
	Base []string
	// Head is the batch ID being verified by this path.
	Head string
}

// SpeculationPathStatus is the observed lifecycle state of a speculation path.
// It is written only by the orchestrator's speculate controller (into the
// speculation tree store) and read by the decision seams (selector, prioritizer)
// as input; the seams never write it.
type SpeculationPathStatus string

const (
	// SpeculationPathStatusUnknown is the unreachable zero value, set by default
	// on init. A persisted path always carries a real status (candidate onward),
	// so this should never be seen in the store.
	SpeculationPathStatusUnknown SpeculationPathStatus = ""
	// SpeculationPathStatusCandidate is a freshly enumerated path the controller
	// has persisted but not yet acted on.
	SpeculationPathStatusCandidate SpeculationPathStatus = "candidate"
	// SpeculationPathStatusSelected is a path the selector has promoted — a
	// per-batch desire to build it — that the queue-wide prioritizer has not yet
	// cleared. It is not sent to build while Selected: it waits on the build
	// budget until it is prioritized (or dropped).
	SpeculationPathStatusSelected SpeculationPathStatus = "selected"
	// SpeculationPathStatusPrioritized is a path the prioritizer has admitted
	// under the queue's build budget; it is cleared to run. The build effector
	// triggers only Prioritized paths.
	SpeculationPathStatusPrioritized SpeculationPathStatus = "prioritized"
	// SpeculationPathStatusBuilding is a path a build signal has confirmed is in
	// flight; its BuildID is known.
	SpeculationPathStatusBuilding SpeculationPathStatus = "building"
	// SpeculationPathStatusPassed is a path whose build succeeded.
	SpeculationPathStatusPassed SpeculationPathStatus = "passed"
	// SpeculationPathStatusFailed is a path whose build failed.
	SpeculationPathStatusFailed SpeculationPathStatus = "failed"
	// SpeculationPathStatusCancelling is a path whose in-flight build the
	// controller has asked to stop but whose cancellation is not yet confirmed. It
	// mirrors the batch-level cancelling intent (BatchStateCancelling): the cancel
	// decision is recorded here and the effector drives it to terminal Cancelled.
	// A path with no build in flight is dropped straight to Cancelled instead.
	SpeculationPathStatusCancelling SpeculationPathStatus = "cancelling"
	// SpeculationPathStatusCancelled is the terminal state for a path that is no
	// longer pursued — its in-flight build was confirmed stopped, or the path was
	// abandoned before a build started.
	SpeculationPathStatusCancelled SpeculationPathStatus = "cancelled"
)

// SpeculationPathAction is the decision a seam (the selector or the prioritizer)
// asks the controller to take for a path. It names the decision, not its effect:
// it is ephemeral (recomputed every time a seam runs) and never persisted. The
// controller maps it to the corresponding SpeculationPathStatus transition —
// applied under the tree's optimistic lock (Version) — and records the result;
// the seams never write status themselves.
type SpeculationPathAction string

const (
	// SpeculationPathActionUnknown is the unreachable zero value. A seam
	// expresses "leave this path as-is" by omitting the path from its decisions,
	// not by returning this.
	SpeculationPathActionUnknown SpeculationPathAction = ""
	// SpeculationPathActionPromote asks the controller to advance this path one
	// stage toward running. The target status depends on which seam decided it:
	// the selector's promote moves a path to Selected; the prioritizer's promote
	// moves it to Prioritized (cleared to build).
	SpeculationPathActionPromote SpeculationPathAction = "promote"
	// SpeculationPathActionCancel asks the controller to stop pursuing this path:
	// it moves to Cancelling if a build is in flight (the effector then confirms
	// the stop and drives it to terminal Cancelled), or straight to Cancelled if
	// no build has started.
	SpeculationPathActionCancel SpeculationPathAction = "cancel"
)

// SpeculationPathInfo is the per-path entry in a speculation tree: a path, its
// latest predicted-success score, its controller-owned status, and a link to
// the build dispatched for it (if any). ID and Path are immutable once the
// entry is persisted; Score, Status, and BuildID are updateable, written only
// by the controller under the tree's Version optimistic lock.
type SpeculationPathInfo struct {
	// ID identifies this path within its tree. It is assigned by the controller
	// when the path entry is first persisted, immutable thereafter, and unique
	// within the tree; its format is the controller's choice and carries no
	// meaning — never parse it. Everything outside the tree names a path by this
	// ID: seam outputs (path scores, path decisions) and durable links from
	// other entities all refer to it rather than restating the Base/Head split.
	ID string
	// Path is the Base/Head split this entry covers. Immutable: it identifies
	// the entry and never changes after the path is first persisted.
	Path SpeculationPath
	// Score is the path's predicted-success score. Updateable: it is computed by
	// the scorer and persisted by the controller, not set at enumeration — the
	// enumerator produces structure only. It is dynamic: the controller re-runs
	// the scorer on every respeculate (as dependencies land, dependency builds
	// pass, or sibling paths fail), so the value tracks the latest state rather
	// than a figure frozen when the path was first enumerated (~0 until the
	// first pass).
	Score float32
	// Status is the observed lifecycle state of the path. Updateable: written
	// only by the controller; read by the decision seams (scorer, selector,
	// prioritizer).
	Status SpeculationPathStatus
	// BuildID links this path to its build. Updateable: empty until a build
	// signal confirms the build and the controller records it (Prioritized ->
	// Building); the controller never knows the ID at send time.
	BuildID string
}

// PathScore is the path scorer's verdict for a single path: the
// path's identity and its freshly computed predicted-success score. It is the
// scorer seam's only output — the controller merges scores into the tree by
// path ID and persists them; tree structure and status never pass through the
// scorer. Like SpeculationPathDecision, it is ephemeral and never persisted.
type PathScore struct {
	// PathID identifies the scored path (SpeculationPathInfo.ID) within the
	// tree the scorer was handed.
	PathID string
	// Score is the path's predicted-success probability, in [0, 1].
	Score float32
}

// SpeculationPathDecision is a seam's decision for a single path: the action the
// controller should take for it. It is the output of both the selector (per
// batch) and the prioritizer (queue-wide), and is not persisted. A seam returns
// a decision only for the paths it wants to act on; omitted paths are left
// as-is.
type SpeculationPathDecision struct {
	// Path identifies the speculation path the action applies to.
	Path SpeculationPath
	// Action is what the controller should do for the path.
	Action SpeculationPathAction
}

// SpeculationTree is the set of candidate speculation paths for a batch, built
// from its dependency graph. BatchID is immutable; Paths is updateable (the
// controller overwrites it wholesale on every respeculate), guarded by Version.
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
	// change to the object; version arithmetic is owned by the controller, the
	// store performs a pure conditional write.
	Version int32
}
