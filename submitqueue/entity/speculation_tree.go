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
// speculation tree store) and read by the path selector as input; enumerators
// and selectors never write it.
type SpeculationPathStatus string

const (
	// SpeculationPathStatusUnknown is the unreachable zero value, set by default
	// on init. A persisted path always carries a real status (candidate onward),
	// so this should never be seen in the store.
	SpeculationPathStatusUnknown SpeculationPathStatus = ""
	// SpeculationPathStatusCandidate is a freshly enumerated path the controller
	// has persisted but not yet sent to build.
	SpeculationPathStatusCandidate SpeculationPathStatus = "candidate"
	// SpeculationPathStatusSelected is a path the controller has sent to the build
	// controller (in response to a selector Build action) but for which no build
	// signal has arrived yet — the build system may not have started it
	// (resource-gated), so whether it is actually building is not yet known.
	SpeculationPathStatusSelected SpeculationPathStatus = "selected"
	// SpeculationPathStatusBuilding is a path a build signal has confirmed is in
	// flight; its BuildID is known.
	SpeculationPathStatusBuilding SpeculationPathStatus = "building"
	// SpeculationPathStatusPassed is a path whose build succeeded.
	SpeculationPathStatusPassed SpeculationPathStatus = "passed"
	// SpeculationPathStatusFailed is a path whose build failed.
	SpeculationPathStatusFailed SpeculationPathStatus = "failed"
	// SpeculationPathStatusCancelled is a path that is no longer pursued — its
	// base was invalidated, its build was cancelled, or the selector dropped it.
	SpeculationPathStatusCancelled SpeculationPathStatus = "cancelled"
)

// SpeculationPathAction is the action a path selector asks the controller to
// take for a path. It is the selector's only output: ephemeral (recomputed
// every time the selector runs) and never persisted. The controller enacts it
// and records the resulting SpeculationPathStatus.
type SpeculationPathAction string

const (
	// SpeculationPathActionUnknown is the unreachable zero value. A real decision
	// always carries Build or Cancel; the selector expresses "leave this path
	// as-is" by omitting it from its decisions, not by returning this.
	SpeculationPathActionUnknown SpeculationPathAction = ""
	// SpeculationPathActionBuild asks the controller to send this path to the
	// build controller (which triggers a build subject to resources). The path moves
	// to Selected on send, then Building once a build signal confirms it.
	SpeculationPathActionBuild SpeculationPathAction = "build"
	// SpeculationPathActionCancel asks the controller to drop this path and
	// cancel any build in flight for it.
	SpeculationPathActionCancel SpeculationPathAction = "cancel"
)

// SpeculationPathInfo is the per-path entry in a speculation tree: a path, its
// latest predicted-success score, its controller-owned status, and a link to
// the build dispatched for it (if any).
type SpeculationPathInfo struct {
	// Path is the Base/Head split this entry covers.
	Path SpeculationPath
	// Score is the path's predicted-success score. It is computed by the scorer
	// and persisted by the controller, not set at enumeration — the enumerator
	// produces structure only. It is dynamic: the controller re-runs the scorer
	// on every respeculate (as dependencies land, dependency builds pass, or
	// sibling paths fail), so the value tracks the latest state rather than a
	// figure frozen when the path was first enumerated (~0 until the first pass).
	Score float32
	// Status is the observed lifecycle state of the path. Written only by the
	// controller; read by the selector.
	Status SpeculationPathStatus
	// BuildID links this path to its build. Empty until a build signal confirms
	// the build and the controller records it (Selected -> Building); the
	// controller never knows the ID at send time.
	BuildID string
}

// SpeculationPathDecision is a path selector's decision for a single path: the
// action the controller should take for it. It is the selector's output and is
// not persisted.
type SpeculationPathDecision struct {
	// Path identifies the speculation path the action applies to.
	Path SpeculationPath
	// Action is what the controller should do for the path.
	Action SpeculationPathAction
}

// SpeculationTree is the set of candidate speculation paths for a batch, built
// from its dependency graph.
type SpeculationTree struct {
	// BatchID is the batch for which this speculation tree is constructed.
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
}
