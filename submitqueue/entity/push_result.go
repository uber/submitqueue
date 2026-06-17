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

import "github.com/uber/submitqueue/platform/base/change"

// OutcomeStatus describes what happened to a single Change during a push.
type OutcomeStatus string

const (
	// OutcomeStatusUnknown is the unreachable zero value, set by default
	// when the structure is initialized. It should never be seen in the system.
	OutcomeStatusUnknown OutcomeStatus = ""
	// OutcomeStatusCommitted means the change produced one or more commits
	// on the target branch. CommitSHAs lists those commits in apply order.
	OutcomeStatusCommitted OutcomeStatus = "committed"
	// OutcomeStatusAlreadyExisted means the change produced no commits
	// because every part of it is already present in the target branch
	// (e.g. it previously landed via another path, or a prior change in
	// the same push subsumed it). CommitSHAs is empty for this status.
	// In git terms this is what a `cherry-pick` surfaces as "rebased out".
	OutcomeStatusAlreadyExisted OutcomeStatus = "already_existed"
)

// ChangeOutcome describes what happened to a single Change inside a push.
type ChangeOutcome struct {
	// Change is the input change this outcome corresponds to.
	Change change.Change
	// Status describes whether the change produced commits or was already
	// present on the target branch.
	Status OutcomeStatus
	// CommitSHAs lists the commits this change produced on the target
	// branch, in apply order. A single Change may produce multiple commits
	// (e.g. a stack of PRs). Empty when Status is OutcomeStatusAlreadyExisted.
	CommitSHAs []string
}

// BatchOutcome groups the per-change outcomes for a single pushed batch, so a
// merge-train push (several batches in one call) stays correlatable back to the
// batch each change belonged to. There is no per-batch status: a push is
// all-or-nothing across the whole call, so a per-batch pass/fail would be
// uniformly redundant.
type BatchOutcome struct {
	// BatchID is the input batch this outcome corresponds to.
	BatchID string
	// Outcomes is one entry per change in the batch, in apply order.
	Outcomes []ChangeOutcome
}

// PushResult is the outcome of a successful push.
type PushResult struct {
	// Batches is one entry per pushed batch, in the same order as the batches
	// passed to the push. The slice length equals the input length.
	Batches []BatchOutcome
}
