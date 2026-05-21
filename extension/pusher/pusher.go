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

package pusher

//go:generate mockgen -source=pusher.go -destination=mock/pusher_mock.go -package=mock

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/entity"
)

// ErrConflict is returned by a Pusher when one of the changes fails to apply
// cleanly on top of the current tip of the target branch. Callers should
// treat conflicts as user-caused and non-retryable.
var ErrConflict = errors.New("change conflict")

// OutcomeStatus describes what happened to a single Change during a Push.
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

// ChangeOutcome describes what happened to a single Change inside a Push.
type ChangeOutcome struct {
	// Change is the input change this outcome corresponds to.
	Change entity.Change
	// Status describes whether the change produced commits or was already
	// present on the target branch.
	Status OutcomeStatus
	// CommitSHAs lists the commits this change produced on the target
	// branch, in apply order. A single Change may produce multiple commits
	// (e.g. a stack of PRs). Empty when Status is OutcomeStatusAlreadyExisted.
	CommitSHAs []string
}

// Result is the outcome of a successful Push call.
type Result struct {
	// Outcomes is one entry per input change, in the same order as the
	// changes passed to Push. The slice length equals the input length.
	Outcomes []ChangeOutcome
}

// Pusher applies a list of Changes on top of a target branch and pushes the
// result to the source-control remote. Each implementation is bound to a
// specific (checkout, remote, target) at construction time.
//
// Atomicity contract: when Push returns a non-nil error, NO change has been
// pushed to the remote — neither partially nor fully. Implementations must
// either roll back any local state or arrange for the push to never happen
// when any change fails to apply. Callers can treat a non-nil error as
// "the remote is exactly as it was before the call".
//
// On success, len(Result.Outcomes) == len(changes) and Outcomes[i] describes
// what happened to changes[i]. A change can produce multiple commits
// (OutcomeStatusCommitted, CommitSHAs populated in apply order) or none at
// all (OutcomeStatusAlreadyExisted, CommitSHAs empty) — the latter happens
// when the change's content is already present on the target branch.
type Pusher interface {
	// Push applies changes onto the target branch and pushes the resulting
	// commits. See the type-level docs for the atomicity contract.
	Push(ctx context.Context, changes []entity.Change) (Result, error)
}
