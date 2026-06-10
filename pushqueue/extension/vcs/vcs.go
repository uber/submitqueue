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

package vcs

//go:generate mockgen -source=vcs.go -destination=mock/vcs_mock.go -package=mock

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/pushqueue/entity"
)

// ErrConflict is returned when a change fails to apply cleanly on top of the target.
var ErrConflict = errors.New("change conflict")

// ErrStaleHead is returned when the target ref moved during the push, making the operation stale.
var ErrStaleHead = errors.New("stale head")

// OutcomeStatus describes what happened to a single item during a Push.
type OutcomeStatus string

const (
	// OutcomeStatusUnknown is the unreachable zero value.
	OutcomeStatusUnknown OutcomeStatus = ""
	// OutcomeStatusCommitted means the change produced one or more revisions on the target.
	OutcomeStatusCommitted OutcomeStatus = "committed"
	// OutcomeStatusAlreadyExisted means the change was already present on the target.
	OutcomeStatusAlreadyExisted OutcomeStatus = "already_existed"
)

// ItemOutcome describes what happened to a single LandItem during a Push.
type ItemOutcome struct {
	// Status describes whether the change produced revisions or was already present.
	Status OutcomeStatus
	// RevisionIDs lists the revisions produced on the target, in apply order.
	RevisionIDs []string
}

// PushResult holds the outcomes of a successful Push call.
type PushResult struct {
	// Outcomes is one entry per input item, in the same order.
	Outcomes []ItemOutcome
}

// MergeabilityResult describes whether a single LandItem can be landed.
type MergeabilityResult struct {
	// Mergeable is true if the item can be landed.
	Mergeable bool
	// Reason is a human-readable explanation when not mergeable.
	Reason string
}

// VCS performs all version control and change-provider operations for landing
// changes onto a target branch. Implementations bundle VCS operations (git,
// Perforce) with platform operations (GitHub, Phabricator) where needed.
//
// Idempotency: every method must be safe to call multiple times with the same
// inputs. The full Land flow (Prepare → Push → Finalize) may be retried
// end-to-end on any failure:
//   - Prepare: re-applying already-applied changes rebuilds the working copy.
//     Implementations detect stale state from a previous failed attempt and
//     recover automatically.
//   - Push: detects already-pushed changes, returns OutcomeStatusAlreadyExisted.
//   - Finalize: closing an already-closed review is a no-op.
//   - CheckMergeability: read-only, naturally idempotent.
//
// Push atomicity: when Push returns a non-nil error, NO change has been pushed
// to the remote.
type VCS interface {
	CheckMergeability(ctx context.Context, target entity.QueueTarget, items []entity.LandItem) ([]MergeabilityResult, error)
	Prepare(ctx context.Context, target entity.QueueTarget, items []entity.LandItem) error
	Push(ctx context.Context, target entity.QueueTarget, items []entity.LandItem) (PushResult, error)
	Finalize(ctx context.Context, target entity.QueueTarget, items []entity.LandItem) error
}
