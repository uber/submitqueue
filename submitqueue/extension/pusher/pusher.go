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

	"github.com/uber/submitqueue/submitqueue/entity"
)

// ErrConflict is returned by a Pusher when one of the changes fails to apply
// cleanly on top of the current tip of the target branch. Callers should
// treat conflicts as user-caused and non-retryable.
var ErrConflict = errors.New("change conflict")

// Pusher applies the changes of one or more batches on top of a target branch
// and pushes the result to the source-control remote. Each implementation is
// bound to a specific (checkout, remote, target) at construction time and
// resolves each batch's changes itself through an injected changeset resolver.
//
// Atomicity contract: when Push returns a non-nil error, NO change has been
// pushed to the remote — neither partially nor fully. Implementations must
// either roll back any local state or arrange for the push to never happen
// when any change fails to apply. Callers can treat a non-nil error as
// "the remote is exactly as it was before the call".
//
// On success, len(entity.PushResult.Batches) == len(batches) and Batches[i]
// describes what happened to batches[i], with one entity.ChangeOutcome per
// change in that batch in apply order. A change can produce multiple commits
// (entity.OutcomeStatusCommitted, CommitSHAs populated in apply order) or none
// at all (entity.OutcomeStatusAlreadyExisted, CommitSHAs empty) — the latter
// happens when the change's content is already present on the target branch.
type Pusher interface {
	// Push resolves and applies the changes of the given batches, in order,
	// onto the target branch and pushes the resulting commits. The batch list
	// designs for a merge-train (land several ready batches in one atomic push);
	// today merge passes a single batch. See the type-level docs for the
	// atomicity contract.
	Push(ctx context.Context, batches []entity.Batch) (entity.PushResult, error)
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (checkout, remote,
// target) is injected at construction by the integrator.
type Config struct {
	// QueueName identifies the queue this Pusher serves.
	QueueName string
}

// Factory builds the Pusher for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the Pusher for the given queue.
	For(cfg Config) (Pusher, error)
}
