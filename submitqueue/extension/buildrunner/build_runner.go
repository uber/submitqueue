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

package buildrunner

//go:generate mockgen -source=build_runner.go -destination=mock/build_runner_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// BuildRunner triggers builds against an external Build Runner, queries
// their status, and cancels them.
//
// Implementations are long-lived singletons and must:
//   - make every method safe for concurrent use by multiple goroutines;
//   - recover from transient connectivity failures internally, returning
//     plain errors during the recovery window rather than blocking the
//     caller indefinitely;
//   - keep only transient local state (caches, pools) — anything that must
//     survive a restart belongs in Storage;
//   - return plain errors and leave classification (user vs infra,
//     retryable or not) to the calling controller, per core/errs.
type BuildRunner interface {
	// Trigger submits a build that applies base then head, in order, on top
	// of the queue's target branch and validates the resulting tree.
	// Validation is implicit and holistic — it is what the runner does
	// after applying everything, not a per-change action.
	//
	// base is the dependency batches (an assumed-good prefix); head is the
	// batch being verified. The runner resolves each batch's changes itself
	// through an injected changeset resolver. Keeping base and head as
	// separate batch inputs lets a runner cache or short-circuit the base
	// when it has validated the same prefix before, and lets it attribute
	// terminal failure to base vs head in BuildMetadata.
	//
	// metadata carries free-form caller-supplied attributes (e.g. requester,
	// ticket ID, trace ID) that the runner MAY persist or echo back via
	// Status. Implementations MUST NOT depend on any specific key; nil is
	// equivalent to an empty map.
	//
	// Trigger MUST return promptly; runner-side work happens
	// asynchronously. Callers learn the build's progress via Status, not
	// via Trigger.
	//
	// The runner is already bound to its queue's job configuration by the
	// Factory that built it. Returns an error if the request is invalid.
	Trigger(
		ctx context.Context,
		base []entity.Batch,
		head entity.Batch,
		metadata entity.BuildMetadata,
	) (buildID entity.BuildID, err error)

	// Status returns the current status and runner-defined metadata
	// (build URL, duration, etc.) for a build. Unlike Trigger, Status MAY be
	// synchronous and lengthy — a runner round trip is typical.
	//
	// Returns an error if the build does not exist.
	Status(
		ctx context.Context,
		buildID entity.BuildID,
	) (entity.BuildStatus, entity.BuildMetadata, error)

	// Cancel requests cancellation and returns once the request has reached
	// the runner; it does not wait for the build to actually stop. A no-op
	// on already-terminal builds. Returns an error if the build does not exist.
	Cancel(ctx context.Context, buildID entity.BuildID) error
}

// Config carries the per-queue identity handed to a Factory. The system knows
// only the queue name; everything an implementation needs (endpoint, pipeline,
// credentials) is injected at construction by the integrator.
type Config struct {
	// QueueName identifies the queue this BuildRunner serves.
	QueueName string
}

// Factory builds the BuildRunner for a queue. Implementations are provided by
// integrators (and tests) and inject whatever they need at construction.
type Factory interface {
	// For returns the BuildRunner for the given queue.
	For(cfg Config) (BuildRunner, error)
}
