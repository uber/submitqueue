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

package build

//go:generate mockgen -source=build_manager.go -destination=mock/build_manager_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// BuildManager triggers builds against an external provider, queries their
// status, and cancels them.
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
type BuildManager interface {
	// Trigger submits changes (in order — order is significant) to the
	// provider and returns the build ID and current status. It MUST return
	// promptly; provider-side work happens asynchronously. Implementations
	// MAY return a terminal status when the input maps to an already-finished
	// build; otherwise return BuildStatusAccepted.
	//
	// queueName selects the provider-specific job configuration.
	// Returns an error if the request is invalid.
	Trigger(
		ctx context.Context,
		queueName string,
		changes []entity.BuildChange,
	) (buildID string, status entity.BuildStatus, err error)

	// Status returns the current status and provider-defined metadata
	// (build URL, duration, etc.) for a build. Unlike Trigger, Status MAY be
	// synchronous and lengthy — a provider round trip is typical.
	//
	// Returns an error if the build does not exist.
	Status(
		ctx context.Context,
		buildID string,
	) (entity.BuildStatus, entity.BuildMetadata, error)

	// Cancel requests cancellation and returns once the request has reached
	// the provider; it does not wait for the build to actually stop. A no-op
	// on already-terminal builds. Returns an error if the build does not exist.
	Cancel(ctx context.Context, buildID string) error

	// Close releases resources held by the manager. Idempotent. After Close,
	// every other method returns an error.
	Close() error
}
