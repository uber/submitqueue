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

// Package buildrunner defines the contract through which Stovepipe triggers and
// polls builds against an external build system. It is shaped the same as
// SubmitQueue's own buildrunner extension — same Trigger/Status/Cancel verbs,
// same async contract, same id model — but is a separate interface rather than a
// shared one: Stovepipe validates a single commit against a baseline (or from
// scratch), not a stack of dependency batches, so Trigger takes URI identity
// instead of batch identity. See doc/rfc/stovepipe/steps/build.md's "Why separate
// contracts" for the full rationale.
package buildrunner

//go:generate mockgen -source=buildrunner.go -destination=mock/buildrunner_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// BuildRunner triggers builds against an external build system, polls their
// status, and cancels them. Implementations are long-lived singletons and must:
//   - be safe for concurrent use by multiple goroutines;
//   - recover from transient connectivity failures internally, returning plain
//     errors during the recovery window rather than blocking the caller
//     indefinitely;
//   - keep only transient local state (caches, pools) — the durable link between
//     a Request and its Build lives in BuildStore, never in the runner;
//   - return plain, unclassified errors and leave user-vs-infra and
//     retryable-vs-not classification to the calling controller, per
//     platform/errs.
type BuildRunner interface {
	// Trigger starts a new build every call and mints the build's identity —
	// there is no caller-supplied dedup input. baseURI is the incremental
	// baseline, empty for a full build; headURI is the commit under
	// validation; both are opaque tokens owned by SourceControl. metadata is
	// caller-supplied annotation the runner may echo back via Status but must
	// not depend on. Trigger is async: it must return promptly with the
	// runner-assigned id, not an outcome — callers learn progress via Status.
	Trigger(ctx context.Context, baseURI, headURI string, metadata entity.BuildMetadata) (entity.BuildID, error)

	// Status returns the build's current status and any provider metadata for
	// the id Trigger returned. Unlike Trigger, Status may round-trip to the
	// backend and block. The returned BuildMetadata is caller-supplied,
	// provider-echoed — the runner must not depend on it, but a caller may
	// read it for its own purposes.
	Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error)

	// Cancel requests cancellation of the build for the id Trigger returned,
	// returning once the request reaches the runner, not once the build
	// actually stops. A no-op on an already-terminal build. No controller
	// calls Cancel today — see doc/rfc/stovepipe/steps/build.md's
	// "Cancellation: defined, not yet called" — it exists for contract parity
	// and future use.
	Cancel(ctx context.Context, buildID entity.BuildID) error
}

// Config carries the per-queue identity handed to a Factory. It is the only
// identity the system hands a Factory; everything else a concrete BuildRunner
// needs (endpoint, credentials, pipeline mapping) is injected by the integrator
// at construction.
type Config struct {
	// QueueName identifies which Queue's build-runner backend to resolve.
	QueueName string
}

// Factory resolves the BuildRunner for a Config. Implementations and the
// per-queue routing that picks a backend for a Config.QueueName live in the
// wiring layer (service/stovepipe/.../server/main.go), not here — see
// CLAUDE.md's extension rules.
type Factory interface {
	// For returns the BuildRunner for the given queue.
	For(cfg Config) (BuildRunner, error)
}
