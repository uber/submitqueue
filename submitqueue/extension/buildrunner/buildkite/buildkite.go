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

// Package buildkite implements buildrunner.BuildRunner backed by the Buildkite
// CI platform.
//
// Trigger calls the Buildkite API to create the build and returns the Buildkite
// build number as the build ID. Status and Cancel parse the number directly
// from the build ID — no local state is required.
//
// The Buildkite build receives base and head change URIs as JSON-encoded
// environment variables (SQ_BASE_URIS, SQ_HEAD_URIS, SQ_QUEUE). The pipeline
// script fetches each PR's diff with the GitHub API, applies them with
// `git apply -3`, produces one commit per layer (base, head), then runs CI.
//
// Caller-supplied BuildMetadata is forwarded to the build as SQ_METADATA
// (JSON-encoded). Buildkite echoes env vars back on the build object, so
// Status recovers and returns the original metadata without any local state.
package buildkite

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"github.com/uber/submitqueue/platform/base/change"
	platformbuildkite "github.com/uber/submitqueue/platform/extension/buildrunner/buildkite"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// Env var keys set on every triggered Buildkite build.
const (
	// EnvKeyBaseURIs carries the JSON-encoded ordered list of change URIs from
	// the dependency batches. The pipeline script applies these first and
	// commits the result as the "base" layer.
	EnvKeyBaseURIs = "SQ_BASE_URIS"

	// EnvKeyHeadURIs carries the JSON-encoded ordered list of change URIs from
	// the batch under test. Applied on top of the base layer, committed
	// separately.
	EnvKeyHeadURIs = "SQ_HEAD_URIS"

	// EnvKeyQueue carries the SQ queue name so the pipeline script can select
	// queue-specific test targets.
	EnvKeyQueue = "SQ_QUEUE"

	// EnvKeyMetadata carries the JSON-encoded BuildMetadata provided by the
	// caller to Trigger. Buildkite echoes env vars on the build object, so
	// Status can recover and return the original metadata without local state.
	EnvKeyMetadata = "SQ_METADATA"
)

// runner implements buildrunner.BuildRunner.
type runner struct {
	cfg      buildrunner.Config
	client   *platformbuildkite.Client
	resolver changeset.Resolver
	logger   *zap.SugaredLogger
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// Params holds the dependencies for a Buildkite BuildRunner.
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config
	// Client is a pre-constructed Buildkite client. The wiring layer builds
	// it once via platformbuildkite.NewClient, with the pipeline's base URL
	// (via platform/http.BaseURLTransport) and auth already configured.
	Client *platformbuildkite.Client
	// Resolver resolves a batch's changes (base and head batches).
	Resolver changeset.Resolver
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
}

// NewBuildRunner constructs a Buildkite-backed BuildRunner bound to a single
// pipeline.
func NewBuildRunner(params Params) (buildrunner.BuildRunner, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("buildkite client is required")
	}
	if params.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	return newRunner(params.Config, params.Client, params.Resolver, params.Logger.Named("buildkite_buildrunner")), nil
}

// newRunner constructs a runner. Used by NewBuildRunner and by tests.
func newRunner(cfg buildrunner.Config, c *platformbuildkite.Client, resolver changeset.Resolver, logger *zap.SugaredLogger) *runner {
	return &runner{
		cfg:      cfg,
		client:   c,
		resolver: resolver,
		logger:   logger,
	}
}

// Trigger calls the Buildkite API to create the build and returns the Buildkite
// build number as the build ID. Errors are propagated to the caller so the
// queue consumer can nack and retry.
func (r *runner) Trigger(ctx context.Context, base []entity.Batch, head entity.Batch, metadata entity.BuildMetadata) (entity.BuildID, error) {
	baseChanges, err := buildrunner.ResolveBatches(ctx, r.resolver, base)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("buildkite: resolve base: %w", err)
	}
	headChanges, err := r.resolver.ChangesForBatch(ctx, head)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("buildkite: resolve head: %w", err)
	}

	baseJSON, _ := json.Marshal(flattenURIs(baseChanges))
	headJSON, _ := json.Marshal(flattenURIs(headChanges))

	env := map[string]string{
		EnvKeyBaseURIs: string(baseJSON),
		EnvKeyHeadURIs: string(headJSON),
		EnvKeyQueue:    r.cfg.QueueName,
	}
	if len(metadata) > 0 {
		metaJSON, _ := json.Marshal(metadata)
		env[EnvKeyMetadata] = string(metaJSON)
	}

	req := platformbuildkite.CreateBuildRequest{
		Message: "submitqueue speculative build",
		Env:     env,
	}

	resp, err := r.client.CreateBuild(ctx, req)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("buildkite: create build: %w", err)
	}

	r.logger.Debugw("triggered Buildkite build",
		"buildkite_number", resp.Number,
	)
	return entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(resp.Number)}, nil
}

// Status fetches the current state of the build from Buildkite and returns it
// with the build URL and any caller-supplied metadata in BuildMetadata.
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	number, err := platformbuildkite.ParseBuildNumber(buildID.ID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: malformed build ID: %w", err)
	}

	resp, err := r.client.GetBuild(ctx, number)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: get build: %w", err)
	}

	meta := decodeMetadata(resp.Env)
	meta["url"] = resp.WebURL
	return mapState(resp.State), meta, nil
}

// Cancel calls the Buildkite API to cancel the build. A no-op on already-terminal
// builds (Buildkite returns 422 for those).
func (r *runner) Cancel(ctx context.Context, buildID entity.BuildID) error {
	number, err := platformbuildkite.ParseBuildNumber(buildID.ID)
	if err != nil {
		return fmt.Errorf("buildkite: malformed build ID: %w", err)
	}

	if err := r.client.CancelBuild(ctx, number); err != nil {
		return fmt.Errorf("buildkite: cancel build: %w", err)
	}
	r.logger.Debugw("cancelled Buildkite build",
		"buildkite_number", number,
	)
	return nil
}

// flattenURIs concatenates the URI lists from all changes into a single slice.
func flattenURIs(changes []change.Change) []string {
	uris := make([]string, 0, len(changes))
	for _, c := range changes {
		uris = append(uris, c.URIs...)
	}
	return uris
}

// decodeMetadata recovers the caller-supplied BuildMetadata from the env vars
// Buildkite echoes back on the build object.
func decodeMetadata(env map[string]string) entity.BuildMetadata {
	return entity.BuildMetadata(platformbuildkite.DecodeMetadataEnv(env, EnvKeyMetadata))
}

// mapState maps a Buildkite build state to a BuildStatus.
func mapState(state string) entity.BuildStatus {
	switch platformbuildkite.ParseState(state) {
	case platformbuildkite.StateAccepted:
		return entity.BuildStatusAccepted
	case platformbuildkite.StateRunning:
		return entity.BuildStatusRunning
	case platformbuildkite.StateSucceeded:
		return entity.BuildStatusSucceeded
	case platformbuildkite.StateFailed:
		return entity.BuildStatusFailed
	case platformbuildkite.StateCancelled:
		return entity.BuildStatusCancelled
	default:
		// Unrecognised Buildkite state. Do NOT assume terminal: Unknown is
		// non-terminal, so the buildsignal poll loop keeps waiting rather than
		// failing the batch on a state this code does not understand.
		return entity.BuildStatusUnknown
	}
}
