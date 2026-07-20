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
// The Buildkite build receives the head and base URIs as environment
// variables (STOVEPIPE_HEAD_URI, STOVEPIPE_BASE_URI, STOVEPIPE_QUEUE). The
// pipeline script checks out headURI and, when baseURI is non-empty, diffs
// against it for an incremental build.
//
// Caller-supplied BuildMetadata is forwarded to the build as
// STOVEPIPE_METADATA (JSON-encoded). Buildkite echoes env vars back on the
// build object, so Status recovers and returns the original metadata without
// any local state.
package buildkite

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	platformbuildkite "github.com/uber/submitqueue/platform/extension/buildrunner/buildkite"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

// Env var keys set on every triggered Buildkite build.
const (
	// EnvKeyHeadURI carries the URI of the commit under validation.
	EnvKeyHeadURI = "STOVEPIPE_HEAD_URI"

	// EnvKeyBaseURI carries the incremental baseline URI, empty for a full
	// build.
	EnvKeyBaseURI = "STOVEPIPE_BASE_URI"

	// EnvKeyQueue carries the queue name so the pipeline script can select
	// queue-specific test targets.
	EnvKeyQueue = "STOVEPIPE_QUEUE"

	// EnvKeyMetadata carries the JSON-encoded BuildMetadata provided by the
	// caller to Trigger. Buildkite echoes env vars on the build object, so
	// Status can recover and return the original metadata without local state.
	EnvKeyMetadata = "STOVEPIPE_METADATA"
)

// runner implements buildrunner.BuildRunner.
type runner struct {
	cfg    buildrunner.Config
	client *platformbuildkite.Client
	logger *zap.SugaredLogger
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
	return newRunner(params.Config, params.Client, params.Logger.Named("buildkite_buildrunner")), nil
}

// newRunner constructs a runner. Used by NewBuildRunner and by tests.
func newRunner(cfg buildrunner.Config, c *platformbuildkite.Client, logger *zap.SugaredLogger) *runner {
	return &runner{
		cfg:    cfg,
		client: c,
		logger: logger,
	}
}

// Trigger calls the Buildkite API to create the build and returns the
// Buildkite build number as the build ID. Errors are propagated to the
// caller so the queue consumer can nack and retry.
func (r *runner) Trigger(ctx context.Context, baseURI, headURI string, metadata entity.BuildMetadata) (entity.BuildID, error) {
	env := map[string]string{
		EnvKeyHeadURI: headURI,
		EnvKeyBaseURI: baseURI,
		EnvKeyQueue:   r.cfg.QueueName,
	}
	if len(metadata) > 0 {
		metaJSON, _ := json.Marshal(metadata)
		env[EnvKeyMetadata] = string(metaJSON)
	}

	req := platformbuildkite.CreateBuildRequest{
		Message: "stovepipe build",
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

// Cancel calls the Buildkite API to cancel the build. A no-op on
// already-terminal builds (Buildkite returns 422 for those).
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
		// failing the build on a state this code does not understand.
		return entity.BuildStatusUnknown
	}
}
