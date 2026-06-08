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
	"net/http"
	"strconv"

	"go.uber.org/zap"

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
	client   *client
	resolver changeset.Resolver
	logger   *zap.SugaredLogger
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// Params holds the dependencies for a Buildkite BuildRunner. The caller is
// responsible for configuring HTTPClient with the base URL (via
// httpclient.BaseURLTransport) and auth (via an Authorization-header transport).
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config
	// HTTPClient is a pre-configured HTTP client. The caller is responsible
	// for the base URL (via httpclient.BaseURLTransport) and auth (via a
	// transport layer). If nil, http.DefaultClient is used.
	HTTPClient *http.Client
	// Resolver resolves a batch's changes (base and head batches).
	Resolver changeset.Resolver
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
}

// NewBuildRunner constructs a Buildkite-backed BuildRunner bound to a single
// pipeline.
//
// The HTTPClient must have BaseURLTransport configured to the pipeline's API
// root (e.g. "https://api.buildkite.com/v2/organizations/{org}/pipelines/{slug}"),
// and an auth transport that injects the Authorization header.
func NewBuildRunner(params Params) (buildrunner.BuildRunner, error) {
	if params.HTTPClient == nil {
		return nil, fmt.Errorf("http client is required")
	}
	if params.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	return newRunner(params.Config, &client{httpClient: params.HTTPClient}, params.Resolver, params.Logger.Named("buildkite_buildrunner")), nil
}

// newRunner constructs a runner. Used by NewBuildRunner and by tests.
func newRunner(cfg buildrunner.Config, c *client, resolver changeset.Resolver, logger *zap.SugaredLogger) *runner {
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

	req := createBuildRequest{
		Message: "submitqueue speculative build",
		Env:     env,
	}

	resp, err := r.client.createBuild(ctx, req)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("buildkite: create build: %w", err)
	}

	r.logger.Debugw("triggered Buildkite build",
		"buildkite_number", resp.Number,
	)
	return entity.BuildID{ID: encodeBuildNumber(resp.Number)}, nil
}

// Status fetches the current state of the build from Buildkite and returns it
// with the build URL and any caller-supplied metadata in BuildMetadata.
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	number, err := parseBuildNumber(buildID.ID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("buildkite: malformed build ID: %w", err)
	}

	resp, err := r.client.getBuild(ctx, number)
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
	number, err := parseBuildNumber(buildID.ID)
	if err != nil {
		return fmt.Errorf("buildkite: malformed build ID: %w", err)
	}

	if err := r.client.cancelBuild(ctx, number); err != nil {
		return fmt.Errorf("buildkite: cancel build: %w", err)
	}
	r.logger.Debugw("cancelled Buildkite build",
		"buildkite_number", number,
	)
	return nil
}

// flattenURIs concatenates the URI lists from all changes into a single slice.
func flattenURIs(changes []entity.Change) []string {
	uris := make([]string, 0, len(changes))
	for _, c := range changes {
		uris = append(uris, c.URIs...)
	}
	return uris
}

// encodeBuildNumber encodes a Buildkite build number as the SQ build ID.
func encodeBuildNumber(number int) string {
	return strconv.Itoa(number)
}

// parseBuildNumber is the inverse of encodeBuildNumber.
func parseBuildNumber(id string) (int, error) {
	n, err := strconv.Atoi(id)
	if err != nil {
		return 0, fmt.Errorf("invalid build ID %q", id)
	}
	return n, nil
}

// decodeMetadata recovers the caller-supplied BuildMetadata from the env vars
// Buildkite echoes back on the build object. Returns an empty non-nil map when
// SQ_METADATA is absent or cannot be decoded — a corrupt env var must not fail
// a Status call.
func decodeMetadata(env map[string]string) entity.BuildMetadata {
	meta := make(entity.BuildMetadata)
	raw, ok := env[EnvKeyMetadata]
	if !ok || raw == "" {
		return meta
	}
	_ = json.Unmarshal([]byte(raw), &meta)
	return meta
}

// mapState maps a Buildkite build state string to a BuildStatus.
//
// Buildkite states: creating, scheduled, running, blocked, passed, failed,
// canceling, canceled, skipped, not_run.
func mapState(state string) entity.BuildStatus {
	switch state {
	case "creating", "scheduled":
		return entity.BuildStatusAccepted
	case "running", "blocked":
		// blocked = waiting on a block step; still live, not yet terminal.
		return entity.BuildStatusRunning
	case "passed":
		return entity.BuildStatusSucceeded
	case "failed", "not_run", "skipped":
		// not_run/skipped never produced a passing result; treat them as
		// terminal failure so the batch is not merged on a non-success verdict.
		return entity.BuildStatusFailed
	case "canceling", "canceled":
		return entity.BuildStatusCancelled
	default:
		// Unrecognised Buildkite state. Do NOT assume terminal: Unknown is
		// non-terminal, so the buildsignal poll loop keeps waiting rather than
		// failing the batch on a state this code does not understand.
		return entity.BuildStatusUnknown
	}
}
