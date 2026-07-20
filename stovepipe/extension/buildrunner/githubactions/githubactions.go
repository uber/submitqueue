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

// Package githubactions implements buildrunner.BuildRunner backed by GitHub
// Actions workflow_dispatch.
//
// Trigger dispatches the configured workflow on a trusted ref and returns the
// GitHub workflow run ID as the build ID. Status and Cancel use that ID to
// call GitHub's workflow-run endpoints — no local state is required.
//
// The workflow receives the head and base URIs as workflow inputs
// (stovepipe_head_uri, stovepipe_base_uri, stovepipe_queue). The workflow
// definition checks out stovepipe_head_uri and, when stovepipe_base_uri is
// non-empty, diffs against it for an incremental build.
//
// Caller-supplied BuildMetadata is forwarded to the dispatch as
// stovepipe_metadata (JSON-encoded). Unlike Buildkite, GitHub does not echo
// dispatch inputs back on the run object, so Status returns the run's own
// identity/outcome metadata instead of the caller-supplied metadata.
package githubactions

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	platformgithubactions "github.com/uber/submitqueue/platform/extension/buildrunner/githubactions"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

// Workflow input keys set on every dispatched run.
const (
	// InputKeyHeadURI carries the URI of the commit under validation.
	InputKeyHeadURI = "stovepipe_head_uri"

	// InputKeyBaseURI carries the incremental baseline URI, empty for a full
	// build.
	InputKeyBaseURI = "stovepipe_base_uri"

	// InputKeyQueue carries the queue name so the workflow can select
	// queue-specific test targets.
	InputKeyQueue = "stovepipe_queue"

	// InputKeyMetadata carries the JSON-encoded BuildMetadata provided by the
	// caller to Trigger.
	InputKeyMetadata = "stovepipe_metadata"

	defaultRef = "main"
)

// Params holds the dependencies for a GitHub Actions BuildRunner.
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config
	// Client is a pre-constructed GitHub Actions client, bound to one
	// repository and workflow. The wiring layer builds it once via
	// platformgithubactions.NewClient, with the GitHub API root (via
	// platform/http.BaseURLTransport) and auth already configured.
	Client *platformgithubactions.Client
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// Ref is the branch, tag, or SHA where the trusted workflow is read from.
	// Defaults to "main".
	Ref string
	// ExtraInputs are copied into every workflow_dispatch request. Reserved
	// stovepipe_* inputs managed by this package override conflicting keys.
	ExtraInputs map[string]string
}

// runner implements buildrunner.BuildRunner.
type runner struct {
	cfg         buildrunner.Config
	ref         string
	extraInputs map[string]string
	client      *platformgithubactions.Client
	logger      *zap.SugaredLogger
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// NewBuildRunner constructs a GitHub Actions-backed BuildRunner bound to one
// repository/workflow and one queue config.
func NewBuildRunner(params Params) (buildrunner.BuildRunner, error) {
	if params.Client == nil {
		return nil, fmt.Errorf("github actions client is required")
	}
	if params.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	ref := params.Ref
	if ref == "" {
		ref = defaultRef
	}
	return newRunner(params.Config, ref, params.ExtraInputs, params.Client, params.Logger.Named("githubactions_buildrunner")), nil
}

// newRunner constructs a runner. Used by NewBuildRunner and by tests.
func newRunner(cfg buildrunner.Config, ref string, extraInputs map[string]string, c *platformgithubactions.Client, logger *zap.SugaredLogger) *runner {
	copied := make(map[string]string, len(extraInputs))
	for k, v := range extraInputs {
		copied[k] = v
	}
	return &runner{
		cfg:         cfg,
		ref:         ref,
		extraInputs: copied,
		client:      c,
		logger:      logger,
	}
}

// Trigger dispatches the configured GitHub Actions workflow and returns the
// GitHub workflow run ID as the build ID. Errors are propagated to the caller
// so the queue consumer can nack and retry.
func (r *runner) Trigger(ctx context.Context, baseURI, headURI string, metadata entity.BuildMetadata) (entity.BuildID, error) {
	inputs := make(map[string]string, len(r.extraInputs)+4)
	for k, v := range r.extraInputs {
		inputs[k] = v
	}
	inputs[InputKeyHeadURI] = headURI
	inputs[InputKeyBaseURI] = baseURI
	inputs[InputKeyQueue] = r.cfg.QueueName
	if len(metadata) > 0 {
		metaJSON, err := json.Marshal(metadata)
		if err != nil {
			return entity.BuildID{}, fmt.Errorf("github actions: marshal metadata: %w", err)
		}
		inputs[InputKeyMetadata] = string(metaJSON)
	}

	resp, err := r.client.DispatchWorkflow(ctx, platformgithubactions.DispatchWorkflowRequest{
		Ref:              r.ref,
		ReturnRunDetails: true,
		Inputs:           inputs,
	})
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("github actions: dispatch workflow: %w", err)
	}
	if resp.WorkflowRunID <= 0 {
		return entity.BuildID{}, fmt.Errorf("github actions: dispatch workflow: response missing workflow_run_id (requires return_run_details support)")
	}

	r.logger.Debugw("dispatched GitHub Actions workflow",
		"owner", r.client.Owner(),
		"repo", r.client.Repo(),
		"workflow_id", r.client.WorkflowID(),
		"ref", r.ref,
		"github_run_id", resp.WorkflowRunID,
	)
	return entity.BuildID{ID: platformgithubactions.EncodeRunID(resp.WorkflowRunID)}, nil
}

// Status fetches the workflow run by GitHub run ID and maps the run's
// status/conclusion onto BuildStatus, returning the run's own identity and
// outcome fields as BuildMetadata.
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	runID, err := platformgithubactions.ParseRunID(buildID.ID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("github actions: malformed build ID: %w", err)
	}

	run, err := r.client.GetRun(ctx, runID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("github actions: get run: %w", err)
	}
	return mapRunStatus(run.Status, run.Conclusion), entity.BuildMetadata(platformgithubactions.RunMetadata(run)), nil
}

// Cancel requests cancellation of the workflow run identified by buildID.
func (r *runner) Cancel(ctx context.Context, buildID entity.BuildID) error {
	runID, err := platformgithubactions.ParseRunID(buildID.ID)
	if err != nil {
		return fmt.Errorf("github actions: malformed build ID: %w", err)
	}
	if err := r.client.CancelRun(ctx, runID); err != nil {
		return fmt.Errorf("github actions: cancel run: %w", err)
	}
	r.logger.Debugw("cancelled GitHub Actions workflow run",
		"github_run_id", runID,
	)
	return nil
}

// mapRunStatus maps a GitHub Actions run status/conclusion pair to a
// BuildStatus.
func mapRunStatus(status, conclusion string) entity.BuildStatus {
	switch platformgithubactions.ParseRunStatus(status, conclusion) {
	case platformgithubactions.RunStatusAccepted:
		return entity.BuildStatusAccepted
	case platformgithubactions.RunStatusRunning:
		return entity.BuildStatusRunning
	case platformgithubactions.RunStatusSucceeded:
		return entity.BuildStatusSucceeded
	case platformgithubactions.RunStatusFailed:
		return entity.BuildStatusFailed
	case platformgithubactions.RunStatusCancelled:
		return entity.BuildStatusCancelled
	default:
		// Unrecognised GitHub status/conclusion. Do NOT assume terminal:
		// Unknown is non-terminal, so the buildsignal poll loop keeps waiting
		// rather than failing the build on a state this code does not
		// understand.
		return entity.BuildStatusUnknown
	}
}
