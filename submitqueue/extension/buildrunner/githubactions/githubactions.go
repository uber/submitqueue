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
// The runner dispatches a trusted workflow and passes SubmitQueue's base/head
// change URIs as workflow inputs. Trigger returns the GitHub workflow run ID;
// Status and Cancel use that ID to call GitHub's workflow-run endpoints.
package githubactions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/uber/submitqueue/entity/change"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

const (
	// InputKeyBaseURIs carries the JSON-encoded ordered list of change URIs
	// from dependency batches.
	InputKeyBaseURIs = "sq_base_uris"
	// InputKeyHeadURIs carries the JSON-encoded ordered list of change URIs
	// from the batch under test.
	InputKeyHeadURIs = "sq_head_uris"
	// InputKeyQueue carries the SubmitQueue queue name.
	InputKeyQueue = "sq_queue"
	// InputKeyMetadata carries caller-supplied BuildMetadata as JSON.
	InputKeyMetadata = "sq_metadata"

	defaultRef = "main"
)

// Params holds the dependencies and GitHub workflow identity for a GitHub
// Actions BuildRunner.
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config
	// HTTPClient is a pre-configured GitHub API client. The caller is responsible
	// for base URL resolution (e.g. via httpclient.BaseURLTransport) and auth.
	// The token needs actions:write to dispatch/cancel workflows and actions:read
	// to poll status.
	HTTPClient *http.Client
	// Resolver resolves a batch's changes (base and head batches).
	Resolver changeset.Resolver
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
	// Owner is the repository owner or organization, for example "uber".
	Owner string
	// Repo is the repository name, for example "submitqueue".
	Repo string
	// WorkflowID is the workflow file name or numeric workflow ID accepted by
	// the GitHub Actions API, for example "submitqueue-ci.yml".
	WorkflowID string
	// Ref is the branch, tag, or SHA where the trusted workflow is read from.
	// Defaults to "main".
	Ref string
	// ExtraInputs are copied into every workflow_dispatch request. Reserved
	// sq_* inputs managed by this package override conflicting keys.
	ExtraInputs map[string]string
}

type runner struct {
	cfg         buildrunner.Config
	ref         string
	extraInputs map[string]string
	client      *client
	resolver    changeset.Resolver
	logger      *zap.SugaredLogger
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// NewBuildRunner constructs a GitHub Actions-backed BuildRunner bound to one
// repository/workflow and one queue config.
func NewBuildRunner(params Params) (buildrunner.BuildRunner, error) {
	if err := validateConfig(params.HTTPClient, params.Logger, params.Owner, params.Repo, params.WorkflowID); err != nil {
		return nil, err
	}
	if params.Ref == "" {
		params.Ref = defaultRef
	}

	return newRunner(
		params.Config,
		params.Ref,
		params.ExtraInputs,
		&client{
			httpClient: params.HTTPClient,
			owner:      params.Owner,
			repo:       params.Repo,
			workflowID: params.WorkflowID,
		},
		params.Resolver,
		params.Logger.Named("githubactions_buildrunner"),
	), nil
}

func newRunner(cfg buildrunner.Config, ref string, extraInputs map[string]string, c *client, resolver changeset.Resolver, logger *zap.SugaredLogger) *runner {
	copied := make(map[string]string, len(extraInputs))
	for k, v := range extraInputs {
		copied[k] = v
	}
	return &runner{
		cfg:         cfg,
		ref:         ref,
		extraInputs: copied,
		client:      c,
		resolver:    resolver,
		logger:      logger,
	}
}

func validateConfig(httpClient *http.Client, logger *zap.SugaredLogger, owner, repo, workflowID string) error {
	if httpClient == nil {
		return fmt.Errorf("http client is required")
	}
	if logger == nil {
		return fmt.Errorf("logger is required")
	}
	if owner == "" {
		return fmt.Errorf("owner is required")
	}
	if repo == "" {
		return fmt.Errorf("repo is required")
	}
	if workflowID == "" {
		return fmt.Errorf("workflow ID is required")
	}
	return nil
}

// Trigger dispatches the configured GitHub Actions workflow and returns the
// GitHub workflow run ID as the SubmitQueue build ID. Errors are propagated to
// the caller so the queue consumer can nack and retry.
func (r *runner) Trigger(ctx context.Context, base []entity.Batch, head entity.Batch, metadata entity.BuildMetadata) (entity.BuildID, error) {
	baseChanges, err := buildrunner.ResolveBatches(ctx, r.resolver, base)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("github actions: resolve base: %w", err)
	}
	headChanges, err := r.resolver.ChangesForBatch(ctx, head)
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("github actions: resolve head: %w", err)
	}

	inputs, err := r.dispatchInputs(baseChanges, headChanges, metadata)
	if err != nil {
		return entity.BuildID{}, err
	}

	resp, err := r.client.dispatchWorkflow(ctx, dispatchWorkflowRequest{
		Ref:              r.ref,
		ReturnRunDetails: true,
		Inputs:           inputs,
	})
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("github actions: dispatch workflow: %w", err)
	}
	if resp.WorkflowRunID <= 0 {
		return entity.BuildID{}, fmt.Errorf("github actions: dispatch workflow: response missing workflow_run_id (requires X-GitHub-Api-Version %s and return_run_details support)", githubAPIVersion)
	}

	r.logger.Debugw("dispatched GitHub Actions workflow",
		"owner", r.client.owner,
		"repo", r.client.repo,
		"workflow_id", r.client.workflowID,
		"ref", r.ref,
		"github_run_id", resp.WorkflowRunID,
	)
	return entity.BuildID{ID: strconv.FormatInt(resp.WorkflowRunID, 10)}, nil
}

func (r *runner) dispatchInputs(base, head []change.Change, metadata entity.BuildMetadata) (map[string]string, error) {
	baseJSON, err := json.Marshal(flattenURIs(base))
	if err != nil {
		return nil, fmt.Errorf("marshal base URIs: %w", err)
	}
	headJSON, err := json.Marshal(flattenURIs(head))
	if err != nil {
		return nil, fmt.Errorf("marshal head URIs: %w", err)
	}

	inputs := make(map[string]string, len(r.extraInputs)+4)
	for k, v := range r.extraInputs {
		inputs[k] = v
	}
	inputs[InputKeyBaseURIs] = string(baseJSON)
	inputs[InputKeyHeadURIs] = string(headJSON)
	inputs[InputKeyQueue] = r.cfg.QueueName
	if len(metadata) > 0 {
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, fmt.Errorf("marshal metadata: %w", err)
		}
		inputs[InputKeyMetadata] = string(metadataJSON)
	}
	return inputs, nil
}

// Status fetches the workflow run by GitHub run ID and maps the run's
// status/conclusion onto BuildStatus.
func (r *runner) Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error) {
	runID, err := parseRunID(buildID.ID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("github actions: malformed build ID: %w", err)
	}

	run, err := r.client.getRun(ctx, runID)
	if err != nil {
		return entity.BuildStatusUnknown, nil, fmt.Errorf("github actions: get run: %w", err)
	}
	return mapRunStatus(run.Status, run.Conclusion), metadataFromRun(run), nil
}

// Cancel requests cancellation of the workflow run identified by buildID.
func (r *runner) Cancel(ctx context.Context, buildID entity.BuildID) error {
	runID, err := parseRunID(buildID.ID)
	if err != nil {
		return fmt.Errorf("github actions: malformed build ID: %w", err)
	}
	if err := r.client.cancelRun(ctx, runID); err != nil {
		return fmt.Errorf("github actions: cancel run: %w", err)
	}
	r.logger.Debugw("cancelled GitHub Actions workflow run",
		"github_run_id", runID,
	)
	return nil
}

func metadataFromRun(run workflowRun) entity.BuildMetadata {
	meta := entity.BuildMetadata{
		"github_run_id":        strconv.FormatInt(run.ID, 10),
		"github_run_attempt":   strconv.Itoa(run.RunAttempt),
		"github_status":        run.Status,
		"github_conclusion":    run.Conclusion,
		"github_display_title": run.DisplayTitle,
		"url":                  run.HTMLURL,
	}
	if run.HeadBranch != "" {
		meta["github_head_branch"] = run.HeadBranch
	}
	if run.CreatedAt != "" {
		meta["github_created_at"] = run.CreatedAt
	}
	return meta
}

func mapRunStatus(status, conclusion string) entity.BuildStatus {
	switch status {
	case "queued", "requested", "waiting", "pending":
		return entity.BuildStatusAccepted
	case "in_progress":
		return entity.BuildStatusRunning
	case "completed":
		switch conclusion {
		case "success":
			return entity.BuildStatusSucceeded
		case "cancelled":
			return entity.BuildStatusCancelled
		case "":
			return entity.BuildStatusUnknown
		default:
			// Any completed non-success/non-cancelled conclusion is a terminal
			// failure for SubmitQueue purposes.
			return entity.BuildStatusFailed
		}
	default:
		return entity.BuildStatusUnknown
	}
}

func flattenURIs(changes []change.Change) []string {
	uris := make([]string, 0, len(changes))
	for _, c := range changes {
		uris = append(uris, c.URIs...)
	}
	return uris
}

func parseRunID(id string) (int64, error) {
	runID, err := strconv.ParseInt(id, 10, 64)
	if err != nil || runID <= 0 {
		return 0, fmt.Errorf("invalid build ID %q", id)
	}
	return runID, nil
}
