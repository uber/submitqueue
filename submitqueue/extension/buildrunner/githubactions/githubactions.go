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

	"go.uber.org/zap"

	"github.com/uber/submitqueue/platform/base/change"
	platformgithubactions "github.com/uber/submitqueue/platform/extension/buildrunner/githubactions"
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

// Params holds the dependencies for a GitHub Actions BuildRunner. The wiring
// layer is responsible for supplying non-nil Client/Logger; a nil value
// panics on first use rather than being validated here.
type Params struct {
	// Config holds the per-queue identity for this BuildRunner.
	Config buildrunner.Config
	// Client is a pre-constructed GitHub Actions client, bound to one
	// repository and workflow. The wiring layer builds it once via
	// platformgithubactions.NewClient, with the GitHub API root (via
	// platform/http.BaseURLTransport) and auth already configured.
	Client *platformgithubactions.Client
	// Resolver resolves a batch's changes (base and head batches).
	Resolver changeset.Resolver
	// Logger is the structured logger.
	Logger *zap.SugaredLogger
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
	client      *platformgithubactions.Client
	resolver    changeset.Resolver
	logger      *zap.SugaredLogger
}

var _ buildrunner.BuildRunner = (*runner)(nil)

// NewBuildRunner constructs a GitHub Actions-backed BuildRunner bound to one
// repository/workflow and one queue config.
func NewBuildRunner(params Params) buildrunner.BuildRunner {
	ref := params.Ref
	if ref == "" {
		ref = defaultRef
	}

	return newRunner(
		params.Config,
		ref,
		params.ExtraInputs,
		params.Client,
		params.Resolver,
		params.Logger.Named("githubactions_buildrunner"),
	)
}

func newRunner(cfg buildrunner.Config, ref string, extraInputs map[string]string, c *platformgithubactions.Client, resolver changeset.Resolver, logger *zap.SugaredLogger) *runner {
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

	resp, err := r.client.DispatchWorkflow(ctx, platformgithubactions.DispatchWorkflowRequest{
		Ref:              r.ref,
		ReturnRunDetails: true,
		Inputs:           inputs,
	})
	if err != nil {
		return entity.BuildID{}, fmt.Errorf("github actions: dispatch workflow: %w", err)
	}
	if resp.WorkflowRunID <= 0 {
		return entity.BuildID{}, fmt.Errorf("github actions: dispatch workflow: response missing workflow_run_id (requires X-GitHub-Api-Version and return_run_details support)")
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
