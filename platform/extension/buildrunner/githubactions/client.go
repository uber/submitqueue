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

// Package githubactions provides the HTTP client and GitHub Actions-specific
// facts (run status/conclusion vocabulary and run id encoding) shared by
// every domain's GitHub Actions-backed BuildRunner. It intentionally holds no
// BuildRunner interface or domain entity types — each domain (submitqueue,
// stovepipe, ...) defines its own BuildRunner and its own BuildStatus, and
// adapts this package's RunStatus to it. See
// platform/extension/buildrunner/buildkite's README for the analogous
// rationale applied to the Buildkite backend.
package githubactions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const githubAPIVersion = "2026-03-10"

// Client is a thin wrapper around the GitHub Actions REST endpoints a
// BuildRunner needs: workflow dispatch, get workflow run, and cancel run. It
// is bound to a single repository and workflow.
type Client struct {
	httpClient *http.Client
	owner      string
	repo       string
	workflowID string
}

// NewClient wraps a pre-configured *http.Client as a GitHub Actions Client
// bound to one repository and workflow. The caller is responsible for base
// URL resolution (e.g. via platform/http.BaseURLTransport, typically
// "https://api.github.com") and auth (a token with actions:read/actions:write
// injected via a transport).
func NewClient(httpClient *http.Client, owner, repo, workflowID string) *Client {
	return &Client{
		httpClient: httpClient,
		owner:      owner,
		repo:       repo,
		workflowID: workflowID,
	}
}

// Owner is the repository owner or organization this Client is bound to.
func (c *Client) Owner() string { return c.owner }

// Repo is the repository name this Client is bound to.
func (c *Client) Repo() string { return c.repo }

// WorkflowID is the workflow file name or numeric workflow ID this Client is
// bound to.
func (c *Client) WorkflowID() string { return c.workflowID }

// DispatchWorkflowRequest is the payload for POST .../dispatches.
type DispatchWorkflowRequest struct {
	Ref              string            `json:"ref"`
	ReturnRunDetails bool              `json:"return_run_details,omitempty"`
	Inputs           map[string]string `json:"inputs,omitempty"`
}

// DispatchWorkflowResponse is the subset of fields callers need from a
// dispatch-workflow response.
type DispatchWorkflowResponse struct {
	WorkflowRunID int64  `json:"workflow_run_id"`
	RunURL        string `json:"run_url"`
	HTMLURL       string `json:"html_url"`
}

// WorkflowRun is the subset of a GitHub Actions workflow run object callers
// need.
type WorkflowRun struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	DisplayTitle string `json:"display_title"`
	Status       string `json:"status"`
	Conclusion   string `json:"conclusion"`
	HTMLURL      string `json:"html_url"`
	RunAttempt   int    `json:"run_attempt"`
	Event        string `json:"event"`
	HeadBranch   string `json:"head_branch"`
	CreatedAt    string `json:"created_at"`
}

// DispatchWorkflow dispatches the bound workflow.
func (c *Client) DispatchWorkflow(ctx context.Context, req DispatchWorkflowRequest) (DispatchWorkflowResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return DispatchWorkflowResponse{}, fmt.Errorf("marshal request: %w", err)
	}
	var resp DispatchWorkflowResponse
	if err := c.do(ctx, http.MethodPost, c.workflowPath("dispatches"), body, &resp); err != nil {
		return DispatchWorkflowResponse{}, err
	}
	return resp, nil
}

// GetRun fetches a workflow run by its GitHub run id.
func (c *Client) GetRun(ctx context.Context, runID int64) (WorkflowRun, error) {
	var run WorkflowRun
	if err := c.do(ctx, http.MethodGet, c.runPath(runID), nil, &run); err != nil {
		return WorkflowRun{}, err
	}
	return run, nil
}

// CancelRun requests cancellation of the workflow run. Returns nil when the
// run is already terminal or otherwise not cancellable (HTTP 409/422) — the
// BuildRunner contract treats that as a no-op.
func (c *Client) CancelRun(ctx context.Context, runID int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.runPath(runID)+"/cancel", nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch resp.StatusCode {
	case http.StatusAccepted, http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return nil
	case http.StatusConflict, http.StatusUnprocessableEntity:
		// Already terminal or otherwise not cancellable: no-op per
		// BuildRunner.Cancel contract.
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("workflow run not found")
	default:
		return fmt.Errorf("unexpected status %d from cancel", resp.StatusCode)
	}
}

func (c *Client) workflowPath(suffix string) string {
	path := fmt.Sprintf(
		"/repos/%s/%s/actions/workflows/%s",
		url.PathEscape(c.owner),
		url.PathEscape(c.repo),
		url.PathEscape(c.workflowID),
	)
	if suffix != "" {
		path += "/" + suffix
	}
	return path
}

func (c *Client) runPath(runID int64) string {
	return fmt.Sprintf(
		"/repos/%s/%s/actions/runs/%s",
		url.PathEscape(c.owner),
		url.PathEscape(c.repo),
		url.PathEscape(strconv.FormatInt(runID, 10)),
	)
}

func (c *Client) do(ctx context.Context, method, rawURL string, body []byte, out any) error {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("GitHub API returned 404 for %s %s", method, rawURL)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("API returned status %d: %s", resp.StatusCode, respBody)
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
}

// EncodeRunID encodes a GitHub Actions workflow run id as an opaque build id
// string.
func EncodeRunID(runID int64) string {
	return strconv.FormatInt(runID, 10)
}

// ParseRunID is the inverse of EncodeRunID.
func ParseRunID(id string) (int64, error) {
	runID, err := strconv.ParseInt(id, 10, 64)
	if err != nil || runID <= 0 {
		return 0, fmt.Errorf("invalid build ID %q", id)
	}
	return runID, nil
}

// RunStatus is GitHub Actions' own run status/conclusion vocabulary,
// collapsed into the five states every domain's BuildStatus already
// distinguishes.
type RunStatus string

const (
	// RunStatusUnknown is returned for a raw status/conclusion pair this
	// package does not recognize. Not terminal — callers should keep polling
	// rather than treat it as a final outcome.
	RunStatusUnknown RunStatus = ""
	// RunStatusAccepted means the run has been accepted for execution but has
	// not started yet (GitHub: queued, requested, waiting, pending).
	RunStatusAccepted RunStatus = "accepted"
	// RunStatusRunning means the run is currently executing (GitHub:
	// in_progress).
	RunStatusRunning RunStatus = "running"
	// RunStatusSucceeded means the run completed successfully (GitHub:
	// completed with conclusion success).
	RunStatusSucceeded RunStatus = "succeeded"
	// RunStatusFailed means the run completed without a passing result
	// (GitHub: completed with any conclusion other than success/cancelled).
	RunStatusFailed RunStatus = "failed"
	// RunStatusCancelled means the run was cancelled (GitHub: completed with
	// conclusion cancelled).
	RunStatusCancelled RunStatus = "cancelled"
)

// ParseRunStatus maps a raw GitHub Actions run status/conclusion pair to a
// RunStatus. conclusion is only consulted when status is "completed". An
// unrecognized status, or a completed run with an empty conclusion, maps to
// RunStatusUnknown rather than being assumed terminal.
func ParseRunStatus(status, conclusion string) RunStatus {
	switch status {
	case "queued", "requested", "waiting", "pending":
		return RunStatusAccepted
	case "in_progress":
		return RunStatusRunning
	case "completed":
		switch conclusion {
		case "success":
			return RunStatusSucceeded
		case "cancelled":
			return RunStatusCancelled
		case "":
			return RunStatusUnknown
		default:
			return RunStatusFailed
		}
	default:
		return RunStatusUnknown
	}
}

// RunMetadata builds the caller-facing metadata map for a workflow run: the
// run's own identity and outcome fields, keyed for direct use as (or merge
// into) a domain's BuildMetadata.
func RunMetadata(run WorkflowRun) map[string]string {
	meta := map[string]string{
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
