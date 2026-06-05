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

// client is a thin wrapper around the GitHub Actions REST endpoints that
// BuildRunner needs: workflow dispatch, get workflow run, and cancel run.
type client struct {
	httpClient *http.Client
	owner      string
	repo       string
	workflowID string
}

type dispatchWorkflowRequest struct {
	Ref              string            `json:"ref"`
	ReturnRunDetails bool              `json:"return_run_details,omitempty"`
	Inputs           map[string]string `json:"inputs,omitempty"`
}

type dispatchWorkflowResponse struct {
	WorkflowRunID int64  `json:"workflow_run_id"`
	RunURL        string `json:"run_url"`
	HTMLURL       string `json:"html_url"`
}

// workflowRun is the subset of a GitHub Actions workflow run object the
// runner needs.
type workflowRun struct {
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

func (c *client) dispatchWorkflow(ctx context.Context, req dispatchWorkflowRequest) (dispatchWorkflowResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return dispatchWorkflowResponse{}, fmt.Errorf("marshal request: %w", err)
	}
	var resp dispatchWorkflowResponse
	if err := c.do(ctx, http.MethodPost, c.workflowPath("dispatches"), body, &resp); err != nil {
		return dispatchWorkflowResponse{}, err
	}
	return resp, nil
}

func (c *client) getRun(ctx context.Context, runID int64) (workflowRun, error) {
	var run workflowRun
	if err := c.do(ctx, http.MethodGet, c.runPath(runID), nil, &run); err != nil {
		return workflowRun{}, err
	}
	return run, nil
}

func (c *client) cancelRun(ctx context.Context, runID int64) error {
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

func (c *client) workflowPath(suffix string) string {
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

func (c *client) runPath(runID int64) string {
	return fmt.Sprintf(
		"/repos/%s/%s/actions/runs/%s",
		url.PathEscape(c.owner),
		url.PathEscape(c.repo),
		url.PathEscape(strconv.FormatInt(runID, 10)),
	)
}

func (c *client) do(ctx context.Context, method, rawURL string, body []byte, out any) error {
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

func (c *client) setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
}
