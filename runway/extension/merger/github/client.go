// Copyright (c) 2026 Uber Technologies, Inc.
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

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	phttp "github.com/uber/submitqueue/platform/http"
)

const githubAPIVersion = "2026-03-10"

// pullStateOpen is the REST API state of a pull request that is neither merged
// nor closed.
const pullStateOpen = "open"

// mergeableStateDirty is GitHub's mergeable_state for a pull request whose
// head conflicts with its base.
const mergeableStateDirty = "dirty"

// Async merge request statuses.
const (
	mergeStatusPending  = "pending"
	mergeStatusMerged   = "merged"
	mergeStatusEnqueued = "enqueued"
	mergeStatusFailed   = "failed"
)

// mergeActionDirect merges immediately instead of entering GitHub's own merge
// queue: Runway is the queue, so a second one would reorder what it decided.
const mergeActionDirect = "direct_merge"

// pullRequest is the subset of a GitHub pull request the merger reads.
type pullRequest struct {
	Number         int    `json:"number"`
	State          string `json:"state"`
	Draft          bool   `json:"draft"`
	Merged         bool   `json:"merged"`
	Mergeable      *bool  `json:"mergeable"`
	MergeableState string `json:"mergeable_state"`
	Head           gitRef `json:"head"`
	Base           gitRef `json:"base"`
}

// gitRef is a branch name and the commit it points at.
type gitRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// stack is a GitHub pull request stack; PullRequests are ordered bottom to top.
type stack struct {
	Number       int         `json:"number"`
	Open         bool        `json:"open"`
	Base         gitRef      `json:"base"`
	PullRequests []stackPull `json:"pull_requests"`
}

// stackPull is one member of a stack.
type stackPull struct {
	Number   int     `json:"number"`
	State    string  `json:"state"`
	MergedAt *string `json:"merged_at"`
	Head     gitRef  `json:"head"`
}

func (p stackPull) unmerged() bool {
	return p.State == pullStateOpen && p.MergedAt == nil
}

// asyncMergeRequest is the body of PUT .../merge-async.
type asyncMergeRequest struct {
	SHA         string `json:"sha"`
	MergeMethod string `json:"merge_method"`
	MergeAction string `json:"merge_action"`
	BypassRules bool   `json:"bypass_rules,omitempty"`
}

// asyncMergeStatus is both the submit response and the poll response of the
// async merge API.
type asyncMergeStatus struct {
	Status  string `json:"status"`
	Details struct {
		Message         string `json:"message"`
		UUID            string `json:"uuid"`
		SHA             string `json:"sha"`
		ExpectedHeadSHA string `json:"expected_head_sha"`
		MergeMethod     string `json:"merge_method"`
	} `json:"details"`
}

// submitOutcome classifies a merge-async submission.
type submitOutcome int

const (
	// submitAccepted means a merge request is in flight and must be polled;
	// that includes adopting one an earlier delivery already submitted.
	submitAccepted submitOutcome = iota + 1
	// submitDone means GitHub answered with a final status straight away.
	submitDone
	// submitRejected means GitHub refused the merge as asked (closed, draft,
	// failed validation); retrying the same request cannot change that.
	submitRejected
	// submitConflicting means a merge request is already in flight for the pull
	// request but for a different head or merge method, so it is not this one.
	submitConflicting
)

// client is a thin wrapper over the GitHub REST endpoints the merger needs,
// bound to one repository. Base URL and auth come from the injected
// *http.Client's transport.
type client struct {
	httpClient *http.Client
	owner      string
	repo       string
}

func (c *client) getPull(ctx context.Context, number int) (pullRequest, error) {
	var pr pullRequest
	if err := c.getJSON(ctx, c.repoPath("pulls", strconv.Itoa(number)), &pr); err != nil {
		return pullRequest{}, fmt.Errorf("get pull request #%d: %w", number, err)
	}
	return pr, nil
}

// issueEvent is one entry of a pull request's issue event timeline.
type issueEvent struct {
	Event    string `json:"event"`
	CommitID string `json:"commit_id"`
}

// issueEventsPageSize is the largest page the issue events endpoint serves.
const issueEventsPageSize = 100

// getMergeCommit returns the commit a merged pull request landed as, and false
// while GitHub has not recorded it yet. API version 2026-03-10 dropped
// merge_commit_sha from the pull request resource; the "merged" issue event is
// where that version still reports it.
func (c *client) getMergeCommit(ctx context.Context, number int) (string, bool, error) {
	var mergeCommit string
	for page := 1; ; page++ {
		var events []issueEvent
		path := fmt.Sprintf("%s?per_page=%d&page=%d", c.repoPath("issues", strconv.Itoa(number), "events"), issueEventsPageSize, page)
		if err := c.getJSON(ctx, path, &events); err != nil {
			return "", false, fmt.Errorf("list events of pull request #%d: %w", number, err)
		}
		for _, e := range events {
			if e.Event == "merged" && e.CommitID != "" {
				mergeCommit = e.CommitID
			}
		}
		if len(events) < issueEventsPageSize {
			return mergeCommit, mergeCommit != "", nil
		}
	}
}

// getStackForPull returns the stack containing the pull request, and false
// when it belongs to none.
func (c *client) getStackForPull(ctx context.Context, number int) (stack, bool, error) {
	var stacks []stack
	path := c.repoPath("stacks") + "?pull_request=" + strconv.Itoa(number)
	if err := c.getJSON(ctx, path, &stacks); err != nil {
		return stack{}, false, fmt.Errorf("list stacks for pull request #%d: %w", number, err)
	}
	for _, s := range stacks {
		for _, p := range s.PullRequests {
			if p.Number == number {
				return s, true, nil
			}
		}
	}
	return stack{}, false, nil
}

// submitMerge asks GitHub to merge the pull request (and, for a stacked one,
// every unmerged pull request below it). A 409 carries the request already in
// flight; it is adopted only when it merges the same head with the same method,
// so a redelivered merge converges on its own GitHub request without taking
// over a different one.
func (c *client) submitMerge(ctx context.Context, number int, req asyncMergeRequest) (asyncMergeStatus, submitOutcome, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return asyncMergeStatus{}, 0, fmt.Errorf("marshal merge request: %w", err)
	}
	path := c.repoPath("pulls", strconv.Itoa(number), "merge-async")
	status, respBody, err := phttp.SendRequest(ctx, c.httpClient, http.MethodPut, path, body, setHeaders)
	if err != nil {
		return asyncMergeStatus{}, 0, fmt.Errorf("merge pull request #%d: %w", number, err)
	}

	var out asyncMergeStatus
	switch status {
	case http.StatusOK, http.StatusAccepted, http.StatusConflict:
		if err := json.Unmarshal(respBody, &out); err != nil {
			return asyncMergeStatus{}, 0, fmt.Errorf("merge pull request #%d: unmarshal response: %w", number, err)
		}
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		_ = json.Unmarshal(respBody, &out)
		return out, submitRejected, nil
	default:
		return asyncMergeStatus{}, 0, fmt.Errorf("merge pull request #%d: %w", number, phttp.NewStatusError(status, respBody))
	}

	switch {
	case status == http.StatusConflict && out.Details.UUID != "":
		if out.Details.ExpectedHeadSHA != req.SHA || out.Details.MergeMethod != req.MergeMethod {
			return out, submitConflicting, nil
		}
		return out, submitAccepted, nil
	case status == http.StatusConflict:
		return asyncMergeStatus{}, 0, fmt.Errorf("merge pull request #%d: %w", number, phttp.NewStatusError(status, respBody))
	case out.Status == mergeStatusPending && out.Details.UUID != "":
		return out, submitAccepted, nil
	default:
		return out, submitDone, nil
	}
}

func (c *client) getMergeStatus(ctx context.Context, number int, uuid string) (asyncMergeStatus, error) {
	var out asyncMergeStatus
	path := c.repoPath("pulls", strconv.Itoa(number), "merge-async", uuid)
	if err := c.getJSON(ctx, path, &out); err != nil {
		return asyncMergeStatus{}, fmt.Errorf("get merge status %s for pull request #%d: %w", uuid, number, err)
	}
	return out, nil
}

func (c *client) getJSON(ctx context.Context, path string, out any) error {
	status, respBody, err := phttp.SendRequest(ctx, c.httpClient, http.MethodGet, path, nil, setHeaders)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return phttp.NewStatusError(status, respBody)
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	return nil
}

func (c *client) repoPath(segments ...string) string {
	path := "/repos/" + url.PathEscape(c.owner) + "/" + url.PathEscape(c.repo)
	for _, s := range segments {
		path += "/" + url.PathEscape(s)
	}
	return path
}

func setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
}
