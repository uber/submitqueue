package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/uber/submitqueue/entity"
	entitygithub "github.com/uber/submitqueue/entity/github"
	"github.com/uber/submitqueue/extension/landprovider"
)

// mergeMethod is the GitHub merge method for the REST API.
type mergeMethod string

const (
	// mergeMethodMerge creates a merge commit.
	mergeMethodMerge mergeMethod = "merge"
	// mergeMethodSquash squashes all commits into one before merging.
	mergeMethodSquash mergeMethod = "squash"
	// mergeMethodRebase rebases commits onto the base branch.
	mergeMethodRebase mergeMethod = "rebase"
)

// mergeRequest is the request body for the GitHub merge PR REST API.
type mergeRequest struct {
	// MergeMethod is the merge strategy to use.
	MergeMethod mergeMethod `json:"merge_method"`
	// SHA is the head SHA to verify before merging.
	SHA string `json:"sha"`
}

// mergeResponse is the response body from the GitHub merge PR REST API.
type mergeResponse struct {
	// Message is a human-readable result message.
	Message string `json:"message"`
}

// mapStrategyToMergeMethod maps a RequestLandStrategy to a GitHub merge method.
func mapStrategyToMergeMethod(strategy entity.RequestLandStrategy) (mergeMethod, error) {
	switch strategy {
	case entity.RequestLandStrategyRebase:
		return mergeMethodRebase, nil
	case entity.RequestLandStrategySquashRebase:
		return mergeMethodSquash, nil
	case entity.RequestLandStrategyMerge:
		return mergeMethodMerge, nil
	default:
		return "", fmt.Errorf("unsupported land strategy: %q", strategy)
	}
}

// mergePR calls the GitHub REST API to merge a single pull request.
// Checks if the PR is already merged before attempting, to ensure idempotency.
func (l *landProvider) mergePR(ctx context.Context, cid entitygithub.ChangeID, strategy entity.RequestLandStrategy) error {
	// Check if already merged before attempting the merge.
	merged, err := l.isPRMerged(ctx, cid)
	if err != nil {
		return fmt.Errorf("failed to check PR merge status: %w", err)
	}
	if merged {
		l.logger.Infow("PR already merged",
			"pr", cid.PRNumber,
			"owner", cid.Org,
			"repo", cid.Repo,
		)
		return landprovider.ErrAlreadyLanded
	}

	// Build the merge request
	method, err := mapStrategyToMergeMethod(strategy)
	if err != nil {
		return err
	}

	reqBody, err := json.Marshal(mergeRequest{
		MergeMethod: method,
		SHA:         cid.HeadCommitSHA,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal merge request: %w", err)
	}

	// PUT /repos/{owner}/{repo}/pulls/{number}/merge
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/merge", l.apiURL, cid.Org, cid.Repo, cid.PRNumber)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := l.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("merge request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read merge response: %w", err)
	}

	// Classify the response: rejection statuses are terminal (no retry),
	// 200 is success, anything else is an infra error (retryable).
	switch resp.StatusCode {
	case http.StatusMethodNotAllowed, http.StatusConflict, http.StatusUnprocessableEntity:
		// 405: merge cannot be performed (conflicts, draft, closed).
		// 409: head SHA does not match the sha parameter (stale change).
		// 422: validation failed (required checks, disallowed merge method).
		var mergeResp mergeResponse
		if err := json.Unmarshal(body, &mergeResp); err != nil {
			return landprovider.WrapLandRejected(
				fmt.Errorf("PR #%d merge rejected (status %d): %s", cid.PRNumber, resp.StatusCode, string(body)),
			)
		}
		return landprovider.WrapLandRejected(
			fmt.Errorf("PR #%d: %s", cid.PRNumber, mergeResp.Message),
		)
	case http.StatusOK:
		// Success — fall through to happy path.
	default:
		return fmt.Errorf("unexpected status %d merging PR #%d: %s", resp.StatusCode, cid.PRNumber, string(body))
	}

	l.logger.Infow("PR merged successfully",
		"pr", cid.PRNumber,
		"owner", cid.Org,
		"repo", cid.Repo,
		"method", method,
	)

	return nil
}

// isPRMerged checks whether a pull request has already been merged.
// Uses the dedicated GitHub "check if merged" endpoint which returns
// 204 if merged, 404 if not merged (empty response body).
func (l *landProvider) isPRMerged(ctx context.Context, cid entitygithub.ChangeID) (bool, error) {
	// GET /repos/{owner}/{repo}/pulls/{number}/merge
	// Returns 204 (merged) or 404 (not merged) with an empty body.
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/merge", l.apiURL, cid.Org, cid.Repo, cid.PRNumber)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create http request: %w", err)
	}

	resp, err := l.httpClient.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("failed to check PR merge status: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d checking merge status of PR #%d", resp.StatusCode, cid.PRNumber)
	}
}
