package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const defaultBaseURL = "https://api.github.com"

// Sentinel errors for GitHub API operations.
var (
	ErrMergeConflict      = errors.New("merge conflict")
	ErrMergeStatusPending = errors.New("merge status pending")
	ErrNotFound           = errors.New("resource not found")
)

type githubClient struct {
	httpClient *http.Client
	baseURL    string
	owner      string
	repo       string
}

func newClient(params Params) *githubClient {
	baseURL := params.BaseURL
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	httpClient := params.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &githubClient{
		httpClient: httpClient,
		baseURL:    baseURL,
		owner:      params.Owner,
		repo:       params.Repo,
	}
}

// doRequest executes an HTTP request and decodes the JSON response.
// If result is nil, the response body is not decoded.
func (c *githubClient) doRequest(ctx context.Context, method, url string, body interface{}, result interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	c.setHeaders(req)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, respBody)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

// PullRequestStatus contains the mergeable status of a PR.
type PullRequestStatus struct {
	Mergeable      *bool
	MergeableState string
}

// hasMergeConflicts checks if the PR has merge conflicts using the GitHub PR API.
// GET /repos/{owner}/{repo}/pulls/{pull_number}
//
// Note: baseSHA and headSHA are currently unused. We use the PR number to get
// mergeability status from GitHub. However, if someone changes the head after
// submitting the request, that state is invalid. Logic to validate head/base SHA
// will be added later in the merge service layer.
func (c *githubClient) hasMergeConflicts(ctx context.Context, baseSHA string, headSHA string, pr string) (bool, string, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%s", c.baseURL, c.owner, c.repo, pr)

	var result struct {
		Mergeable      *bool  `json:"mergeable"`
		MergeableState string `json:"mergeable_state"`
	}

	if err := c.doRequest(ctx, http.MethodGet, url, nil, &result); err != nil {
		return false, "", err
	}

	// mergeable is null while GitHub is computing merge status
	if result.Mergeable == nil {
		return false, result.MergeableState, ErrMergeStatusPending
	}

	// mergeable=false means there are conflicts
	return !*result.Mergeable, result.MergeableState, nil
}

// mergeRequest represents the request body for the GitHub merge API.
type mergeRequest struct {
	Base          string `json:"base"`
	Head          string `json:"head"`
	CommitMessage string `json:"commit_message,omitempty"`
}

// merge merges the head SHA into the base SHA using the GitHub merges API.
// POST /repos/{owner}/{repo}/merges
func (c *githubClient) merge(ctx context.Context, baseSHA string, headSHA string) error {
	url := fmt.Sprintf("%s/repos/%s/%s/merges", c.baseURL, c.owner, c.repo)

	var bodyReader io.Reader
	bodyBytes, err := json.Marshal(mergeRequest{
		Base: baseSHA,
		Head: headSHA,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal merge request: %w", err)
	}
	bodyReader = bytes.NewReader(bodyBytes)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute merge request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return fmt.Errorf("%w: cannot merge %s into %s", ErrMergeConflict, headSHA, baseSHA)
	case http.StatusNotFound:
		return fmt.Errorf("%w: base or head SHA not found", ErrNotFound)
	default:
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("merge request failed with status %d: %s", resp.StatusCode, respBody)
	}
}

// setHeaders sets common headers for GitHub API requests.
func (c *githubClient) setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
}
