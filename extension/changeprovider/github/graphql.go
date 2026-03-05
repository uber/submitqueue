package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
)

// pullRequestQuery is the GraphQL query to fetch pull request information including files, author, and head SHA.
const pullRequestQuery = `
query($owner: String!, $repo: String!, $prNumber: Int!, $filesCursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      number
      headRefOid
      author {
        login
        ... on User {
          name
          email
        }
      }
      files(first: 100, after: $filesCursor) {
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          path
          additions
          deletions
          changeType
          patch
        }
      }
    }
  }
}
`

// graphqlRequest represents a GraphQL request.
type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

// graphqlResponse represents the top-level GraphQL response.
type graphqlResponse struct {
	Data struct {
		Repository struct {
			PullRequest pullRequestData `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
	Errors []graphqlError `json:"errors,omitempty"`
}

// graphqlError represents a GraphQL error.
type graphqlError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// pullRequestData contains the pull request metadata.
type pullRequestData struct {
	Number     int        `json:"number"`
	HeadRefOid string     `json:"headRefOid"`
	Author     authorData `json:"author"`
	Files      filesData  `json:"files"`
}

// authorData contains the author information.
type authorData struct {
	Login string `json:"login"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// filesData contains the files changed in the pull request.
type filesData struct {
	TotalCount int        `json:"totalCount"`
	PageInfo   pageInfo   `json:"pageInfo"`
	Nodes      []fileNode `json:"nodes"`
}

// pageInfo contains pagination information.
type pageInfo struct {
	EndCursor   string `json:"endCursor"`
	HasNextPage bool   `json:"hasNextPage"`
}

// fileNode represents a single changed file.
type fileNode struct {
	Path       string `json:"path"`
	Additions  int    `json:"additions"`
	Deletions  int    `json:"deletions"`
	ChangeType string `json:"changeType"`
	Patch      string `json:"patch"`
}

// buildGraphQLRequest builds a GraphQL request for fetching pull request data.
func buildGraphQLRequest(org, repo string, prNumber int, cursor string) graphqlRequest {
	return graphqlRequest{
		Query: pullRequestQuery,
		Variables: map[string]any{
			"owner":       org,
			"repo":        repo,
			"prNumber":    prNumber,
			"filesCursor": cursor,
		},
	}
}

// doGraphQLRequest executes a GraphQL HTTP request.
func doGraphQLRequest(
	ctx context.Context,
	bodyBytes []byte,
	client *Client,
	org, repo string,
	metrics tally.Scope,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.GraphQLURL(), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.HTTPClient().Do(req)
	if err != nil {
		metrics.Tagged(map[string]string{
			"org":        org,
			"repo":       repo,
			"error_type": "http_error",
		}).Counter("get_errors").Inc(1)
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}

	return resp, nil
}

// parseGraphQLResponse parses and validates a GraphQL response.
func parseGraphQLResponse(
	resp *http.Response,
	org, repo string,
	prNumber int,
	logger *zap.SugaredLogger,
	metrics tally.Scope,
) (*pullRequestData, error) {
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logger.Errorw("GitHub API error",
			"status", resp.StatusCode,
			"org", org,
			"repo", repo,
			"pr", prNumber,
			"response", string(body),
		)
		metrics.Tagged(map[string]string{
			"org":        org,
			"repo":       repo,
			"error_type": "api_error",
		}).Counter("get_errors").Inc(1)
		return nil, fmt.Errorf("GitHub API returned status %d: %s", resp.StatusCode, string(body))
	}

	var gqlResp graphqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&gqlResp); err != nil {
		metrics.Tagged(map[string]string{
			"org":        org,
			"repo":       repo,
			"error_type": "decode_error",
		}).Counter("get_errors").Inc(1)
		return nil, fmt.Errorf("failed to decode GraphQL response: %w", err)
	}

	if len(gqlResp.Errors) > 0 {
		logger.Errorw("GraphQL errors",
			"org", org,
			"repo", repo,
			"pr", prNumber,
			"errors", gqlResp.Errors,
		)
		metrics.Tagged(map[string]string{
			"org":        org,
			"repo":       repo,
			"error_type": "graphql_error",
		}).Counter("get_errors").Inc(1)
		return nil, fmt.Errorf("GraphQL errors: %+v", gqlResp.Errors)
	}

	return &gqlResp.Data.Repository.PullRequest, nil
}
