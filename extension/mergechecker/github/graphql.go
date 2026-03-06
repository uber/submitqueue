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

package github

import (
	"encoding/json"
	"fmt"
	"strings"

	entitygithub "github.com/uber/submitqueue/entity/github"
)

// graphQLRequest is the request body for the GitHub GraphQL API.
type graphQLRequest struct {
	// Query is the GraphQL query string.
	Query string `json:"query"`
}

// graphQLResponse is the top-level response from the GitHub GraphQL API.
type graphQLResponse struct {
	// Data contains the query results keyed by alias.
	Data map[string]json.RawMessage `json:"data"`
	// Errors contains any GraphQL errors.
	Errors []graphQLError `json:"errors"`
}

// graphQLError represents a single GraphQL error.
type graphQLError struct {
	// Message is the error message.
	Message string `json:"message"`
}

// repositoryResponse represents a repository query result.
type repositoryResponse struct {
	// PullRequest contains the PR data.
	PullRequest prResponse `json:"pullRequest"`
}

// prResponse represents the fields fetched for a single pull request.
type prResponse struct {
	// Number is the PR number.
	Number int `json:"number"`
	// Mergeable is the mergeability state.
	Mergeable string `json:"mergeable"`
	// BaseRefName is the base branch name.
	BaseRefName string `json:"baseRefName"`
	// HeadRefName is the head branch name.
	HeadRefName string `json:"headRefName"`
	// HeadRefOid is the head commit SHA.
	HeadRefOid string `json:"headRefOid"`
	// State is the PR state (OPEN, CLOSED, MERGED).
	State string `json:"state"`
}

// buildGraphQLQuery builds a batched GraphQL query for multiple PRs.
// Each PR gets an alias like "pr0", "pr1", etc.
func buildGraphQLQuery(changeIDs []entitygithub.ChangeID) string {
	var sb strings.Builder
	sb.WriteString("query {")

	for i, cid := range changeIDs {
		fmt.Fprintf(&sb, `
  pr%d: repository(owner: %q, name: %q) {
    pullRequest(number: %d) {
      number
      mergeable
      baseRefName
      headRefName
      headRefOid
      state
    }
  }`, i, cid.Org, cid.Repo, cid.PRNumber)
	}

	sb.WriteString("\n}")
	return sb.String()
}

// parseGraphQLResponse parses the GraphQL response body and returns a map of PR number to PRInfo.
func parseGraphQLResponse(body []byte, changeIDs []entitygithub.ChangeID) (map[int]PRInfo, error) {
	var resp graphQLResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse GraphQL response: %w", err)
	}

	if len(resp.Errors) > 0 {
		messages := make([]string, len(resp.Errors))
		for i, e := range resp.Errors {
			messages[i] = e.Message
		}
		return nil, fmt.Errorf("GraphQL errors: %s", strings.Join(messages, "; "))
	}

	result := make(map[int]PRInfo, len(changeIDs))
	for i := range changeIDs {
		alias := fmt.Sprintf("pr%d", i)
		raw, ok := resp.Data[alias]
		if !ok {
			return nil, fmt.Errorf("missing alias %q in GraphQL response", alias)
		}

		var repoResp repositoryResponse
		if err := json.Unmarshal(raw, &repoResp); err != nil {
			return nil, fmt.Errorf("failed to parse alias %q: %w", alias, err)
		}

		pr := repoResp.PullRequest
		result[pr.Number] = PRInfo{
			Number:      pr.Number,
			Mergeable:   PRMergeableState(pr.Mergeable),
			BaseRefName: pr.BaseRefName,
			HeadRefName: pr.HeadRefName,
			HeadRefOid:  pr.HeadRefOid,
			State:       PRState(pr.State),
		}
	}

	return result, nil
}
