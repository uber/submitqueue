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

	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
	phttp "github.com/uber/submitqueue/platform/http"
)

// maxCheckContexts bounds the checks read; one past it still surfaces through
// GitHub's BLOCKED merge state, only without its name.
const maxCheckContexts = 100

const pullRequestStateQuery = `
query($owner: String!, $repo: String!, $number: Int!, $maxChecks: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      headRefOid
      reviewDecision
      mergeStateStatus
      commits(last: 1) {
        nodes {
          commit {
            statusCheckRollup {
              contexts(first: $maxChecks) {
                nodes {
                  __typename
                  ... on CheckRun {
                    name
                    status
                    conclusion
                    isRequired(pullRequestNumber: $number)
                  }
                  ... on StatusContext {
                    context
                    state
                    isRequired(pullRequestNumber: $number)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
`

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

type graphqlResponse struct {
	Data struct {
		Repository struct {
			PullRequest pullRequestNode `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
	Errors []graphqlError `json:"errors"`
}

type graphqlError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type pullRequestNode struct {
	HeadRefOid       string `json:"headRefOid"`
	ReviewDecision   string `json:"reviewDecision"`
	MergeStateStatus string `json:"mergeStateStatus"`
	Commits          struct {
		Nodes []struct {
			Commit struct {
				StatusCheckRollup struct {
					Contexts struct {
						Nodes []checkContextNode `json:"nodes"`
					} `json:"contexts"`
				} `json:"statusCheckRollup"`
			} `json:"commit"`
		} `json:"nodes"`
	} `json:"commits"`
}

// checkContextNode is one member of a commit's status check rollup: either a
// check run (Name, Status, Conclusion) or a commit status (Context, State).
type checkContextNode struct {
	Typename   string `json:"__typename"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Context    string `json:"context"`
	State      string `json:"state"`
	IsRequired bool   `json:"isRequired"`
}

// fetchPullRequestState fails, rather than rejects, on a missing pull request:
// the change provider resolved the URI before validators run.
func fetchPullRequestState(ctx context.Context, client *http.Client, cid entitygithub.ChangeID) (pullRequestState, error) {
	body, err := json.Marshal(graphqlRequest{
		Query: pullRequestStateQuery,
		Variables: map[string]any{
			"owner":     cid.Org,
			"repo":      cid.Repo,
			"number":    cid.PRNumber,
			"maxChecks": maxCheckContexts,
		},
	})
	if err != nil {
		return pullRequestState{}, fmt.Errorf("marshal GraphQL request: %w", err)
	}

	// The path is relative: the client's transport roots it at the API.
	status, respBody, err := phttp.SendRequest(ctx, client, http.MethodPost, "/graphql", body, setGraphQLHeaders)
	if err != nil {
		return pullRequestState{}, err
	}
	if status != http.StatusOK {
		return pullRequestState{}, phttp.NewStatusError(status, respBody)
	}

	var resp graphqlResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return pullRequestState{}, fmt.Errorf("decode GraphQL response: %w", err)
	}
	if len(resp.Errors) > 0 {
		return pullRequestState{}, fmt.Errorf("GraphQL errors: %+v", resp.Errors)
	}
	pr := resp.Data.Repository.PullRequest
	if pr.HeadRefOid == "" {
		return pullRequestState{}, fmt.Errorf("GraphQL response has no pull request")
	}
	return toPullRequestState(pr), nil
}

func setGraphQLHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
}

func toPullRequestState(pr pullRequestNode) pullRequestState {
	state := pullRequestState{
		headSHA:          pr.HeadRefOid,
		reviewDecision:   pr.ReviewDecision,
		mergeStateStatus: pr.MergeStateStatus,
	}
	for _, c := range pr.Commits.Nodes {
		for _, node := range c.Commit.StatusCheckRollup.Contexts.Nodes {
			state.checks = append(state.checks, toCheck(node))
		}
	}
	return state
}

func toCheck(node checkContextNode) check {
	if node.Typename == "StatusContext" {
		return check{name: node.Context, required: node.IsRequired, outcome: statusContextOutcome(node.State)}
	}
	return check{name: node.Name, required: node.IsRequired, outcome: checkRunOutcome(node.Status, node.Conclusion)}
}

// checkRunOutcome maps a check run onto an outcome. Neutral and skipped runs
// satisfy a required check on GitHub, so they pass here too.
func checkRunOutcome(status, conclusion string) checkOutcome {
	if status != "COMPLETED" {
		return checkPending
	}
	switch conclusion {
	case "SUCCESS", "NEUTRAL", "SKIPPED":
		return checkPassed
	default:
		return checkFailed
	}
}

func statusContextOutcome(state string) checkOutcome {
	switch state {
	case "SUCCESS":
		return checkPassed
	case "PENDING", "EXPECTED":
		return checkPending
	default:
		return checkFailed
	}
}
