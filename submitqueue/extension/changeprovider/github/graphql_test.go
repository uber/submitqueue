package github

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildGraphQLRequest(t *testing.T) {
	tests := []struct {
		name     string
		org      string
		repo     string
		prNumber int
		cursor   string
		wantVars map[string]any
	}{
		{
			name:     "no cursor",
			org:      "uber",
			repo:     "submitqueue",
			prNumber: 123,
			cursor:   "",
			wantVars: map[string]any{
				"owner":       "uber",
				"repo":        "submitqueue",
				"prNumber":    123,
				"filesCursor": "",
			},
		},
		{
			name:     "with cursor",
			org:      "myorg",
			repo:     "myrepo",
			prNumber: 456,
			cursor:   "cursor_token_xyz",
			wantVars: map[string]any{
				"owner":       "myorg",
				"repo":        "myrepo",
				"prNumber":    456,
				"filesCursor": "cursor_token_xyz",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildGraphQLRequest(tt.org, tt.repo, tt.prNumber, tt.cursor)
			assert.Equal(t, pullRequestQuery, req.Query)
			assert.Equal(t, tt.wantVars, req.Variables)
		})
	}
}

func TestPullRequestQueryData_DecodesGraphQLData(t *testing.T) {
	tests := []struct {
		name string
		data string
		want pullRequestData
	}{
		{
			name: "pull request with author and files",
			data: `{
				"repository": {
					"pullRequest": {
						"number": 42,
						"headRefOid": "abc123",
						"author": {"login": "octocat", "name": "The Octocat", "email": "octocat@example.com"},
						"files": {
							"totalCount": 1,
							"pageInfo": {"endCursor": "cur1", "hasNextPage": false},
							"nodes": [{"path": "main.go", "additions": 10, "deletions": 2, "changeType": "MODIFIED"}]
						}
					}
				}
			}`,
			want: pullRequestData{
				Number:     42,
				HeadRefOid: "abc123",
				Author:     authorData{Login: "octocat", Name: "The Octocat", Email: "octocat@example.com"},
				Files: filesData{
					TotalCount: 1,
					PageInfo:   pageInfo{EndCursor: "cur1", HasNextPage: false},
					Nodes:      []fileNode{{Path: "main.go", Additions: 10, Deletions: 2, ChangeType: "MODIFIED"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got pullRequestQueryData
			require.NoError(t, json.Unmarshal([]byte(tt.data), &got))
			assert.Equal(t, tt.want, got.Repository.PullRequest)
		})
	}
}
