package github

import (
	"io"
	"net/http"
	"strings"
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

func TestParseGraphQLResponse(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantErr    bool
		wantData   *pullRequestData
	}{
		{
			name:       "success",
			statusCode: http.StatusOK,
			body: `{
				"data": {
					"repository": {
						"pullRequest": {
							"number": 42,
							"headRefOid": "abc123",
							"author": {"login": "octocat", "name": "The Octocat", "email": "octocat@example.com"},
							"files": {
								"totalCount": 1,
								"pageInfo": {"endCursor": "cur1", "hasNextPage": false},
								"nodes": [{"path": "main.go", "additions": 10, "deletions": 2, "changeType": "MODIFIED", "patch": "diff content"}]
							}
						}
					}
				}
			}`,
			wantData: &pullRequestData{
				Number:     42,
				HeadRefOid: "abc123",
				Author:     authorData{Login: "octocat", Name: "The Octocat", Email: "octocat@example.com"},
				Files: filesData{
					TotalCount: 1,
					PageInfo:   pageInfo{EndCursor: "cur1", HasNextPage: false},
					Nodes:      []fileNode{{Path: "main.go", Additions: 10, Deletions: 2, ChangeType: "MODIFIED", Patch: "diff content"}},
				},
			},
		},
		{
			name:       "non-200 status",
			statusCode: http.StatusInternalServerError,
			body:       `{"message":"Internal Server Error"}`,
			wantErr:    true,
		},
		{
			name:       "404 not found",
			statusCode: http.StatusNotFound,
			body:       `{"message":"Not Found"}`,
			wantErr:    true,
		},
		{
			name:       "invalid JSON",
			statusCode: http.StatusOK,
			body:       `{invalid json`,
			wantErr:    true,
		},
		{
			name:       "GraphQL errors",
			statusCode: http.StatusOK,
			body:       `{"errors":[{"message":"Field doesn't exist","type":"INVALID_FIELD"}]}`,
			wantErr:    true,
		},
		{
			name:       "multiple GraphQL errors",
			statusCode: http.StatusOK,
			body:       `{"errors":[{"message":"Not Found","type":"NOT_FOUND"},{"message":"Forbidden","type":"FORBIDDEN"}]}`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}

			got, err := parseGraphQLResponse(resp)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantData, got)
		})
	}
}
