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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entitygithub "github.com/uber/submitqueue/platform/base/change/github"
)

func TestBuildGraphQLQuery(t *testing.T) {
	tests := []struct {
		name      string
		changeIDs []entitygithub.ChangeID
		wantParts []string
	}{
		{
			name: "single PR",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "submitqueue", PRNumber: 42, HeadCommitSHA: "abc123"},
			},
			wantParts: []string{
				"query {",
				`pr0: repository(owner: "uber", name: "submitqueue")`,
				"pullRequest(number: 42)",
				"number", "mergeable", "baseRefName", "headRefName", "headRefOid", "state",
			},
		},
		{
			name: "multiple PRs across repos",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 2, HeadCommitSHA: "sha2"},
				{Scheme: "ghe", Org: "corp", Repo: "app", PRNumber: 99, HeadCommitSHA: "sha99"},
			},
			wantParts: []string{
				`pr0: repository(owner: "uber", name: "repo")`,
				"pullRequest(number: 1)",
				`pr1: repository(owner: "uber", name: "repo")`,
				"pullRequest(number: 2)",
				`pr2: repository(owner: "corp", name: "app")`,
				"pullRequest(number: 99)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := buildGraphQLQuery(tt.changeIDs)
			for _, part := range tt.wantParts {
				assert.Contains(t, query, part)
			}
		})
	}
}

func TestParseGraphQLResponse(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		changeIDs []entitygithub.ChangeID
		want      map[int]PRInfo
		wantErr   bool
	}{
		{
			name: "success with two PRs",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 2, HeadCommitSHA: "sha2"},
			},
			body: mustMarshalGraphQLResponse(t, map[string]json.RawMessage{
				"pr0": json.RawMessage(`{"pullRequest":{"number":1,"mergeable":"MERGEABLE","baseRefName":"main","headRefName":"feature-1","headRefOid":"sha1","state":"OPEN"}}`),
				"pr1": json.RawMessage(`{"pullRequest":{"number":2,"mergeable":"CONFLICTING","baseRefName":"feature-1","headRefName":"feature-2","headRefOid":"sha2","state":"OPEN"}}`),
			}),
			want: map[int]PRInfo{
				1: {Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateOpen},
				2: {Number: 2, Mergeable: PRMergeableStateConflicting, BaseRefName: "feature-1", HeadRefName: "feature-2", HeadRefOid: "sha2", State: PRStateOpen},
			},
		},
		{
			name: "GraphQL errors",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			body:    `{"data":null,"errors":[{"message":"Not Found"},{"message":"Forbidden"}]}`,
			wantErr: true,
		},
		{
			name: "invalid JSON",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			body:    `invalid`,
			wantErr: true,
		},
		{
			name: "missing alias",
			changeIDs: []entitygithub.ChangeID{
				{Scheme: "github", Org: "uber", Repo: "repo", PRNumber: 1, HeadCommitSHA: "sha1"},
			},
			body:    `{"data":{}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseGraphQLResponse([]byte(tt.body), tt.changeIDs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

// mustMarshalGraphQLResponse is a test helper to build a GraphQL response body.
func mustMarshalGraphQLResponse(t *testing.T, data map[string]json.RawMessage) string {
	t.Helper()
	resp := graphQLResponse{Data: data}
	body, err := json.Marshal(resp)
	require.NoError(t, err)
	return string(body)
}
