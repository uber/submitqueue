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

package githubapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	phttp "github.com/uber/submitqueue/platform/http"
)

func TestSetRESTHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/repos/o/r", nil)
	require.NoError(t, err)

	SetRESTHeaders(req)

	assert.Equal(t, "application/vnd.github+json", req.Header.Get("Accept"))
	assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
	assert.Equal(t, Version, req.Header.Get("X-GitHub-Api-Version"))
}

type pullRequestData struct {
	Repository struct {
		PullRequest struct {
			HeadRefOid string `json:"headRefOid"`
		} `json:"pullRequest"`
	} `json:"repository"`
}

func TestQueryGraphQL(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		body           string
		wantHead       string
		wantErr        bool
		wantStatus     int
		wantGraphQLErr bool
	}{
		{
			name:     "decodes data",
			status:   http.StatusOK,
			body:     `{"data":{"repository":{"pullRequest":{"headRefOid":"abc"}}}}`,
			wantHead: "abc",
		},
		{
			name:   "null data leaves the result untouched",
			status: http.StatusOK,
			body:   `{"data":null}`,
		},
		{
			name:   "missing data leaves the result untouched",
			status: http.StatusOK,
			body:   `{}`,
		},
		{
			name:       "non-200 status is a status error",
			status:     http.StatusBadGateway,
			body:       `upstream unavailable`,
			wantErr:    true,
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "401 is a status error",
			status:     http.StatusUnauthorized,
			body:       `{"message":"Bad credentials"}`,
			wantErr:    true,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:    "undecodable body",
			status:  http.StatusOK,
			body:    `not json`,
			wantErr: true,
		},
		{
			name:           "GraphQL errors",
			status:         http.StatusOK,
			body:           `{"errors":[{"type":"NOT_FOUND","message":"Could not resolve to a Repository"},{"type":"FORBIDDEN","message":"Resource not accessible"}]}`,
			wantErr:        true,
			wantGraphQLErr: true,
		},
		{
			name:           "GraphQL errors alongside data",
			status:         http.StatusOK,
			body:           `{"data":{"repository":{"pullRequest":null}},"errors":[{"type":"NOT_FOUND","message":"Could not resolve to a PullRequest"}]}`,
			wantErr:        true,
			wantGraphQLErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod, gotPath, gotContentType string
			var gotReq GraphQLRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath, gotContentType = r.Method, r.URL.Path, r.Header.Get("Content-Type")
				raw, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(raw, &gotReq)
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			t.Cleanup(server.Close)
			client, err := phttp.NewClient(server.URL)
			require.NoError(t, err)

			req := GraphQLRequest{Query: "query { viewer { login } }", Variables: map[string]any{"number": float64(7)}}
			var data pullRequestData
			err = QueryGraphQL(context.Background(), client, req, &data)

			assert.Equal(t, http.MethodPost, gotMethod)
			assert.Equal(t, "/graphql", gotPath)
			assert.Equal(t, "application/json", gotContentType)
			assert.Equal(t, req, gotReq)

			var statusErr *phttp.StatusError
			var gqlErr graphQLErrors
			if !tt.wantErr {
				require.NoError(t, err)
				assert.Equal(t, tt.wantHead, data.Repository.PullRequest.HeadRefOid)
				return
			}
			require.Error(t, err)
			if tt.wantStatus != 0 {
				require.True(t, errors.As(err, &statusErr))
				assert.Equal(t, tt.wantStatus, statusErr.StatusCode)
			} else {
				assert.False(t, errors.As(err, &statusErr))
			}
			assert.Equal(t, tt.wantGraphQLErr, errors.As(err, &gqlErr))
		})
	}
}
