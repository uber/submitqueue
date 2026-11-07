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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	phttp "github.com/uber/submitqueue/platform/http"
)

const (
	testQueue = "monorepo"
	testSHA   = "9f2c1d0e3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d"
)

func TestResolver_Resolve(t *testing.T) {
	tests := []struct {
		name     string
		queue    string
		target   Target
		status   int
		body     string
		wantPath string
		want     string
		wantErr  bool
	}{
		{
			name:     "returns the branch tip",
			queue:    testQueue,
			target:   Target{Owner: "org", Repo: "repo", Branch: "main"},
			status:   http.StatusOK,
			body:     fmt.Sprintf(`{"ref":"refs/heads/main","object":{"sha":%q,"type":"commit"}}`, testSHA),
			wantPath: "/repos/org/repo/git/ref/heads/main",
			want:     testSHA,
		},
		{
			name:     "reads a branch whose name has a slash",
			queue:    testQueue,
			target:   Target{Owner: "org", Repo: "repo", Branch: "release/1.0"},
			status:   http.StatusOK,
			body:     fmt.Sprintf(`{"object":{"sha":%q}}`, testSHA),
			wantPath: "/repos/org/repo/git/ref/heads/release/1.0",
			want:     testSHA,
		},
		{
			name:    "queue without a target fails",
			queue:   "other",
			target:  Target{Owner: "org", Repo: "repo", Branch: "main"},
			wantErr: true,
		},
		{
			name:     "missing branch fails",
			queue:    testQueue,
			target:   Target{Owner: "org", Repo: "repo", Branch: "gone"},
			status:   http.StatusNotFound,
			body:     `{"message":"Not Found"}`,
			wantPath: "/repos/org/repo/git/ref/heads/gone",
			wantErr:  true,
		},
		{
			name:     "malformed response fails",
			queue:    testQueue,
			target:   Target{Owner: "org", Repo: "repo", Branch: "main"},
			status:   http.StatusOK,
			body:     `not json`,
			wantPath: "/repos/org/repo/git/ref/heads/main",
			wantErr:  true,
		},
		{
			name:     "abbreviated SHA fails",
			queue:    testQueue,
			target:   Target{Owner: "org", Repo: "repo", Branch: "main"},
			status:   http.StatusOK,
			body:     `{"object":{"sha":"9f2c1d0"}}`,
			wantPath: "/repos/org/repo/git/ref/heads/main",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.EscapedPath()
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			t.Cleanup(server.Close)
			client, err := phttp.NewClient(server.URL)
			require.NoError(t, err)

			got, err := New(Params{HTTPClient: client, Targets: map[string]Target{testQueue: tt.target}}).Resolve(context.Background(), tt.queue)

			assert.Equal(t, tt.wantPath, gotPath)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
