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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/httpclient"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/mergechecker"
	"go.uber.org/zap/zaptest"
)

func newTestMergeChecker(t *testing.T, serverURL string) mergechecker.MergeChecker {
	t.Helper()
	client, err := httpclient.NewClient(serverURL)
	require.NoError(t, err)
	return NewMergeChecker(Params{
		HTTPClient:   client,
		Logger:       zaptest.NewLogger(t).Sugar(),
		MetricsScope: tally.NoopScope,
	})
}

// Sample 40-char lowercase hex SHAs used across the test cases.
const (
	sha1Full   = "1111111111111111111111111111111111111111"
	sha2Full   = "2222222222222222222222222222222222222222"
	shaAFull   = "abcdef0123456789abcdef0123456789abcdef01"
	shaOldFull = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	shaNewFull = "feedfacefeedfacefeedfacefeedfacefeedface"
)

func graphQLHandler(t *testing.T, prInfos []PRInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t.Helper()

		data := make(map[string]json.RawMessage, len(prInfos))
		for i, pr := range prInfos {
			alias := fmt.Sprintf("pr%d", i)
			prJSON, err := json.Marshal(map[string]any{
				"pullRequest": map[string]any{
					"number":      pr.Number,
					"mergeable":   string(pr.Mergeable),
					"baseRefName": pr.BaseRefName,
					"headRefName": pr.HeadRefName,
					"headRefOid":  pr.HeadRefOid,
					"state":       string(pr.State),
				},
			})
			require.NoError(t, err)
			data[alias] = json.RawMessage(prJSON)
		}

		resp := graphQLResponse{Data: data}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(resp)
		require.NoError(t, err)
	}
}

func TestMergeChecker_Check(t *testing.T) {
	tests := []struct {
		name          string
		handler       http.HandlerFunc
		queue         string
		change        entity.Change
		wantMergeable bool
		wantReason    string
		wantErr       bool
	}{
		{
			name: "single PR mergeable",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: shaAFull, State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/pull/1/" + shaAFull}},
			wantMergeable: true,
		},
		{
			name: "single PR conflicting",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateConflicting, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: shaAFull, State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/pull/1/" + shaAFull}},
			wantMergeable: false,
			wantReason:    "PR #1 has merge conflicts",
		},
		{
			name: "stack of two PRs mergeable",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: sha1Full, State: PRStateOpen},
				{Number: 2, Mergeable: PRMergeableStateMergeable, BaseRefName: "feature-1", HeadRefName: "feature-2", HeadRefOid: sha2Full, State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/pull/1/" + sha1Full, "github://uber/repo/pull/2/" + sha2Full}},
			wantMergeable: true,
		},
		{
			name: "unknown mergeability returns error",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateUnknown, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: shaAFull, State: PRStateOpen},
			}),
			queue:   "test-queue",
			change:  entity.Change{URIs: []string{"github://uber/repo/pull/1/" + shaAFull}},
			wantErr: true,
		},
		{
			name: "stale SHA not mergeable",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: shaNewFull, State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/pull/1/" + shaOldFull}},
			wantMergeable: false,
			wantReason:    fmt.Sprintf("PR #1 head SHA changed: expected %s, got %s", shaOldFull, shaNewFull),
		},
		{
			name: "invalid change ID",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("should not reach server")
			}),
			queue:   "test-queue",
			change:  entity.Change{URIs: []string{"invalid-change-id"}},
			wantErr: true,
		},
		{
			name: "server error",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("internal server error"))
			}),
			queue:   "test-queue",
			change:  entity.Change{URIs: []string{"github://uber/repo/pull/1/" + shaAFull}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			mc := newTestMergeChecker(t, server.URL)
			result, err := mc.Check(context.Background(), tt.queue, tt.change)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMergeable, result.Mergeable)
			assert.Equal(t, tt.wantReason, result.Reason)
		})
	}
}
