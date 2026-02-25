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
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/mergechecker"
	"go.uber.org/zap/zaptest"
)

func newTestMergeChecker(t *testing.T, serverURL string) mergechecker.MergeChecker {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	return NewMergeChecker(Params{
		HTTPClient:   &http.Client{},
		GraphQLURL:   serverURL,
		Logger:       logger,
		MetricsScope: scope,
	})
}

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
				{Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "abc123", State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
			wantMergeable: true,
		},
		{
			name: "single PR conflicting",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateConflicting, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "abc123", State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
			wantMergeable: false,
			wantReason:    "PR #1 has merge conflicts",
		},
		{
			name: "stack of two PRs mergeable",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "sha1", State: PRStateOpen},
				{Number: 2, Mergeable: PRMergeableStateMergeable, BaseRefName: "feature-1", HeadRefName: "feature-2", HeadRefOid: "sha2", State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/1/sha1", "github://uber/repo/2/sha2"}},
			wantMergeable: true,
		},
		{
			name: "unknown mergeability returns error",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateUnknown, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "abc123", State: PRStateOpen},
			}),
			queue:   "test-queue",
			change:  entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
			wantErr: true,
		},
		{
			name: "stale SHA not mergeable",
			handler: graphQLHandler(t, []PRInfo{
				{Number: 1, Mergeable: PRMergeableStateMergeable, BaseRefName: "main", HeadRefName: "feature-1", HeadRefOid: "new_sha", State: PRStateOpen},
			}),
			queue:         "test-queue",
			change:        entity.Change{URIs: []string{"github://uber/repo/1/old_sha"}},
			wantMergeable: false,
			wantReason:    "PR #1 head SHA changed: expected old_sha, got new_sha",
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
			change:  entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
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
