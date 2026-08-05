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

package githubactions

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	phttp "github.com/uber/submitqueue/platform/http"
)

// newTestClient creates a Client backed by a test HTTP server.
func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := phttp.NewClient(srv.URL)
	require.NoError(t, err)
	return NewClient(c, "uber", "submitqueue", "submitqueue-ci.yml")
}

// --- NewClient / accessors ---

func TestNewClient_ExposesIdentity(t *testing.T) {
	c := NewClient(http.DefaultClient, "uber", "submitqueue", "submitqueue-ci.yml")
	assert.Equal(t, "uber", c.Owner())
	assert.Equal(t, "submitqueue", c.Repo())
	assert.Equal(t, "submitqueue-ci.yml", c.WorkflowID())
}

// --- DispatchWorkflow ---

func TestDispatchWorkflow_SubmitsPayloadAndReturnsResponse(t *testing.T) {
	var capturedMethod, capturedPath string
	var capturedBody []byte
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedPath = req.URL.Path
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(DispatchWorkflowResponse{
			WorkflowRunID: 42,
			RunURL:        "https://api.github.com/repos/uber/submitqueue/actions/runs/42",
			HTMLURL:       "https://github.com/uber/submitqueue/actions/runs/42",
		})
	}))

	resp, err := c.DispatchWorkflow(context.Background(), DispatchWorkflowRequest{
		Ref:              "main",
		ReturnRunDetails: true,
		Inputs:           map[string]string{"key": "value"},
	})
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, capturedMethod)
	assert.Equal(t, "/repos/uber/submitqueue/actions/workflows/submitqueue-ci.yml/dispatches", capturedPath)
	assert.Equal(t, int64(42), resp.WorkflowRunID)

	var req DispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "main", req.Ref)
	assert.True(t, req.ReturnRunDetails)
	assert.Equal(t, "value", req.Inputs["key"])
}

func TestDispatchWorkflow_ErrorStatus_ReturnsError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, err := c.DispatchWorkflow(context.Background(), DispatchWorkflowRequest{})
	require.Error(t, err)
}

// --- GetRun ---

func TestGetRun_ReturnsResponse(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Equal(t, "/repos/uber/submitqueue/actions/runs/42", req.URL.Path)
		_ = json.NewEncoder(w).Encode(WorkflowRun{ID: 42, Status: "in_progress"})
	}))

	run, err := c.GetRun(context.Background(), 42)
	require.NoError(t, err)
	assert.Equal(t, "in_progress", run.Status)
}

func TestGetRun_NotFound_ReturnsError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	_, err := c.GetRun(context.Background(), 99)
	require.Error(t, err)
}

// --- CancelRun ---

func TestCancelRun_CallsGitHub(t *testing.T) {
	var cancelledRunID int64
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.True(t, req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/cancel"))
		parts := strings.Split(req.URL.Path, "/")
		id, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
		require.NoError(t, err)
		cancelledRunID = id
		w.WriteHeader(http.StatusAccepted)
	}))

	require.NoError(t, c.CancelRun(context.Background(), 99))
	assert.Equal(t, int64(99), cancelledRunID)
}

func TestCancelRun_AlreadyTerminal_Noop(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))

	require.NoError(t, c.CancelRun(context.Background(), 5))
}

func TestCancelRun_NotFound_ReturnsError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	require.Error(t, c.CancelRun(context.Background(), 5))
}

// --- EncodeRunID / ParseRunID ---

func TestEncodeParseRunID_RoundTrip(t *testing.T) {
	for _, id := range []int64{1, 9999} {
		got, err := ParseRunID(EncodeRunID(id))
		require.NoError(t, err)
		assert.Equal(t, id, got)
	}
}

func TestParseRunID_Invalid(t *testing.T) {
	for _, id := range []string{"", "notanumber", "0", "-1", "gha-old-id"} {
		t.Run(id, func(t *testing.T) {
			_, err := ParseRunID(id)
			require.Error(t, err)
		})
	}
}

// --- ParseRunStatus ---

func TestParseRunStatus(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       RunStatus
	}{
		{name: "queued", status: "queued", want: RunStatusAccepted},
		{name: "pending", status: "pending", want: RunStatusAccepted},
		{name: "requested", status: "requested", want: RunStatusAccepted},
		{name: "waiting", status: "waiting", want: RunStatusAccepted},
		{name: "running", status: "in_progress", want: RunStatusRunning},
		{name: "success", status: "completed", conclusion: "success", want: RunStatusSucceeded},
		{name: "failure", status: "completed", conclusion: "failure", want: RunStatusFailed},
		{name: "timed out", status: "completed", conclusion: "timed_out", want: RunStatusFailed},
		{name: "cancelled", status: "completed", conclusion: "cancelled", want: RunStatusCancelled},
		{name: "completed missing conclusion", status: "completed", want: RunStatusUnknown},
		{name: "unknown", status: "some_future_state", want: RunStatusUnknown},
		{name: "empty", want: RunStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ParseRunStatus(tt.status, tt.conclusion))
		})
	}
}

// --- RunMetadata ---

func TestRunMetadata_IncludesIdentityAndOutcome(t *testing.T) {
	meta := RunMetadata(WorkflowRun{
		ID:           42,
		DisplayTitle: "SubmitQueue gha-trace",
		Status:       "in_progress",
		HTMLURL:      "https://github.com/uber/submitqueue/actions/runs/42",
		RunAttempt:   2,
		HeadBranch:   "main",
		CreatedAt:    "2026-06-05T00:00:00Z",
	})
	assert.Equal(t, "42", meta["github_run_id"])
	assert.Equal(t, "2", meta["github_run_attempt"])
	assert.Equal(t, "in_progress", meta["github_status"])
	assert.Equal(t, "https://github.com/uber/submitqueue/actions/runs/42", meta["url"])
	assert.Equal(t, "main", meta["github_head_branch"])
	assert.Equal(t, "2026-06-05T00:00:00Z", meta["github_created_at"])
}

func TestRunMetadata_OmitsEmptyOptionalFields(t *testing.T) {
	meta := RunMetadata(WorkflowRun{ID: 7, Status: "queued"})
	assert.NotContains(t, meta, "github_head_branch")
	assert.NotContains(t, meta, "github_created_at")
}
