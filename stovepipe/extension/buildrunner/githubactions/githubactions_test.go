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
	"go.uber.org/zap"

	platformgithubactions "github.com/uber/submitqueue/platform/extension/buildrunner/githubactions"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

// newTestRunner creates a runner backed by a test HTTP server.
func newTestRunner(t *testing.T, handler http.Handler) *runner {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := phttp.NewClient(srv.URL)
	require.NoError(t, err)
	return newRunner(
		buildrunner.Config{QueueName: "my-queue"},
		"main",
		map[string]string{"custom": "value"},
		platformgithubactions.NewClient(c, "uber", "stovepipe", "stovepipe-ci.yml"),
		zap.NewNop().Sugar(),
	)
}

// --- Interface / constructor ---

func TestNew_ImplementsInterface(t *testing.T) {
	r, err := NewBuildRunner(Params{Logger: zap.NewNop().Sugar()})
	require.Error(t, err, "github actions client is required")
	var _ buildrunner.BuildRunner = r
}

// --- Trigger ---

func TestTrigger_DispatchesWorkflowAndReturnsRunID(t *testing.T) {
	var capturedMethod, capturedPath string
	var capturedBody []byte

	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedPath = req.URL.Path
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(platformgithubactions.DispatchWorkflowResponse{WorkflowRunID: 42})
	}))

	id, err := r.Trigger(context.Background(), "github://repo/base/bbb", "github://repo/head/aaa", nil)
	require.NoError(t, err)
	assert.Equal(t, platformgithubactions.EncodeRunID(42), id.ID)

	assert.Equal(t, http.MethodPost, capturedMethod)
	assert.Equal(t, "/repos/uber/stovepipe/actions/workflows/stovepipe-ci.yml/dispatches", capturedPath)

	var req platformgithubactions.DispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "main", req.Ref)
	assert.True(t, req.ReturnRunDetails)
	assert.Equal(t, "github://repo/head/aaa", req.Inputs[InputKeyHeadURI])
	assert.Equal(t, "github://repo/base/bbb", req.Inputs[InputKeyBaseURI])
	assert.Equal(t, "my-queue", req.Inputs[InputKeyQueue])
	assert.Equal(t, "value", req.Inputs["custom"])
}

func TestTrigger_EmptyBaseURI_FullBuild(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(platformgithubactions.DispatchWorkflowResponse{WorkflowRunID: 1})
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
	require.NoError(t, err)

	var req platformgithubactions.DispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "", req.Inputs[InputKeyBaseURI])
}

func TestTrigger_DispatchError_ReturnsError(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
	require.Error(t, err)
}

func TestTrigger_ErrorsWhenDispatchResponseHasNoRunID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(platformgithubactions.DispatchWorkflowResponse{})
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
	require.Error(t, err)
}

func TestTrigger_WithMetadata_SetsInput(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(platformgithubactions.DispatchWorkflowResponse{WorkflowRunID: 10})
	}))

	metadata := entity.BuildMetadata{"requester": "alice", "ticket": "SQ-42"}
	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", metadata)
	require.NoError(t, err)

	var req platformgithubactions.DispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	require.Contains(t, req.Inputs, InputKeyMetadata)

	var got entity.BuildMetadata
	require.NoError(t, json.Unmarshal([]byte(req.Inputs[InputKeyMetadata]), &got))
	assert.Equal(t, metadata, got)
}

func TestTrigger_NilMetadata_NoMetadataInput(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(platformgithubactions.DispatchWorkflowResponse{WorkflowRunID: 11})
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
	require.NoError(t, err)

	var req platformgithubactions.DispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.NotContains(t, req.Inputs, InputKeyMetadata)
}

// --- Status ---

func TestStatus_StatusMapping(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       entity.BuildStatus
	}{
		{name: "queued", status: "queued", want: entity.BuildStatusAccepted},
		{name: "in_progress", status: "in_progress", want: entity.BuildStatusRunning},
		{name: "success", status: "completed", conclusion: "success", want: entity.BuildStatusSucceeded},
		{name: "failure", status: "completed", conclusion: "failure", want: entity.BuildStatusFailed},
		{name: "cancelled", status: "completed", conclusion: "cancelled", want: entity.BuildStatusCancelled},
		// Unrecognised status/conclusion maps to the non-terminal Unknown, not
		// Failed, so a state this code doesn't know about doesn't terminally
		// fail a build.
		{name: "some_future_state", status: "some_future_state", want: entity.BuildStatusUnknown},
		{name: "empty", want: entity.BuildStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, mapRunStatus(tt.status, tt.conclusion))
		})
	}
}

func TestStatus_ReturnsRunMetadata(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Equal(t, "/repos/uber/stovepipe/actions/runs/7", req.URL.Path)
		_ = json.NewEncoder(w).Encode(platformgithubactions.WorkflowRun{
			ID:      7,
			Status:  "in_progress",
			HTMLURL: "https://github.com/uber/stovepipe/actions/runs/7",
		})
	}))

	status, meta, err := r.Status(context.Background(), entity.BuildID{ID: platformgithubactions.EncodeRunID(7)})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)
	assert.Equal(t, "https://github.com/uber/stovepipe/actions/runs/7", meta["url"])
	assert.Equal(t, "7", meta["github_run_id"])
}

func TestStatus_MalformedBuildID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, _, err := r.Status(context.Background(), entity.BuildID{ID: "not-a-number"})
	require.Error(t, err)
}

func TestStatus_NotFound(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	_, _, err := r.Status(context.Background(), entity.BuildID{ID: platformgithubactions.EncodeRunID(99)})
	require.Error(t, err)
}

// --- Cancel ---

func TestCancel_CallsGitHub(t *testing.T) {
	var cancelledRunID int64
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.True(t, req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/cancel"))
		parts := strings.Split(req.URL.Path, "/")
		id, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
		require.NoError(t, err)
		cancelledRunID = id
		w.WriteHeader(http.StatusAccepted)
	}))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: platformgithubactions.EncodeRunID(5)}))
	assert.Equal(t, int64(5), cancelledRunID)
}

func TestCancel_AlreadyTerminal_Noop(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: platformgithubactions.EncodeRunID(5)}))
}

func TestCancel_MalformedBuildID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	require.Error(t, r.Cancel(context.Background(), entity.BuildID{ID: "not-a-number"}))
}
