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

	"github.com/uber/submitqueue/platform/base/change"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

func newTestRunner(t *testing.T, handler http.Handler, resolver ...changeset.Resolver) *runner {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := phttp.NewClient(srv.URL)
	require.NoError(t, err)

	r := changeset.Resolver(changesetfake.New())
	if len(resolver) > 0 {
		r = resolver[0]
	}

	return newRunner(
		buildrunner.Config{QueueName: "my-queue"},
		"main",
		map[string]string{"custom": "value"},
		&client{
			httpClient: c,
			owner:      "uber",
			repo:       "submitqueue",
			workflowID: "submitqueue-ci.yml",
		},
		r,
		zap.NewNop().Sugar(),
	)
}

func TestNewBuildRunner_ValidatesConfig(t *testing.T) {
	_, err := NewBuildRunner(Params{Logger: zap.NewNop().Sugar()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "http client is required")
}

func TestNewBuildRunner_BindsQueueConfigAndExtraInputs(t *testing.T) {
	br, err := NewBuildRunner(Params{
		Config:      buildrunner.Config{QueueName: "queue-a"},
		HTTPClient:  http.DefaultClient,
		Logger:      zap.NewNop().Sugar(),
		Owner:       "uber",
		Repo:        "submitqueue",
		WorkflowID:  "submitqueue-ci.yml",
		Ref:         "main",
		ExtraInputs: map[string]string{"runner": "ubuntu-latest"},
	})
	require.NoError(t, err)

	r := br.(*runner)
	assert.Equal(t, "queue-a", r.cfg.QueueName)
	assert.Equal(t, "ubuntu-latest", r.extraInputs["runner"])
}

func TestTrigger_DispatchesWorkflowAndReturnsRunID(t *testing.T) {
	var capturedMethod string
	var capturedPath string
	var capturedBody []byte

	resolver := changesetfake.New().
		Set("base-batch", change.Change{URIs: []string{"github://github.example.com/org/repo/pull/1/aaa111"}}).
		Set("head-batch", change.Change{URIs: []string{"github://github.example.com/org/repo/pull/2/bbb222"}})

	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedPath = req.URL.String()
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(dispatchWorkflowResponse{
			WorkflowRunID: 42,
			RunURL:        "https://api.github.com/repos/uber/submitqueue/actions/runs/42",
			HTMLURL:       "https://github.com/uber/submitqueue/actions/runs/42",
		})
	}), resolver)

	metadata := entity.BuildMetadata{"requester": "alice"}

	id, err := r.Trigger(context.Background(), []entity.Batch{{ID: "base-batch"}}, entity.Batch{ID: "head-batch"}, metadata)
	require.NoError(t, err)
	assert.Equal(t, "42", id.ID)

	assert.Equal(t, http.MethodPost, capturedMethod)
	assert.Equal(t, "/repos/uber/submitqueue/actions/workflows/submitqueue-ci.yml/dispatches", capturedPath)

	var req dispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "main", req.Ref)
	assert.True(t, req.ReturnRunDetails)
	assert.NotContains(t, req.Inputs, "sq_build_id")
	assert.Equal(t, `["github://github.example.com/org/repo/pull/1/aaa111"]`, req.Inputs[InputKeyBaseURIs])
	assert.Equal(t, `["github://github.example.com/org/repo/pull/2/bbb222"]`, req.Inputs[InputKeyHeadURIs])
	assert.Equal(t, "my-queue", req.Inputs[InputKeyQueue])
	assert.Equal(t, "value", req.Inputs["custom"])

	var gotMetadata entity.BuildMetadata
	require.NoError(t, json.Unmarshal([]byte(req.Inputs[InputKeyMetadata]), &gotMetadata))
	assert.Equal(t, metadata, gotMetadata)
}

func TestTrigger_EmptyBaseProducesJSONArray(t *testing.T) {
	var capturedBody []byte
	resolver := changesetfake.New().Set("head-batch", change.Change{URIs: []string{"u"}})
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		_ = json.NewEncoder(w).Encode(dispatchWorkflowResponse{WorkflowRunID: 7})
	}), resolver)

	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, nil)
	require.NoError(t, err)

	var req dispatchWorkflowRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "[]", req.Inputs[InputKeyBaseURIs])
	assert.NotContains(t, req.Inputs, InputKeyMetadata)
}

func TestTrigger_ErrorsWhenDispatchResponseHasNoRunID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_ = json.NewEncoder(w).Encode(dispatchWorkflowResponse{})
	}))

	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "response missing workflow_run_id")
}

func TestStatus_GetsRunAndMapsState(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Equal(t, "/repos/uber/submitqueue/actions/runs/42", req.URL.Path)
		_ = json.NewEncoder(w).Encode(workflowRun{
			ID:           42,
			DisplayTitle: "SubmitQueue gha-trace",
			Status:       "in_progress",
			HTMLURL:      "https://github.com/uber/submitqueue/actions/runs/42",
			RunAttempt:   2,
			Event:        "workflow_dispatch",
			HeadBranch:   "main",
			CreatedAt:    "2026-06-05T00:00:00Z",
		})
	}))

	status, meta, err := r.Status(context.Background(), entity.BuildID{ID: "42"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)
	assert.Equal(t, "42", meta["github_run_id"])
	assert.Equal(t, "2", meta["github_run_attempt"])
	assert.Equal(t, "https://github.com/uber/submitqueue/actions/runs/42", meta["url"])
	assert.Equal(t, "main", meta["github_head_branch"])
}

func TestStatus_MalformedBuildIDErrors(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
	}))

	_, _, err := r.Status(context.Background(), entity.BuildID{ID: "gha-old-id"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed build ID")
}

func TestCancel_CancelsRun(t *testing.T) {
	var cancelledRunID int64
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/cancel") {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
		}
		parts := strings.Split(req.URL.Path, "/")
		id, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
		require.NoError(t, err)
		cancelledRunID = id
		w.WriteHeader(http.StatusAccepted)
	}))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "99"}))
	assert.Equal(t, int64(99), cancelledRunID)
}

func TestCancel_MalformedBuildIDErrors(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
	}))

	err := r.Cancel(context.Background(), entity.BuildID{ID: "gha-old-id"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed build ID")
}

func TestMapRunStatus(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       entity.BuildStatus
	}{
		{name: "queued", status: "queued", want: entity.BuildStatusAccepted},
		{name: "pending", status: "pending", want: entity.BuildStatusAccepted},
		{name: "requested", status: "requested", want: entity.BuildStatusAccepted},
		{name: "waiting", status: "waiting", want: entity.BuildStatusAccepted},
		{name: "running", status: "in_progress", want: entity.BuildStatusRunning},
		{name: "success", status: "completed", conclusion: "success", want: entity.BuildStatusSucceeded},
		{name: "failure", status: "completed", conclusion: "failure", want: entity.BuildStatusFailed},
		{name: "timed out", status: "completed", conclusion: "timed_out", want: entity.BuildStatusFailed},
		{name: "cancelled", status: "completed", conclusion: "cancelled", want: entity.BuildStatusCancelled},
		{name: "completed missing conclusion", status: "completed", want: entity.BuildStatusUnknown},
		{name: "unknown", status: "some_future_state", want: entity.BuildStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, mapRunStatus(tt.status, tt.conclusion))
		})
	}
}
