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

package buildkite

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/change"
	platformbuildkite "github.com/uber/submitqueue/platform/extension/buildrunner/buildkite"
	phttp "github.com/uber/submitqueue/platform/http"
	"github.com/uber/submitqueue/submitqueue/core/changeset"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// newTestRunner creates a runner backed by a test HTTP server. An optional
// resolver seeds the batch changes the runner resolves; omit it for tests that
// do not trigger builds (Status/Cancel).
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
		platformbuildkite.NewClient(c),
		r,
		zap.NewNop().Sugar(),
	)
}

// buildJSON encodes fields into a minimal Buildkite build JSON response.
func buildJSON(number int, state, webURL string) []byte {
	return buildJSONWithEnv(number, state, webURL, nil)
}

// buildJSONWithEnv encodes fields into a Buildkite build JSON response including env vars.
func buildJSONWithEnv(number int, state, webURL string, env map[string]string) []byte {
	b, _ := json.Marshal(platformbuildkite.BuildResponse{Number: number, State: state, WebURL: webURL, Env: env})
	return b
}

// --- Interface / constructor ---

func TestNewBuildRunner_ImplementsInterface(t *testing.T) {
	c, err := phttp.NewClient("http://example.com")
	require.NoError(t, err)
	var _ buildrunner.BuildRunner = NewBuildRunner(Params{
		Client:   platformbuildkite.NewClient(c),
		Resolver: changesetfake.New(),
		Logger:   zap.NewNop().Sugar(),
	})
}

// --- Trigger ---

func TestTrigger_SubmitsCorrectPayloadAndReturnsBuildkiteNumber(t *testing.T) {
	var capturedMethod string
	var capturedBody []byte

	resolver := changesetfake.New().
		Set("base-batch", change.Change{URIs: []string{"github://github.example.com/org/repo/pull/1/aaa111"}}).
		Set("head-batch", change.Change{URIs: []string{"github://github.example.com/org/repo/pull/2/bbb222"}})

	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(42, "scheduled", "https://buildkite.com/test-org/my-pipeline/builds/42"))
	}), resolver)

	id, err := r.Trigger(context.Background(), []entity.Batch{{ID: "base-batch"}}, entity.Batch{ID: "head-batch"}, nil)
	require.NoError(t, err)
	assert.Equal(t, platformbuildkite.EncodeBuildNumber(42), id.ID)

	assert.Equal(t, http.MethodPost, capturedMethod)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, `["github://github.example.com/org/repo/pull/1/aaa111"]`, req.Env[EnvKeyBaseURIs])
	assert.Equal(t, `["github://github.example.com/org/repo/pull/2/bbb222"]`, req.Env[EnvKeyHeadURIs])
	assert.Equal(t, "my-queue", req.Env[EnvKeyQueue])
}

func TestTrigger_EmptyBase_ProducesJSONArray(t *testing.T) {
	var capturedBody []byte
	resolver := changesetfake.New().Set("head-batch", change.Change{URIs: []string{"u"}})
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(1, "scheduled", ""))
	}), resolver)

	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, nil)
	require.NoError(t, err)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	// nil base must produce [] in JSON, not null.
	assert.Equal(t, "[]", req.Env[EnvKeyBaseURIs])
}

func TestTrigger_MultipleChangesFlattened(t *testing.T) {
	var capturedBody []byte
	resolver := changesetfake.New().Set("head-batch",
		change.Change{URIs: []string{"github://github.example.com/org/repo/pull/1/aaa"}},
		change.Change{URIs: []string{"github://github.example.com/org/repo/pull/2/bbb", "github://github.example.com/org/repo/pull/3/ccc"}},
	)
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(2, "scheduled", ""))
	}), resolver)

	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, nil)
	require.NoError(t, err)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t,
		`["github://github.example.com/org/repo/pull/1/aaa","github://github.example.com/org/repo/pull/2/bbb","github://github.example.com/org/repo/pull/3/ccc"]`,
		req.Env[EnvKeyHeadURIs],
	)
}

func TestTrigger_BuildkiteError_ReturnsError(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, nil)
	require.Error(t, err)
}

func TestTrigger_WithMetadata_SetsEnvVar(t *testing.T) {
	var capturedBody []byte
	resolver := changesetfake.New().Set("head-batch", change.Change{URIs: []string{"u"}})
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(10, "scheduled", ""))
	}), resolver)

	metadata := entity.BuildMetadata{"requester": "alice", "ticket": "SQ-42"}
	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, metadata)
	require.NoError(t, err)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	require.Contains(t, req.Env, EnvKeyMetadata)

	var got entity.BuildMetadata
	require.NoError(t, json.Unmarshal([]byte(req.Env[EnvKeyMetadata]), &got))
	assert.Equal(t, metadata, got)
}

func TestTrigger_NilMetadata_NoMetadataEnvVar(t *testing.T) {
	var capturedBody []byte
	resolver := changesetfake.New().Set("head-batch", change.Change{URIs: []string{"u"}})
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(11, "scheduled", ""))
	}), resolver)

	_, err := r.Trigger(context.Background(), nil, entity.Batch{ID: "head-batch"}, nil)
	require.NoError(t, err)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.NotContains(t, req.Env, EnvKeyMetadata)
}

// --- Status ---

func TestStatus_StateMapping(t *testing.T) {
	tests := []struct {
		bkState string
		want    entity.BuildStatus
	}{
		{"creating", entity.BuildStatusAccepted},
		{"scheduled", entity.BuildStatusAccepted},
		{"running", entity.BuildStatusRunning},
		{"blocked", entity.BuildStatusRunning},
		{"passed", entity.BuildStatusSucceeded},
		{"failed", entity.BuildStatusFailed},
		{"not_run", entity.BuildStatusFailed},
		{"skipped", entity.BuildStatusFailed},
		{"canceling", entity.BuildStatusCancelled},
		{"canceled", entity.BuildStatusCancelled},
		// Unrecognised states map to the non-terminal Unknown, not Failed, so a
		// state this code doesn't know about doesn't terminally fail a batch.
		{"some_future_state", entity.BuildStatusUnknown},
		{"", entity.BuildStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.bkState, func(t *testing.T) {
			assert.Equal(t, tt.want, mapState(tt.bkState))
		})
	}
}

func TestStatus_ReturnsLiveBuildkiteState(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(7, "running", "https://buildkite.com/test-org/my-pipeline/builds/7"))
	}))

	status, meta, err := r.Status(context.Background(), entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(7)})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)
	assert.Equal(t, "https://buildkite.com/test-org/my-pipeline/builds/7", meta["url"])
}

func TestStatus_BuildkiteNotFound(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	_, _, err := r.Status(context.Background(), entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(99)})
	require.Error(t, err)
}

func TestStatus_EchosCallerMetadata(t *testing.T) {
	metadata := entity.BuildMetadata{"requester": "alice", "ticket": "SQ-42"}
	metaJSON, _ := json.Marshal(metadata)

	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSONWithEnv(7, "passed", "https://buildkite.com/test-org/my-pipeline/builds/7",
			map[string]string{EnvKeyMetadata: string(metaJSON)},
		))
	}))

	_, meta, err := r.Status(context.Background(), entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(7)})
	require.NoError(t, err)
	assert.Equal(t, "alice", meta["requester"])
	assert.Equal(t, "SQ-42", meta["ticket"])
	assert.Equal(t, "https://buildkite.com/test-org/my-pipeline/builds/7", meta["url"])
}

func TestStatus_NoMetadata_ReturnsOnlyURL(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(8, "running", "https://buildkite.com/test-org/my-pipeline/builds/8"))
	}))

	_, meta, err := r.Status(context.Background(), entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(8)})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildMetadata{"url": "https://buildkite.com/test-org/my-pipeline/builds/8"}, meta)
}

// --- Cancel ---

func TestCancel_CallsBuildkite(t *testing.T) {
	var cancelCalled bool
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancelCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buildJSON(5, "canceled", ""))
	}))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(5)}))
	assert.True(t, cancelCalled)
}

func TestCancel_AlreadyTerminal_Noop(t *testing.T) {
	// Buildkite returns 422 when the build cannot be cancelled (already terminal).
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: platformbuildkite.EncodeBuildNumber(5)}))
}
