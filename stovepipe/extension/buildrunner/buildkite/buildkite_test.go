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

	platformbuildkite "github.com/uber/submitqueue/platform/extension/buildrunner/buildkite"
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
		platformbuildkite.NewClient(c),
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
		Client: platformbuildkite.NewClient(c),
		Logger: zap.NewNop().Sugar(),
	})
}

// --- Trigger ---

func TestTrigger_SubmitsCorrectPayloadAndReturnsBuildkiteNumber(t *testing.T) {
	var capturedMethod string
	var capturedBody []byte

	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(42, "scheduled", "https://buildkite.com/test-org/my-pipeline/builds/42"))
	}))

	id, err := r.Trigger(context.Background(), "github://repo/base/bbb", "github://repo/head/aaa", nil)
	require.NoError(t, err)
	assert.Equal(t, platformbuildkite.EncodeBuildNumber(42), id.ID)

	assert.Equal(t, http.MethodPost, capturedMethod)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "github://repo/head/aaa", req.Env[EnvKeyHeadURI])
	assert.Equal(t, "github://repo/base/bbb", req.Env[EnvKeyBaseURI])
	assert.Equal(t, "my-queue", req.Env[EnvKeyQueue])
}

func TestTrigger_EmptyBaseURI_FullBuild(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(1, "scheduled", ""))
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
	require.NoError(t, err)

	var req platformbuildkite.CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "", req.Env[EnvKeyBaseURI])
}

func TestTrigger_BuildkiteError_ReturnsError(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
	require.Error(t, err)
}

func TestTrigger_WithMetadata_SetsEnvVar(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(10, "scheduled", ""))
	}))

	metadata := entity.BuildMetadata{"requester": "alice", "ticket": "SQ-42"}
	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", metadata)
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
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(11, "scheduled", ""))
	}))

	_, err := r.Trigger(context.Background(), "", "github://repo/head/aaa", nil)
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
		// state this code doesn't know about doesn't terminally fail a build.
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

func TestStatus_MalformedBuildID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, _, err := r.Status(context.Background(), entity.BuildID{ID: "not-a-number"})
	require.Error(t, err)
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

func TestCancel_MalformedBuildID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	require.Error(t, r.Cancel(context.Background(), entity.BuildID{ID: "not-a-number"}))
}
