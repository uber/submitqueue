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
	return NewClient(c)
}

func buildJSON(t *testing.T, number int, state, webURL string) []byte {
	t.Helper()
	return buildJSONWithEnv(t, number, state, webURL, nil)
}

func buildJSONWithEnv(t *testing.T, number int, state, webURL string, env map[string]string) []byte {
	t.Helper()
	b, err := json.Marshal(BuildResponse{Number: number, State: state, WebURL: webURL, Env: env})
	require.NoError(t, err)
	return b
}

// --- CreateBuild ---

func TestCreateBuild_SubmitsPayloadAndReturnsResponse(t *testing.T) {
	var capturedMethod string
	var capturedBody []byte
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(t, 42, "scheduled", "https://buildkite.com/test-org/my-pipeline/builds/42"))
	}))

	resp, err := c.CreateBuild(context.Background(), CreateBuildRequest{
		Message: "test build",
		Env:     map[string]string{"KEY": "value"},
	})
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, capturedMethod)
	assert.Equal(t, 42, resp.Number)
	assert.Equal(t, "scheduled", resp.State)
	assert.Equal(t, "https://buildkite.com/test-org/my-pipeline/builds/42", resp.WebURL)

	var req CreateBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, "test build", req.Message)
	assert.Equal(t, "value", req.Env["KEY"])
}

func TestCreateBuild_ErrorStatus_ReturnsError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	_, err := c.CreateBuild(context.Background(), CreateBuildRequest{})
	require.Error(t, err)
}

// --- GetBuild ---

func TestGetBuild_ReturnsResponse(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Equal(t, "/builds/7", req.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(t, 7, "running", "https://buildkite.com/test-org/my-pipeline/builds/7"))
	}))

	resp, err := c.GetBuild(context.Background(), 7)
	require.NoError(t, err)
	assert.Equal(t, "running", resp.State)
}

func TestGetBuild_NotFound_ReturnsError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	_, err := c.GetBuild(context.Background(), 99)
	require.Error(t, err)
}

func TestGetBuild_EchoesEnv(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSONWithEnv(t, 7, "passed", "", map[string]string{"FOO": "bar"}))
	}))

	resp, err := c.GetBuild(context.Background(), 7)
	require.NoError(t, err)
	assert.Equal(t, "bar", resp.Env["FOO"])
}

// --- CancelBuild ---

func TestCancelBuild_CallsBuildkite(t *testing.T) {
	var cancelled bool
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		cancelled = true
		assert.Equal(t, http.MethodPut, req.Method)
		w.WriteHeader(http.StatusOK)
	}))

	require.NoError(t, c.CancelBuild(context.Background(), 5))
	assert.True(t, cancelled)
}

func TestCancelBuild_AlreadyTerminal_Noop(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))

	require.NoError(t, c.CancelBuild(context.Background(), 5))
}

func TestCancelBuild_ErrorStatus_ReturnsError(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	require.Error(t, c.CancelBuild(context.Background(), 5))
}

// --- EncodeBuildNumber / ParseBuildNumber ---

func TestEncodeParseBuildNumber_RoundTrip(t *testing.T) {
	for _, n := range []int{1, 9999, 0} {
		id := EncodeBuildNumber(n)
		got, err := ParseBuildNumber(id)
		require.NoError(t, err)
		assert.Equal(t, n, got)
	}
}

func TestParseBuildNumber_Invalid(t *testing.T) {
	for _, id := range []string{"", "notanumber", "org/pipeline/1"} {
		t.Run(id, func(t *testing.T) {
			_, err := ParseBuildNumber(id)
			require.Error(t, err)
		})
	}
}

// --- ParseState ---

func TestParseState(t *testing.T) {
	tests := []struct {
		raw  string
		want State
	}{
		{"creating", StateAccepted},
		{"scheduled", StateAccepted},
		{"running", StateRunning},
		{"blocked", StateRunning},
		{"passed", StateSucceeded},
		{"failed", StateFailed},
		{"not_run", StateFailed},
		{"skipped", StateFailed},
		{"canceling", StateCancelled},
		{"canceled", StateCancelled},
		{"some_future_state", StateUnknown},
		{"", StateUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			assert.Equal(t, tt.want, ParseState(tt.raw))
		})
	}
}

// --- DecodeMetadataEnv ---

func TestDecodeMetadataEnv_PresentAndValid(t *testing.T) {
	env := map[string]string{"SQ_METADATA": `{"requester":"alice","ticket":"SQ-42"}`}
	got, err := DecodeMetadataEnv(env, "SQ_METADATA")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"requester": "alice", "ticket": "SQ-42"}, got)
}

func TestDecodeMetadataEnv_Absent_ReturnsEmptyMap(t *testing.T) {
	got, err := DecodeMetadataEnv(map[string]string{}, "SQ_METADATA")
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.NotNil(t, got)
}

func TestDecodeMetadataEnv_Malformed_ReturnsError(t *testing.T) {
	env := map[string]string{"SQ_METADATA": "not json"}
	got, err := DecodeMetadataEnv(env, "SQ_METADATA")
	require.Error(t, err)
	assert.Nil(t, got)
}
