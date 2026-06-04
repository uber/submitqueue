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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/core/httpclient"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

// newTestRunner creates a runner backed by a test HTTP server without starting
// the background goroutine. Tests drive job processing synchronously via
// drainTrigger and drainCancel to avoid goroutine timing races. The retry
// backoff is set to a millisecond so retry paths run fast.
func newTestRunner(t *testing.T, handler http.Handler) *runner {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := httpclient.NewClient(srv.URL)
	require.NoError(t, err)
	return newRunner(
		Config{
			QueueName:         "my-queue",
			SubmitTimeout:     5 * time.Second,
			MaxSubmitAttempts: 3,
			SubmitBackoff:     time.Millisecond,
		},
		&client{httpClient: c},
		16, // triggerSize
		16, // cancelSize
	)
}

// drainTrigger synchronously processes the next pending trigger job.
// Use after Trigger() to simulate the background worker in tests.
func drainTrigger(t *testing.T, r *runner) {
	t.Helper()
	select {
	case job := <-r.triggerCh:
		r.processTrigger(job)
	default:
		t.Fatal("drainTrigger: no pending trigger job in channel")
	}
}

// drainCancel synchronously processes the next pending cancel job.
func drainCancel(t *testing.T, r *runner) {
	t.Helper()
	select {
	case job := <-r.cancelCh:
		r.processCancel(job)
	default:
		t.Fatal("drainCancel: no pending cancel job in channel")
	}
}

// buildJSON encodes fields into a minimal Buildkite build JSON response.
func buildJSON(number int, state, webURL string) []byte {
	b, _ := json.Marshal(buildResponse{Number: number, State: state, WebURL: webURL})
	return b
}

// emptyListHandler responds to a metadata-filtered list query with an empty
// array (no build matches yet), failing the test on any other request.
func emptyListHandler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			t.Fatalf("unexpected %s request; expected only a metadata list lookup", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}
}

// --- Interface / constructor ---

func TestNew_ImplementsInterface(t *testing.T) {
	r, err := NewBuildRunner(Params{})
	require.NoError(t, err)
	var _ buildrunner.BuildRunner = r
}

// --- Trigger ---

func TestTrigger_EnqueuesJobAndReturnsID(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK) // should not be reached before drain
	}))

	id, err := r.Trigger(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, id.ID)

	// Exactly one job should be in the channel.
	assert.Len(t, r.triggerCh, 1)
}

func TestTrigger_StatusIsAcceptedBeforeWorkerRuns(t *testing.T) {
	// Before the worker submits, no build carries this ID in Buildkite, so the
	// metadata lookup returns empty and Status reports Accepted.
	r := newTestRunner(t, emptyListHandler(t))

	id, err := r.Trigger(context.Background(), nil, nil, nil)
	require.NoError(t, err)

	status, _, err := r.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusAccepted, status)
}

func TestTrigger_SubmitsCorrectPayloadToBuildkite(t *testing.T) {
	var capturedMethod string
	var capturedBody []byte

	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedMethod = req.Method
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(42, "scheduled", "https://buildkite.com/test-org/my-pipeline/builds/42"))
	}))

	base := []entity.Change{{URIs: []string{"github://org/repo/pull/1/aaa111"}}}
	head := []entity.Change{{URIs: []string{"github://org/repo/pull/2/bbb222"}}}

	id, err := r.Trigger(context.Background(), base, head, nil)
	require.NoError(t, err)

	drainTrigger(t, r)

	assert.Equal(t, http.MethodPost, capturedMethod)

	var req createBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t, `["github://org/repo/pull/1/aaa111"]`, req.Env[EnvKeyBaseURIs])
	assert.Equal(t, `["github://org/repo/pull/2/bbb222"]`, req.Env[EnvKeyHeadURIs])
	assert.Equal(t, "my-queue", req.Env[EnvKeyQueue])
	// The SQ build ID is stamped into metadata so Status/Cancel can recover the
	// build after a cache loss.
	assert.Equal(t, id.ID, req.MetaData[metaKeyBuildID])

	// After a successful submit the ref is cached, so Status uses getBuild.
	ref, ok := r.lookupRef(id.ID)
	require.True(t, ok)
	assert.Equal(t, encodeBuildRef(42), ref)
}

func TestTrigger_EmptyBase_ProducesJSONArray(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(1, "scheduled", ""))
	}))

	_, err := r.Trigger(context.Background(), nil, []entity.Change{{URIs: []string{"u"}}}, nil)
	require.NoError(t, err)
	drainTrigger(t, r)

	var req createBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	// nil base must produce [] in JSON, not null.
	assert.Equal(t, "[]", req.Env[EnvKeyBaseURIs])
}

func TestTrigger_MultipleChangesFlattened(t *testing.T) {
	var capturedBody []byte
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedBody, _ = io.ReadAll(req.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(2, "scheduled", ""))
	}))

	head := []entity.Change{
		{URIs: []string{"github://org/repo/pull/1/aaa"}},
		{URIs: []string{"github://org/repo/pull/2/bbb", "github://org/repo/pull/3/ccc"}},
	}
	_, err := r.Trigger(context.Background(), nil, head, nil)
	require.NoError(t, err)
	drainTrigger(t, r)

	var req createBuildRequest
	require.NoError(t, json.Unmarshal(capturedBody, &req))
	assert.Equal(t,
		`["github://org/repo/pull/1/aaa","github://org/repo/pull/2/bbb","github://org/repo/pull/3/ccc"]`,
		req.Env[EnvKeyHeadURIs],
	)
}

func TestTrigger_QueueFull_ReturnsError(t *testing.T) {
	r := newRunner(
		Config{},
		&client{httpClient: http.DefaultClient},
		1, 1,
	)
	// Fill the channel.
	_, err := r.Trigger(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	// Second call must fail.
	_, err = r.Trigger(context.Background(), nil, nil, nil)
	require.Error(t, err)
}

func TestProcessTrigger_RetriesTransientFailureThenSucceeds(t *testing.T) {
	var posts int
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		posts++
		if posts < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildJSON(7, "scheduled", ""))
	}))

	id, err := r.Trigger(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	drainTrigger(t, r)

	assert.Equal(t, 2, posts, "submit should retry after a transient failure")
	ref, ok := r.lookupRef(id.ID)
	require.True(t, ok)
	assert.Equal(t, encodeBuildRef(7), ref)
}

func TestTrigger_SubmitExhaustsRetries_BuildFails(t *testing.T) {
	// create (POST) always fails; the worker exhausts its retries and records a
	// submission failure.
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Nothing was created, so any metadata lookup finds nothing.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))

	id, err := r.Trigger(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	drainTrigger(t, r)

	// With submission permanently failed, Status reports terminal Failed (with a
	// reason) rather than polling Accepted forever.
	status, meta, err := r.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusFailed, status)
	assert.NotEmpty(t, meta["error"])
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

	// Inject ref directly (simulates successful processTrigger).
	r.storeRef("some-id", encodeBuildRef(7))

	status, meta, err := r.Status(context.Background(), entity.BuildID{ID: "some-id"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)
	assert.Equal(t, "https://buildkite.com/test-org/my-pipeline/builds/7", meta["url"])
}

func TestStatus_RecoversRefAfterCacheMiss(t *testing.T) {
	var listed bool
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Cache miss path: Status lists builds filtered by the stamped metadata.
		require.Equal(t, http.MethodGet, req.Method)
		assert.Contains(t, req.URL.RawQuery, "meta_data")
		listed = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"number":7,"state":"running","web_url":"https://bk/7"}]`))
	}))

	// refs is empty (e.g. after a restart); Status must recover the ref.
	status, meta, err := r.Status(context.Background(), entity.BuildID{ID: "bk-lost"})
	require.NoError(t, err)
	assert.True(t, listed)
	assert.Equal(t, entity.BuildStatusRunning, status)
	assert.Equal(t, "https://bk/7", meta["url"])

	// The recovered ref is now cached for subsequent calls.
	ref, ok := r.lookupRef("bk-lost")
	require.True(t, ok)
	assert.Equal(t, encodeBuildRef(7), ref)
}

func TestStatus_BuildkiteNotFound(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	r.storeRef("some-id", encodeBuildRef(99))

	_, _, err := r.Status(context.Background(), entity.BuildID{ID: "some-id"})
	require.Error(t, err)
}

// --- Cancel ---

func TestCancel_EnqueuesJobAndReturnsImmediately(t *testing.T) {
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("Buildkite API called before cancel drain")
	}))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "some-id"}))
	assert.Len(t, r.cancelCh, 1)
}

func TestCancel_CallsBuildkiteWhenRefKnown(t *testing.T) {
	var cancelCalled bool
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancelCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buildJSON(5, "canceled", ""))
	}))
	r.storeRef("some-id", encodeBuildRef(5))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "some-id"}))
	drainCancel(t, r)
	assert.True(t, cancelCalled)
}

func TestCancel_RecoversRefAfterCacheMiss(t *testing.T) {
	var listed, cancelled bool
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodGet:
			listed = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"number":5,"state":"running","web_url":""}]`))
		case http.MethodPut:
			cancelled = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected %s request", req.Method)
		}
	}))

	// refs is empty; processCancel must recover the ref from metadata first.
	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "bk-lost"}))
	drainCancel(t, r)
	assert.True(t, listed, "cancel should look up the build by metadata on cache miss")
	assert.True(t, cancelled, "cancel should reach Buildkite after recovering the ref")
}

func TestCancel_NoopWhenBuildNotYetSubmitted(t *testing.T) {
	// The metadata lookup finds nothing, so there is nothing to cancel; the
	// handler fails the test if a cancel (PUT) is attempted.
	r := newTestRunner(t, emptyListHandler(t))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "some-id"}))
	drainCancel(t, r)
}

func TestCancel_AlreadyTerminal_Noop(t *testing.T) {
	// Buildkite returns 422 when the build cannot be cancelled (already terminal).
	r := newTestRunner(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	r.storeRef("some-id", encodeBuildRef(5))

	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "some-id"}))
	drainCancel(t, r) // must not panic or error
}

func TestCancel_QueueFull_ReturnsError(t *testing.T) {
	r := newRunner(
		Config{},
		&client{httpClient: http.DefaultClient},
		1, 1,
	)
	require.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "a"}))
	require.Error(t, r.Cancel(context.Background(), entity.BuildID{ID: "b"}))
}

// --- Internal helpers ---

func TestEncodeParseBuildRef_RoundTrip(t *testing.T) {
	for _, n := range []int{1, 9999, 0} {
		ref := encodeBuildRef(n)
		got, err := parseBuildRef(ref)
		require.NoError(t, err)
		assert.Equal(t, n, got)
	}
}

func TestParseBuildRef_Invalid(t *testing.T) {
	for _, ref := range []string{"", "notanumber", "org/pipeline/1"} {
		t.Run(ref, func(t *testing.T) {
			_, err := parseBuildRef(ref)
			require.Error(t, err)
		})
	}
}

func TestNewBuildID_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := newBuildID()
		assert.NotEmpty(t, id)
		assert.False(t, seen[id], "duplicate build ID: %s", id)
		seen[id] = true
	}
}
