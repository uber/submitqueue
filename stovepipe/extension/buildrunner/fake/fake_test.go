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

package fake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
)

// testCfg is the per-queue identity used by every case in this file.
var testCfg = buildrunner.Config{QueueName: "test-queue"}

// newFake constructs a fake for the test queue from params, failing the test if
// they are invalid. Most tests want default behavior, for which the zero-value
// Params suffice.
func newFake(t *testing.T, params Params) buildrunner.BuildRunner {
	t.Helper()
	params.Config = testCfg
	runner, err := New(params)
	require.NoError(t, err)
	return runner
}

func TestNew_ImplementsInterface(t *testing.T) {
	var _ buildrunner.BuildRunner = newFake(t, Params{})
}

func TestTrigger(t *testing.T) {
	tests := []struct {
		name    string
		headURI string
		wantErr bool
	}{
		{name: "no marker succeeds", headURI: "git://repo/ref/deadbeef"},
		{name: "unrelated query params succeed", headURI: "git://repo/ref/deadbeef?attempt=2"},
		{name: "trigger-error marker fails", headURI: "git://repo/ref/deadbeef?buildrunner-fake=trigger-error", wantErr: true},
		{name: "build-fail marker still triggers", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-fail"},
		{name: "build-error marker still triggers", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := newFake(t, Params{}).Trigger(context.Background(), "", tt.headURI, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, id.ID)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, id.ID)
		})
	}
}

func TestTrigger_UniqueIDs(t *testing.T) {
	a, err := newFake(t, Params{}).Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
	require.NoError(t, err)
	b, err := newFake(t, Params{}).Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
	require.NoError(t, err)
	assert.NotEqual(t, a.ID, b.ID)
}

func TestStatus(t *testing.T) {
	tests := []struct {
		name       string
		headURI    string
		wantStatus entity.BuildStatus
		wantErr    bool
	}{
		{name: "no marker succeeds", headURI: "git://repo/ref/deadbeef", wantStatus: entity.BuildStatusSucceeded},
		{name: "build-fail marker fails", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-fail", wantStatus: entity.BuildStatusFailed},
		{name: "build-error marker errors", headURI: "git://repo/ref/deadbeef?buildrunner-fake=build-error", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := newFake(t, Params{}).Trigger(context.Background(), "", tt.headURI, nil)
			require.NoError(t, err)

			status, metadata, err := newFake(t, Params{}).Status(context.Background(), id)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
			assert.Nil(t, metadata)
		})
	}
}

func TestStatus_UnrecognizedIDSucceeds(t *testing.T) {
	status, metadata, err := newFake(t, Params{}).Status(context.Background(), entity.BuildID{ID: "not-minted-by-this-fake"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
	assert.Nil(t, metadata)
}

func TestStatus_StatelessAcrossInstances(t *testing.T) {
	id, err := newFake(t, Params{}).Trigger(context.Background(), "", "git://repo/ref/deadbeef?buildrunner-fake=build-fail", nil)
	require.NoError(t, err)

	status, _, err := newFake(t, Params{}).Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusFailed, status)
}

func TestCancel_NoOp(t *testing.T) {
	err := newFake(t, Params{}).Cancel(context.Background(), entity.BuildID{ID: "anything"})
	assert.NoError(t, err)
}

// TestStatus_BuildSlowReportsRunningThenSucceeds covers the one marker that yields a
// non-terminal status, which is what makes a caller's poll loop reachable in an
// integration or e2e stack.
func TestStatus_BuildSlowReportsRunningThenSucceeds(t *testing.T) {
	// A window long enough that the build is still running when Status is called.
	slow := newRunner(Params{})
	slow.slowBuildDuration = 30 * time.Second

	id, err := slow.Trigger(context.Background(), "", "git://repo/ref/deadbeef?buildrunner-fake=build-slow", nil)
	require.NoError(t, err)

	status, _, err := newFake(t, Params{}).Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)

	// An id whose deadline has already passed reports the terminal outcome. Encoding
	// the deadline in the id is what keeps Status stateless across instances.
	elapsed := entity.BuildID{ID: fmt.Sprintf("fake-ok-%d-abcd1234", time.Now().UnixMilli()-1)}
	status, _, err = newFake(t, Params{}).Status(context.Background(), elapsed)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}

// TestStatus_WithoutDeadlineIsTerminal pins the fallback: an id carrying no parsable
// deadline is treated as already terminal rather than polling forever. Ids minted
// before the deadline became part of every id take this path.
func TestStatus_WithoutDeadlineIsTerminal(t *testing.T) {
	tests := []struct {
		name       string
		buildID    string
		wantStatus entity.BuildStatus
	}{
		{name: "no deadline segment", buildID: "fake-build-slow-nodeadline", wantStatus: entity.BuildStatusSucceeded},
		{name: "legacy succeeded id", buildID: "fake-ok-abcd1234", wantStatus: entity.BuildStatusSucceeded},
		{name: "legacy failed id", buildID: "fake-build-fail-abcd1234", wantStatus: entity.BuildStatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, err := newFake(t, Params{}).Status(context.Background(), entity.BuildID{ID: tt.buildID})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		params  Params
		wantErr bool
	}{
		{name: "zero value is valid", params: Params{}},
		{name: "rate and duration", params: Params{FailurePercent: 50, BuildDuration: time.Second}},
		{name: "always fails", params: Params{FailurePercent: 100}},
		{name: "negative percent", params: Params{FailurePercent: -1}, wantErr: true},
		{name: "percent above 100", params: Params{FailurePercent: 101}, wantErr: true},
		{name: "negative duration", params: Params{BuildDuration: -time.Second}, wantErr: true},
		{name: "duration with jitter", params: Params{BuildDuration: time.Minute, DurationJitterPercent: 25}},
		{name: "full jitter", params: Params{BuildDuration: time.Minute, DurationJitterPercent: 100}},
		{name: "negative jitter", params: Params{DurationJitterPercent: -1}, wantErr: true},
		{name: "jitter above 100", params: Params{DurationJitterPercent: 101}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.params)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestStatus_FailurePercent covers the configured outcome rate at both extremes,
// where the drawn outcome is fully determined.
func TestStatus_FailurePercent(t *testing.T) {
	tests := []struct {
		name           string
		failurePercent int
		wantStatus     entity.BuildStatus
	}{
		{name: "never fails", failurePercent: 0, wantStatus: entity.BuildStatusSucceeded},
		{name: "always fails", failurePercent: 100, wantStatus: entity.BuildStatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newFake(t, Params{FailurePercent: tt.failurePercent})

			for range 20 {
				id, err := r.Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
				require.NoError(t, err)

				status, _, err := r.Status(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, status)
			}
		})
	}
}

// TestDrawOutcome_Boundary pins how a partial rate maps a draw onto an outcome,
// which the extremes above cannot distinguish.
func TestDrawOutcome_Boundary(t *testing.T) {
	tests := []struct {
		name           string
		failurePercent int
		draw           int
		wantOutcome    string
	}{
		{name: "draw below rate fails", failurePercent: 25, draw: 24, wantOutcome: _tokenFail},
		{name: "draw at rate succeeds", failurePercent: 25, draw: 25, wantOutcome: _outcomeOK},
		{name: "zero rate never draws", failurePercent: 0, draw: 0, wantOutcome: _outcomeOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRunner(Params{FailurePercent: tt.failurePercent})
			r.intn = func(int) int { return tt.draw }
			assert.Equal(t, tt.wantOutcome, r.drawOutcome())
		})
	}
}

// TestDrawDuration_Bounds pins the bounds a configured jitter promises, at both
// ends of the draw: the duration never leaves ±jitter percent of the window.
func TestDrawDuration_Bounds(t *testing.T) {
	tests := []struct {
		name         string
		window       time.Duration
		jitter       int
		draw         int
		wantDuration time.Duration
	}{
		{name: "no jitter pins the window", window: time.Minute, jitter: 0, draw: 0, wantDuration: time.Minute},
		{name: "lowest draw is the lower bound", window: time.Minute, jitter: 25, draw: 0, wantDuration: 45 * time.Second},
		{name: "midpoint draw is the window", window: time.Minute, jitter: 25, draw: 25, wantDuration: time.Minute},
		{name: "highest draw is the upper bound", window: time.Minute, jitter: 25, draw: 50, wantDuration: 75 * time.Second},
		{name: "full jitter can erase the window", window: time.Minute, jitter: 100, draw: 0, wantDuration: 0},
		{name: "full jitter can double the window", window: time.Minute, jitter: 100, draw: 200, wantDuration: 2 * time.Minute},
		{name: "no window has nothing to spread", window: 0, jitter: 100, draw: 0, wantDuration: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRunner(Params{BuildDuration: tt.window, DurationJitterPercent: tt.jitter})
			r.intn = func(int) int { return tt.draw }
			assert.Equal(t, tt.wantDuration, r.drawDuration(tt.window))
		})
	}
}

// TestTrigger_DurationJitterStaysWithinBounds covers the jitter as Trigger applies
// it, over real draws: every deadline baked into an id falls inside the window's
// stated bounds, so an integrator can size a poll loop from the configuration
// alone.
func TestTrigger_DurationJitterStaysWithinBounds(t *testing.T) {
	const (
		window = 30 * time.Second
		jitter = 20
	)
	r := newFake(t, Params{BuildDuration: window, DurationJitterPercent: jitter})

	varied := false
	var first int64
	for i := range 50 {
		before := time.Now()
		id, err := r.Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
		require.NoError(t, err)
		after := time.Now()

		_, readyAt := decodeID(id.ID)

		// Trigger measured the deadline from an instant inside [before, after],
		// so the loosest true bounds shift each end by the call's own duration.
		low := before.Add(window * (100 - jitter) / 100).UnixMilli()
		high := after.Add(window * (100 + jitter) / 100).UnixMilli()
		assert.GreaterOrEqual(t, readyAt, low)
		assert.LessOrEqual(t, readyAt, high)

		if i == 0 {
			first = readyAt
		} else if readyAt != first {
			varied = true
		}
	}
	// The draws are random, so this asserts only that they are not all identical —
	// 50 draws over a 12s span collide only if the jitter is not applied at all.
	assert.True(t, varied, "expected jittered deadlines to vary across triggers")
}

// TestStatus_BuildDurationReportsRunningFirst covers a configured build duration
// applying to every build, not just marked ones, and holding the build
// non-terminal until the window elapses.
func TestStatus_BuildDurationReportsRunningFirst(t *testing.T) {
	// A window long enough that the build is still running when Status is called.
	r := newFake(t, Params{FailurePercent: 100, BuildDuration: 30 * time.Second})

	id, err := r.Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
	require.NoError(t, err)

	status, _, err := r.Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)

	// The same build reports its configured outcome once the window has elapsed.
	elapsed := entity.BuildID{ID: fmt.Sprintf("fake-%s-%d-abcd1234", _tokenFail, time.Now().UnixMilli()-1)}
	status, _, err = r.Status(context.Background(), elapsed)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusFailed, status)
}

// TestStatus_MarkerOverridesConfiguredRate covers a marker pinning the outcome for
// its own build, so a stack running a failure rate can still ask for a specific
// outcome per request.
func TestStatus_MarkerOverridesConfiguredRate(t *testing.T) {
	tests := []struct {
		name           string
		failurePercent int
		headURI        string
		wantStatus     entity.BuildStatus
	}{
		{
			name:           "fail marker under a never-fail rate",
			failurePercent: 0,
			headURI:        "git://repo/ref/deadbeef?buildrunner-fake=build-fail",
			wantStatus:     entity.BuildStatusFailed,
		},
		{
			name:           "unmarked build under an always-fail rate",
			failurePercent: 100,
			headURI:        "git://repo/ref/deadbeef",
			wantStatus:     entity.BuildStatusFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newFake(t, Params{FailurePercent: tt.failurePercent})

			id, err := r.Trigger(context.Background(), "", tt.headURI, nil)
			require.NoError(t, err)

			status, _, err := r.Status(context.Background(), id)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

// TestMarker_StopsAtPathSegment covers a marker that arrives mid-URI rather than at the
// end, as it does when the head URI is built as "git://<queue>/HEAD" and the marker
// rides in on the queue name.
func TestMarker_StopsAtPathSegment(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{name: "trailing path segment", uri: "git://repo/main?buildrunner-fake=build-slow/HEAD", want: "build-slow"},
		{name: "end of uri", uri: "git://repo/ref/deadbeef?buildrunner-fake=build-fail", want: "build-fail"},
		{name: "query separator", uri: "git://repo/ref?buildrunner-fake=build-fail&other=1", want: "build-fail"},
		{name: "no marker", uri: "git://repo/ref/deadbeef", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, marker(tt.uri))
		})
	}
}
