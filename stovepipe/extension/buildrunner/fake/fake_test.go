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

func TestNew_ImplementsInterface(t *testing.T) {
	var _ buildrunner.BuildRunner = New(testCfg)
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
			id, err := New(testCfg).Trigger(context.Background(), "", tt.headURI, nil)
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
	a, err := New(testCfg).Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
	require.NoError(t, err)
	b, err := New(testCfg).Trigger(context.Background(), "", "git://repo/ref/deadbeef", nil)
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
			id, err := New(testCfg).Trigger(context.Background(), "", tt.headURI, nil)
			require.NoError(t, err)

			status, metadata, err := New(testCfg).Status(context.Background(), id)
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
	status, metadata, err := New(testCfg).Status(context.Background(), entity.BuildID{ID: "not-minted-by-this-fake"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
	assert.Nil(t, metadata)
}

func TestStatus_StatelessAcrossInstances(t *testing.T) {
	id, err := New(testCfg).Trigger(context.Background(), "", "git://repo/ref/deadbeef?buildrunner-fake=build-fail", nil)
	require.NoError(t, err)

	status, _, err := New(testCfg).Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusFailed, status)
}

func TestCancel_NoOp(t *testing.T) {
	err := New(testCfg).Cancel(context.Background(), entity.BuildID{ID: "anything"})
	assert.NoError(t, err)
}

// TestStatus_BuildSlowReportsRunningThenSucceeds covers the one marker that yields a
// non-terminal status, which is what makes a caller's poll loop reachable in an
// integration or e2e stack.
func TestStatus_BuildSlowReportsRunningThenSucceeds(t *testing.T) {
	// A window long enough that the build is still running when Status is called.
	slow := runner{slowBuildDuration: 30 * time.Second}

	id, err := slow.Trigger(context.Background(), "", "git://repo/ref/deadbeef?buildrunner-fake=build-slow", nil)
	require.NoError(t, err)

	status, _, err := New(testCfg).Status(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, status)

	// An id whose deadline has already passed reports the terminal outcome. Encoding
	// the deadline in the id is what keeps Status stateless across instances.
	elapsed := entity.BuildID{ID: fmt.Sprintf("fake-build-slow-%d-abcd1234", time.Now().UnixMilli()-1)}
	status, _, err = New(testCfg).Status(context.Background(), elapsed)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}

// TestStatus_BuildSlowWithoutDeadlineSucceeds pins the fallback: an id carrying the
// marker but no parsable deadline is treated as already terminal rather than polling
// forever.
func TestStatus_BuildSlowWithoutDeadlineSucceeds(t *testing.T) {
	status, _, err := New(testCfg).Status(context.Background(), entity.BuildID{ID: "fake-build-slow-nodeadline"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
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
