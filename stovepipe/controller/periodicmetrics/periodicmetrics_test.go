// Copyright (c) 2026 Uber Technologies, Inc.
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

package periodicmetrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tally "github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcecontrolmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

const (
	_testNowMs = int64(1_700_000_000_123)

	_testQueue   = "example-queue"
	_testURI     = "git://github.com/example/repo/refs%2Fheads%2Fmain/abc"
	_testHeadURI = "git://github.com/example/repo/refs%2Fheads%2Fmain/def"

	_timestampSeconds     = "stovepipe.periodicmetrics.last_green_timestamp_seconds+queue=example-queue"
	_durationSeconds      = "stovepipe.periodicmetrics.last_green_duration_seconds+queue=example-queue"
	_headTimestampSeconds = "stovepipe.periodicmetrics.ref_head_timestamp_seconds+queue=example-queue"
	_behindHeadSeconds    = "stovepipe.periodicmetrics.last_green_behind_head_seconds+queue=example-queue"
	_counterPrefix        = "stovepipe.periodicmetrics."
	_queueTagSuffix       = "+queue=example-queue"
)

type controllerMocks struct {
	stores         *storagemock.MockFactory
	store          *storagemock.MockStorage
	queueStore     *storagemock.MockQueueStore
	sourceControls *sourcecontrolmock.MockFactory
	sourceControl  *sourcecontrolmock.MockSourceControl
}

func (m controllerMocks) expectQueue(lastGreenURI string) {
	m.stores.EXPECT().For(storage.Config{QueueName: _testQueue}).Return(m.store, nil)
	m.store.EXPECT().GetQueueStore().Return(m.queueStore)
	m.queueStore.EXPECT().Get(gomock.Any(), _testQueue).
		Return(entity.Queue{Name: _testQueue, LastGreenURI: lastGreenURI}, nil)
}

func (m controllerMocks) expectChangeInfo(info sourcecontrol.ChangeInfo, err error) {
	m.sourceControls.EXPECT().For(sourcecontrol.Config{QueueName: _testQueue}).Return(m.sourceControl, nil)
	m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), _testURI).Return(info, err)
}

func (m controllerMocks) expectRefHead(info sourcecontrol.ChangeInfo, err error) {
	m.sourceControl.EXPECT().Latest(gomock.Any()).Return(_testHeadURI, nil)
	m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), _testHeadURI).Return(info, err)
}

func newTestController(t *testing.T, ctrl *gomock.Controller) (*controller, controllerMocks, tally.TestScope) {
	t.Helper()

	m := controllerMocks{
		stores:         storagemock.NewMockFactory(ctrl),
		store:          storagemock.NewMockStorage(ctrl),
		queueStore:     storagemock.NewMockQueueStore(ctrl),
		sourceControls: sourcecontrolmock.NewMockFactory(ctrl),
		sourceControl:  sourcecontrolmock.NewMockSourceControl(ctrl),
	}

	scope := tally.NewTestScope("stovepipe", nil)
	observer := New(scope, m.stores, m.sourceControls).(*controller)
	observer.now = func() time.Time { return time.UnixMilli(_testNowMs) }
	return observer, m, scope
}

func TestObserve_ReportsLastGreen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, m, scope := newTestController(t, ctrl)
	createdAt := time.UnixMilli(_testNowMs).Add(-time.Hour)

	m.expectQueue(_testURI)
	m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)
	m.expectRefHead(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)

	require.NoError(t, c.Observe(context.Background(), _testQueue))

	snapshot := scope.Snapshot()

	timestamp, ok := snapshot.Gauges()[_timestampSeconds]
	require.True(t, ok)
	assert.EqualValues(t, createdAt.Unix(), timestamp.Value())

	duration, ok := snapshot.Gauges()[_durationSeconds]
	require.True(t, ok)
	assert.Equal(t, time.Hour.Seconds(), duration.Value())

	assertCounter(t, snapshot, "observed")
}

func TestObserve_ReportsRefHead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, m, scope := newTestController(t, ctrl)
	createdAt := time.UnixMilli(_testNowMs).Add(-time.Hour)
	headCreatedAt := time.UnixMilli(_testNowMs).Add(-20 * time.Minute)

	m.expectQueue(_testURI)
	m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)
	m.expectRefHead(sourcecontrol.ChangeInfo{CreatedAt: headCreatedAt.UnixMilli()}, nil)

	require.NoError(t, c.Observe(context.Background(), _testQueue))

	snapshot := scope.Snapshot()

	headTimestamp, ok := snapshot.Gauges()[_headTimestampSeconds]
	require.True(t, ok)
	assert.EqualValues(t, headCreatedAt.Unix(), headTimestamp.Value())

	behind, ok := snapshot.Gauges()[_behindHeadSeconds]
	require.True(t, ok)
	assert.Equal(t, (40 * time.Minute).Seconds(), behind.Value())

	assertCounter(t, snapshot, "observed")
}

func TestObserve_ReportsNothingBehindACaughtUpRef(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, m, scope := newTestController(t, ctrl)
	createdAt := time.UnixMilli(_testNowMs).Add(-72 * time.Hour)

	m.expectQueue(_testURI)
	m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)
	m.expectRefHead(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)

	require.NoError(t, c.Observe(context.Background(), _testQueue))

	snapshot := scope.Snapshot()

	behind, ok := snapshot.Gauges()[_behindHeadSeconds]
	require.True(t, ok)
	assert.Zero(t, behind.Value())

	duration, ok := snapshot.Gauges()[_durationSeconds]
	require.True(t, ok)
	assert.Equal(t, (72 * time.Hour).Seconds(), duration.Value())
}

func TestObserve_ReportsNoNegativeDistance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, m, scope := newTestController(t, ctrl)
	createdAt := time.UnixMilli(_testNowMs).Add(-time.Minute)
	headCreatedAt := time.UnixMilli(_testNowMs).Add(-time.Hour)

	m.expectQueue(_testURI)
	m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)
	m.expectRefHead(sourcecontrol.ChangeInfo{CreatedAt: headCreatedAt.UnixMilli()}, nil)

	require.NoError(t, c.Observe(context.Background(), _testQueue))

	snapshot := scope.Snapshot()

	behind, ok := snapshot.Gauges()[_behindHeadSeconds]
	require.True(t, ok)
	assert.Zero(t, behind.Value())

	headTimestamp, ok := snapshot.Gauges()[_headTimestampSeconds]
	require.True(t, ok)
	assert.EqualValues(t, headCreatedAt.Unix(), headTimestamp.Value())

	assertCounter(t, snapshot, "last_green_ahead_of_head")
}

func TestObserve_RefHeadErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		counter string
		setup   func(m controllerMocks)
	}{
		{
			name:    "the ref head cannot be resolved",
			counter: "ref_head_timestamp_errors",
			setup: func(m controllerMocks) {
				m.sourceControl.EXPECT().Latest(gomock.Any()).Return("", errors.New("boom"))
			},
		},
		{
			name:    "dating the ref head fails",
			counter: "ref_head_timestamp_errors",
			setup: func(m controllerMocks) {
				m.expectRefHead(sourcecontrol.ChangeInfo{}, errors.New("boom"))
			},
		},
		{
			name:    "the ref head is undated",
			counter: "ref_head_timestamp_invalid",
			setup: func(m controllerMocks) {
				m.expectRefHead(sourcecontrol.ChangeInfo{}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			c, m, scope := newTestController(t, ctrl)

			m.expectQueue(_testURI)
			m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: _testNowMs}, nil)
			tt.setup(m)

			require.Error(t, c.Observe(context.Background(), _testQueue))

			snapshot := scope.Snapshot()
			assert.NotContains(t, snapshot.Gauges(), _headTimestampSeconds)
			assert.NotContains(t, snapshot.Gauges(), _behindHeadSeconds)
			assert.Contains(t, snapshot.Gauges(), _timestampSeconds)
			assert.NotContains(t, snapshot.Counters(), _counterPrefix+"observed"+_queueTagSuffix)
			assertCounter(t, snapshot, tt.counter)
		})
	}
}

func TestObserve_ReportsNoNegativeDuration(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, m, scope := newTestController(t, ctrl)
	createdAt := time.UnixMilli(_testNowMs).Add(time.Hour)

	m.expectQueue(_testURI)
	m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)
	m.expectRefHead(sourcecontrol.ChangeInfo{CreatedAt: createdAt.UnixMilli()}, nil)

	require.NoError(t, c.Observe(context.Background(), _testQueue))

	snapshot := scope.Snapshot()

	duration, ok := snapshot.Gauges()[_durationSeconds]
	require.True(t, ok)
	assert.Zero(t, duration.Value())

	timestamp, ok := snapshot.Gauges()[_timestampSeconds]
	require.True(t, ok)
	assert.EqualValues(t, createdAt.Unix(), timestamp.Value())

	assertCounter(t, snapshot, "last_green_timestamp_future")
}

func TestObserve_ReportsMissingLastGreen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, m, scope := newTestController(t, ctrl)

	m.expectQueue("")

	require.NoError(t, c.Observe(context.Background(), _testQueue))

	snapshot := scope.Snapshot()
	assert.Empty(t, snapshot.Gauges(), "a queue that has never gone green has no last green to report")
	assertCounter(t, snapshot, "last_green_missing")
	assertCounter(t, snapshot, "observed")
}

func TestObserve_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		counter string
		setup   func(m controllerMocks)
	}{
		{
			name:    "storage cannot be resolved",
			counter: "last_green_timestamp_storage_errors",
			setup: func(m controllerMocks) {
				m.stores.EXPECT().For(gomock.Any()).Return(nil, errors.New("boom"))
			},
		},
		{
			name:    "the queue cannot be read",
			counter: "last_green_timestamp_storage_errors",
			setup: func(m controllerMocks) {
				m.stores.EXPECT().For(gomock.Any()).Return(m.store, nil)
				m.store.EXPECT().GetQueueStore().Return(m.queueStore)
				m.queueStore.EXPECT().Get(gomock.Any(), _testQueue).
					Return(entity.Queue{}, errors.New("boom"))
			},
		},
		{
			name:    "source control cannot be resolved",
			counter: "last_green_timestamp_resolve_errors",
			setup: func(m controllerMocks) {
				m.expectQueue(_testURI)
				m.sourceControls.EXPECT().For(gomock.Any()).Return(nil, errors.New("boom"))
			},
		},
		{
			name:    "dating the change fails",
			counter: "last_green_timestamp_errors",
			setup: func(m controllerMocks) {
				m.expectQueue(_testURI)
				m.expectChangeInfo(sourcecontrol.ChangeInfo{}, errors.New("boom"))
			},
		},
		{
			name:    "the change is undated",
			counter: "last_green_timestamp_invalid",
			setup: func(m controllerMocks) {
				m.expectQueue(_testURI)
				m.expectChangeInfo(sourcecontrol.ChangeInfo{}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			c, m, scope := newTestController(t, ctrl)
			tt.setup(m)

			require.Error(t, c.Observe(context.Background(), _testQueue))

			snapshot := scope.Snapshot()
			assert.Empty(t, snapshot.Gauges(), "nothing may be reported when it cannot be observed")
			assert.NotContains(t, snapshot.Counters(), _counterPrefix+"observed"+_queueTagSuffix)
			assertCounter(t, snapshot, tt.counter)
		})
	}
}

func TestObserve_RejectsUnnamedQueue(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	c, _, scope := newTestController(t, ctrl)

	require.Error(t, c.Observe(context.Background(), ""))
	assert.Empty(t, scope.Snapshot().Gauges())
}

func assertCounter(t *testing.T, snapshot tally.Snapshot, name string) {
	t.Helper()

	counter, ok := snapshot.Counters()[_counterPrefix+name+_queueTagSuffix]
	require.True(t, ok, "expected counter %s", name)
	assert.EqualValues(t, 1, counter.Value())
}
