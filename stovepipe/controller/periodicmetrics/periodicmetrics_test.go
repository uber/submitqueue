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

package periodicmetrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcecontrolmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testURI   = "git://github.com/uber-code/repo/refs%2Fheads%2Fmain/abc"

	// Metric names as they appear in a snapshot, so a case asserts on the series an
	// operator queries rather than on how the emit is composed.
	ageSeconds = "stovepipe.periodicmetrics_controller.last_green.age_seconds+queue=monorepo/main"
	ageMissing = "stovepipe.periodicmetrics_controller.last_green.age_missing+queue=monorepo/main"
	ageErrors  = "stovepipe.periodicmetrics_controller.last_green.age_errors+queue=monorepo/main,step="
)

// periodicMetricsMocks bundles the mocks a test case wires expectations on.
type periodicMetricsMocks struct {
	stores         *storagemock.MockFactory
	store          *storagemock.MockStorage
	queueStore     *storagemock.MockQueueStore
	sourceControls *sourcecontrolmock.MockFactory
	sourceControl  *sourcecontrolmock.MockSourceControl
}

// expectQueue wires resolution down to a queue row holding the given bookmark.
func (m periodicMetricsMocks) expectQueue(lastGreenURI string) {
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.store.EXPECT().GetQueueStore().Return(m.queueStore)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).
		Return(entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI}, nil)
}

// expectChangeInfo wires the dating of the bookmarked commit.
func (m periodicMetricsMocks) expectChangeInfo(info sourcecontrol.ChangeInfo, err error) {
	m.sourceControls.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sourceControl, nil)
	m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).Return(info, err)
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, periodicMetricsMocks, tally.TestScope) {
	t.Helper()

	m := periodicMetricsMocks{
		stores:         storagemock.NewMockFactory(ctrl),
		store:          storagemock.NewMockStorage(ctrl),
		queueStore:     storagemock.NewMockQueueStore(ctrl),
		sourceControls: sourcecontrolmock.NewMockFactory(ctrl),
		sourceControl:  sourcecontrolmock.NewMockSourceControl(ctrl),
	}

	scope := tally.NewTestScope("stovepipe", nil)
	c := NewController(
		zap.NewNop().Sugar(),
		scope,
		m.stores,
		m.sourceControls,
		stovepipemq.TopicKeyPeriodicMetrics,
		"stovepipe-periodicmetrics",
	)
	return c, m, scope
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testQueue, payload, testQueue, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func payload(t *testing.T, queue string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.PeriodicMetrics{QueueName: queue})
	require.NoError(t, err)
	return b
}

func TestProcess_EmitsLastGreenAge(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m, scope := newController(t, ctrl)
	createdAt := time.Now().Add(-time.Hour)

	m.expectQueue(testURI)
	m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: createdAt}, nil)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, payload(t, testQueue))))

	gauge, ok := scope.Snapshot().Gauges()[ageSeconds]
	require.True(t, ok)
	assert.InDelta(t, time.Since(createdAt).Seconds(), gauge.Value(), 1)
}

// TestProcess_RecordsMissingLastGreen covers a queue that has never gone green: it
// has no age, and emitting zero would read as "green as of right now".
func TestProcess_RecordsMissingLastGreen(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m, scope := newController(t, ctrl)

	m.expectQueue("")

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, payload(t, testQueue))))

	snapshot := scope.Snapshot()
	assert.Empty(t, snapshot.Gauges(), "a queue that has never gone green has no age to report")
	counter, ok := snapshot.Counters()[ageMissing]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

// TestProcess_AcksWhenAgeCannotBeObserved covers the stage's error posture: every way
// the observation can fail is counted with the step that failed and acked, because the
// schedule supersedes the missed sample and nothing downstream reads this stage.
func TestProcess_AcksWhenAgeCannotBeObserved(t *testing.T) {
	tests := []struct {
		name  string
		step  string
		setup func(m periodicMetricsMocks)
	}{
		{
			name: "storage cannot be resolved",
			step: "resolve_storage",
			setup: func(m periodicMetricsMocks) {
				m.stores.EXPECT().For(gomock.Any()).Return(nil, errors.New("boom"))
			},
		},
		{
			name: "the queue cannot be read",
			step: "get_queue",
			setup: func(m periodicMetricsMocks) {
				m.stores.EXPECT().For(gomock.Any()).Return(m.store, nil)
				m.store.EXPECT().GetQueueStore().Return(m.queueStore)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).
					Return(entity.Queue{}, errors.New("boom"))
			},
		},
		{
			name: "source control cannot be resolved",
			step: "resolve_source_control",
			setup: func(m periodicMetricsMocks) {
				m.expectQueue(testURI)
				m.sourceControls.EXPECT().For(gomock.Any()).Return(nil, errors.New("boom"))
			},
		},
		{
			name: "change info fails",
			step: "get_change_info",
			setup: func(m periodicMetricsMocks) {
				m.expectQueue(testURI)
				m.expectChangeInfo(sourcecontrol.ChangeInfo{}, errors.New("boom"))
			},
		},
		{
			name: "the change is undated",
			step: "get_change_info",
			setup: func(m periodicMetricsMocks) {
				m.expectQueue(testURI)
				m.expectChangeInfo(sourcecontrol.ChangeInfo{}, nil)
			},
		},
		{
			name: "the change is dated in the future",
			step: "future_change",
			setup: func(m periodicMetricsMocks) {
				m.expectQueue(testURI)
				m.expectChangeInfo(sourcecontrol.ChangeInfo{CreatedAt: time.Now().Add(time.Hour)}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m, scope := newController(t, ctrl)
			tt.setup(m)

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, payload(t, testQueue))))

			snapshot := scope.Snapshot()
			assert.Empty(t, snapshot.Gauges(), "no age may be reported when it cannot be observed")
			counter, ok := snapshot.Counters()[ageErrors+tt.step]
			require.True(t, ok)
			assert.EqualValues(t, 1, counter.Value())
		})
	}
}

func TestProcess_RejectsMalformedMessages(t *testing.T) {
	tests := []struct {
		name    string
		payload func(t *testing.T) []byte
	}{
		{
			name:    "not protojson",
			payload: func(*testing.T) []byte { return []byte("not-protojson") },
		},
		{
			name:    "no queue to observe",
			payload: func(t *testing.T) []byte { return payload(t, "") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, _, _ := newController(t, ctrl)

			require.Error(t, c.Process(context.Background(), delivery(t, ctrl, tt.payload(t))))
		})
	}
}
