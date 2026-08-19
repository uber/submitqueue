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

package record

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
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
	testQueue   = "monorepo/main"
	testID      = "request/monorepo/main/7"
	testURI     = "git://remote/monorepo/main/head-sha"
	testBaseURI = "git://remote/monorepo/main/base-sha"
)

// Metric names as they appear in a snapshot, so a case asserts on the series an
// operator queries rather than on how the emit is composed.
const (
	failureDetectionLatency = "record_controller.record.failure_detection_latency+queue=monorepo/main,strategy=incremental_since_green"
	failureDetectionMissing = "record_controller.record.failure_detection_missing+queue=monorepo/main,strategy=full"
	failureDetectionErrors  = "record_controller.record.failure_detection_errors+queue=monorepo/main,step="
)

var testChangeTime = time.Unix(1_700_000_000, 0).UTC()

// recordMocks bundles the mocks a record controller test case wires
// expectations on.
type recordMocks struct {
	reqStore      *storagemock.MockRequestStore
	queueStore    *storagemock.MockQueueStore
	factStore     *storagemock.MockValidationFactStore
	sourceControl *sourcecontrolmock.MockSourceControl
	metricsScope  tally.TestScope
}

// expectFactCreated wires a successful fact write and captures it, so a case can
// assert on the recorded degree without pinning the wall-clock CreatedAt.
func (m recordMocks) expectFactCreated(captured *entity.ValidationFact) {
	m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, fact entity.ValidationFact) error {
			*captured = fact
			return nil
		})
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

type staticSourceControlFactory struct {
	sourceControl sourcecontrol.SourceControl
}

func (f staticSourceControlFactory) For(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return f.sourceControl, nil
}

// failingSourceControlFactory resolves no queue.
type failingSourceControlFactory struct{}

func (failingSourceControlFactory) For(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return nil, errors.New("no source control for queue")
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, recordMocks) {
	return newControllerForTopic(t, ctrl, stovepipemq.TopicKeyRecord, "stovepipe-record")
}

func newControllerForTopic(t *testing.T, ctrl *gomock.Controller, topicKey consumer.TopicKey, consumerGroup string) (*Controller, recordMocks) {
	t.Helper()

	scope := tally.NewTestScope("", nil)
	m := recordMocks{
		reqStore:      storagemock.NewMockRequestStore(ctrl),
		queueStore:    storagemock.NewMockQueueStore(ctrl),
		factStore:     storagemock.NewMockValidationFactStore(ctrl),
		sourceControl: sourcecontrolmock.NewMockSourceControl(ctrl),
		metricsScope:  scope,
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()
	store.EXPECT().GetValidationFactStore().Return(m.factStore).AnyTimes()

	c := NewController(
		zap.NewNop().Sugar(),
		scope,
		staticStorageFactory{store: store},
		staticSourceControlFactory{sourceControl: m.sourceControl},
		topicKey,
		consumerGroup,
	)
	return c, m
}

func TestControllerIdentity(t *testing.T) {
	c, _ := newControllerForTopic(t, gomock.NewController(t), consumer.TopicKey("record_dlq"), "stovepipe-record-dlq")

	assert.Equal(t, "record_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("record_dlq"), c.TopicKey())
	assert.Equal(t, "stovepipe-record-dlq", c.ConsumerGroup())
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, testID, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func recordPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.Record{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return b
}

// requestWithState returns a terminal-stage Request in the given state.
func requestWithState(state entity.RequestState) entity.Request {
	return entity.Request{
		ID:      testID,
		Queue:   testQueue,
		URI:     testURI,
		State:   state,
		Version: 2,
	}
}

// failedRequest returns a failed request validated incrementally against
// testBaseURI — the shape that has a detection latency to report.
func failedRequest() entity.Request {
	request := requestWithState(entity.RequestStateFailed)
	request.BaseURI = testBaseURI
	request.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
	return request
}

// totalSamples sums the samples across a duration histogram's buckets.
func totalSamples(buckets map[time.Duration]int64) int64 {
	var sum int64
	for _, count := range buckets {
		sum += count
	}
	return sum
}

// queueRow returns the testQueue's row holding the given bookmark.
func queueRow(lastGreenURI, lastGreenRequestID string, version int32) entity.Queue {
	return entity.Queue{
		Name:               testQueue,
		LastGreenURI:       lastGreenURI,
		LastGreenRequestID: lastGreenRequestID,
		Version:            version,
	}
}

func TestProcess_AdvancesBookmarkOnSuccess(t *testing.T) {
	tests := []struct {
		name    string
		stored  entity.Queue
		wantURI string
	}{
		{
			name:    "bookmark unset",
			stored:  queueRow("", "", 1),
			wantURI: testURI,
		},
		{
			name:    "stored bookmark is older",
			stored:  queueRow("git://remote/monorepo/main/old", "request/monorepo/main/3", 4),
			wantURI: testURI,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			var fact entity.ValidationFact
			m.expectFactCreated(&fact)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(tt.stored, nil)

			var written entity.Queue
			m.queueStore.EXPECT().
				Update(gomock.Any(), gomock.Any(), tt.stored.Version, tt.stored.Version+1).
				DoAndReturn(func(_ context.Context, q entity.Queue, _, _ int32) error {
					written = q
					return nil
				})
			m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).
				Return(sourcecontrol.ChangeInfo{CreatedAt: testChangeTime.UnixMilli()}, nil)
			m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(nil)

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
			assert.Equal(t, tt.wantURI, written.LastGreenURI)
			assert.Equal(t, testID, written.LastGreenRequestID)

			gauge, ok := m.metricsScope.Snapshot().Gauges()["record_controller.record.last_green_timestamp_seconds+queue=monorepo/main"]
			require.True(t, ok)
			assert.Equal(t, float64(testChangeTime.Unix()), gauge.Value())

			// The green fact is what authorises the advance.
			assert.Equal(t, entity.DegreeGreen, fact.Degree)
			assert.Equal(t, testURI, fact.URI)
			assert.Equal(t, testID, fact.RequestID)
			assert.Equal(t, wholeRepositoryProject, fact.Project)
			assert.Positive(t, fact.CreatedAt)
		})
	}
}

func TestProcess_TimestampReportingFailureDoesNotFailRecord(t *testing.T) {
	tests := []struct {
		name        string
		info        sourcecontrol.ChangeInfo
		err         error
		wantCounter string
	}{
		{
			name:        "lookup fails",
			err:         errors.New("boom"),
			wantCounter: "record_controller.record.last_green_timestamp_errors+queue=monorepo/main",
		},
		{
			// A zero timestamp breaks the extension contract, so it is counted
			// apart from a lookup failure rather than emitted as a 1970 gauge.
			name:        "timestamp missing",
			info:        sourcecontrol.ChangeInfo{CreatedAt: 0},
			wantCounter: "record_controller.record.last_green_timestamp_invalid+queue=monorepo/main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			var fact entity.ValidationFact
			m.expectFactCreated(&fact)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
			m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
			m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).Return(tt.info, tt.err)
			m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(nil)

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
			assert.Empty(t, m.metricsScope.Snapshot().Gauges())
			counter, ok := m.metricsScope.Snapshot().Counters()[tt.wantCounter]
			require.True(t, ok)
			assert.Equal(t, int64(1), counter.Value())
		})
	}
}

// A backend that cannot be resolved leaves the timestamp unreported, which is
// counted and swallowed. The promotion that follows needs the same backend, so it
// is what fails the record and sends the message round again.
func TestProcess_UnresolvableSourceControlCountsTimestampFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)
	c.sourceControl = failingSourceControlFactory{}

	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)
	var fact entity.ValidationFact
	m.expectFactCreated(&fact)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
	m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
	assert.Empty(t, m.metricsScope.Snapshot().Gauges())
	counter, ok := m.metricsScope.Snapshot().Counters()["record_controller.record.last_green_timestamp_resolve_errors+queue=monorepo/main"]
	require.True(t, ok)
	assert.Equal(t, int64(1), counter.Value())
}

func TestProcess_RecordsBrokenFactWithoutAdvancing(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateFailed), nil)

	var fact entity.ValidationFact
	m.expectFactCreated(&fact)
	// No queue reads or writes: a broken fact never moves the bookmark.

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
	assert.Equal(t, entity.DegreeBroken, fact.Degree)
	assert.False(t, fact.IsGreen())
}

func TestProcess_ReportsFailureDetectionLatency(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(failedRequest(), nil)
	var fact entity.ValidationFact
	m.expectFactCreated(&fact)
	m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testBaseURI).
		Return(sourcecontrol.ChangeInfo{CreatedAt: time.Now().Add(-time.Hour).UnixMilli()}, nil)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))

	histogram, ok := m.metricsScope.Snapshot().Histograms()[failureDetectionLatency]
	require.True(t, ok)
	assert.EqualValues(t, 1, totalSamples(histogram.Durations()))
}

// TestProcess_FullBuildFailureHasNoBaseline covers a full build: it pins no base
// commit, so there is nothing to measure the latency from. That is the ordinary case
// for the strategy, not a fault, so it must not land among the errors.
func TestProcess_FullBuildFailureHasNoBaseline(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	request := failedRequest()
	request.BaseURI = ""
	request.BuildStrategy = entity.BuildStrategyFull

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
	var fact entity.ValidationFact
	m.expectFactCreated(&fact)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))

	snapshot := m.metricsScope.Snapshot()
	assert.Empty(t, snapshot.Histograms(), "a build with no baseline has no latency to report")
	counter, ok := snapshot.Counters()[failureDetectionMissing]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

// TestProcess_RedeliveredFailureIsNotResampled covers a redelivery that adopts a
// broken fact it already wrote: one break must contribute one sample, or the
// distribution counts the flakiest deliveries twice.
func TestProcess_RedeliveredFailureIsNotResampled(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(failedRequest(), nil)
	m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	m.factStore.EXPECT().Get(gomock.Any(), testURI, wholeRepositoryProject).
		Return(entity.ValidationFact{URI: testURI, Degree: entity.DegreeBroken, RequestID: testID}, nil)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
	assert.Empty(t, m.metricsScope.Snapshot().Histograms())
}

// TestProcess_UnobservableDetectionLatencyDoesNotFailRecord covers the observation's
// error posture: every way it can fail is counted with the step that failed and
// swallowed, because a reporting fault must not disturb the fact already recorded.
func TestProcess_UnobservableDetectionLatencyDoesNotFailRecord(t *testing.T) {
	tests := []struct {
		name  string
		step  string
		setup func(c *Controller, m recordMocks)
	}{
		{
			name: "source control cannot be resolved",
			step: "resolve_source_control",
			setup: func(c *Controller, _ recordMocks) {
				c.sourceControl = failingSourceControlFactory{}
			},
		},
		{
			name: "the base change cannot be looked up",
			step: "get_change_info",
			setup: func(_ *Controller, m recordMocks) {
				m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testBaseURI).
					Return(sourcecontrol.ChangeInfo{}, errors.New("boom"))
			},
		},
		{
			name: "the base change is undated",
			step: "undated_change",
			setup: func(_ *Controller, m recordMocks) {
				m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testBaseURI).
					Return(sourcecontrol.ChangeInfo{CreatedAt: 0}, nil)
			},
		},
		{
			name: "the base change is dated in the future",
			step: "future_change",
			setup: func(_ *Controller, m recordMocks) {
				m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testBaseURI).
					Return(sourcecontrol.ChangeInfo{CreatedAt: time.Now().Add(time.Hour).UnixMilli()}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			tt.setup(c, m)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(failedRequest(), nil)
			var fact entity.ValidationFact
			m.expectFactCreated(&fact)

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))

			snapshot := m.metricsScope.Snapshot()
			assert.Empty(t, snapshot.Histograms(), "no latency may be reported when it cannot be observed")
			counter, ok := snapshot.Counters()[failureDetectionErrors+tt.step]
			require.True(t, ok)
			assert.EqualValues(t, 1, counter.Value())
		})
	}
}

func TestProcess_AdoptsExistingFactFromSameRequest(t *testing.T) {
	tests := []struct {
		name       string
		stored     entity.ValidationFact
		wantUpdate bool
	}{
		{
			name:       "green fact still advances the bookmark",
			stored:     entity.ValidationFact{URI: testURI, Degree: entity.DegreeGreen, RequestID: testID},
			wantUpdate: true,
		},
		{
			name:   "broken fact does not",
			stored: entity.ValidationFact{URI: testURI, Degree: entity.DegreeBroken, RequestID: testID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			// A redelivery after the fact was written but before the bookmark
			// advanced: the write loses, and the stored fact decides.
			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
			m.factStore.EXPECT().Get(gomock.Any(), testURI, wholeRepositoryProject).Return(tt.stored, nil)

			if tt.wantUpdate {
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
				m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
				m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).
					Return(sourcecontrol.ChangeInfo{CreatedAt: testChangeTime.UnixMilli()}, nil)
				m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(nil)
			}

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_ExistingFactFromDifferentRequestFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)
	m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	m.factStore.EXPECT().Get(gomock.Any(), testURI, wholeRepositoryProject).
		Return(entity.ValidationFact{URI: testURI, Degree: entity.DegreeGreen, RequestID: "request/monorepo/main/1"}, nil)
	// The bookmark must not move on an identity this request does not own.

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
}

func TestProcess_SkipsBookmarkWhenNotNewer(t *testing.T) {
	tests := []struct {
		name        string
		stored      entity.Queue
		wantPromote bool
	}{
		{
			// The request already holds the bookmark, so it still owns the ref:
			// the promotion is retried in case the first attempt never landed.
			name:        "same request redelivered",
			stored:      queueRow(testURI, testID, 3),
			wantPromote: true,
		},
		{
			// A newer green commit owns the bookmark and the ref, so promoting
			// this one would move the ref backwards.
			name:   "stored bookmark is newer",
			stored: queueRow("git://remote/monorepo/main/newer", "request/monorepo/main/9", 5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			var fact entity.ValidationFact
			m.expectFactCreated(&fact)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(tt.stored, nil)
			// No Update: the bookmark only moves forward.

			if tt.wantPromote {
				m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(nil)
			}

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
			assert.NotContains(
				t,
				m.metricsScope.Snapshot().Gauges(),
				"record_controller.record.last_green_timestamp_seconds+queue=monorepo/main",
			)
		})
	}
}

func TestProcess_SkipsPromotionWhenCommitLeftTheRef(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)
	var fact entity.ValidationFact
	m.expectFactCreated(&fact)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
	m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
	m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).
		Return(sourcecontrol.ChangeInfo{CreatedAt: testChangeTime.UnixMilli()}, nil)

	// A rewritten history dropped the commit from the ref: no retry can promote
	// it, so the message is acked rather than sent round again.
	m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(sourcecontrol.ErrNotFound)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
}

func TestProcess_PromotionErrorsPropagate(t *testing.T) {
	tests := []struct {
		name  string
		setup func(c *Controller, m recordMocks)
	}{
		{
			name: "source control resolve fails",
			setup: func(c *Controller, _ recordMocks) {
				c.sourceControl = failingSourceControlFactory{}
			},
		},
		{
			name: "promote fails",
			setup: func(_ *Controller, m recordMocks) {
				m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).
					Return(sourcecontrol.ChangeInfo{CreatedAt: testChangeTime.UnixMilli()}, nil)
				m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(errors.New("boom"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			var fact entity.ValidationFact
			m.expectFactCreated(&fact)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
			m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
			tt.setup(c, m)

			// The bookmark already advanced, so the redelivery re-promotes the
			// same commit; failing here is what makes that retry happen.
			require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_TerminalWithoutFactDoesNotTouchStores(t *testing.T) {
	tests := []struct {
		name  string
		state entity.RequestState
	}{
		{name: "cancelled", state: entity.RequestStateCancelled},
		{name: "superseded", state: entity.RequestStateSuperseded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(tt.state), nil)
			// Neither a fact nor a queue write: these outcomes decide nothing.

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_NonTerminalRequestFails(t *testing.T) {
	tests := []struct {
		name  string
		state entity.RequestState
	}{
		{name: "accepted", state: entity.RequestStateAccepted},
		{name: "processing", state: entity.RequestStateProcessing},
		{name: "unknown", state: entity.RequestStateUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(tt.state), nil)

			require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_RetriesBookmarkOnVersionMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	stale := queueRow("", "", 1)
	fresh := queueRow("", "", 2)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)
	var fact entity.ValidationFact
	m.expectFactCreated(&fact)

	gomock.InOrder(
		m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(stale, nil),
		m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), stale.Version, stale.Version+1).
			Return(storage.ErrVersionMismatch),
		m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(fresh, nil),
		m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), fresh.Version, fresh.Version+1).
			Return(nil),
	)
	m.sourceControl.EXPECT().ChangeInfo(gomock.Any(), testURI).
		Return(sourcecontrol.ChangeInfo{CreatedAt: testChangeTime.UnixMilli()}, nil)
	m.sourceControl.EXPECT().Promote(gomock.Any(), testURI).Return(nil)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
}

func TestProcess_MalformedRequestIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	request := requestWithState(entity.RequestStateSucceeded)
	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
	var fact entity.ValidationFact
	m.expectFactCreated(&fact)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).
		Return(queueRow("git://remote/monorepo/main/old", "not-a-request-id", 1), nil)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
}

func TestProcess_StorageErrorsPropagate(t *testing.T) {
	tests := []struct {
		name  string
		setup func(m recordMocks)
	}{
		{
			name: "request load fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(entity.Request{}, errors.New("boom"))
			},
		},
		{
			name: "fact create fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(requestWithState(entity.RequestStateSucceeded), nil)
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("boom"))
			},
		},
		{
			name: "existing fact load fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(requestWithState(entity.RequestStateSucceeded), nil)
				m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
				m.factStore.EXPECT().Get(gomock.Any(), testURI, wholeRepositoryProject).
					Return(entity.ValidationFact{}, errors.New("boom"))
			},
		},
		{
			name: "queue load fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(requestWithState(entity.RequestStateSucceeded), nil)
				var fact entity.ValidationFact
				m.expectFactCreated(&fact)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).
					Return(entity.Queue{}, errors.New("boom"))
			},
		},
		{
			name: "queue update fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(requestWithState(entity.RequestStateSucceeded), nil)
				var fact entity.ValidationFact
				m.expectFactCreated(&fact)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
				m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).
					Return(errors.New("boom"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			tt.setup(m)

			require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, []byte("not-protojson"))))
}

func TestProcess_QueueMismatchFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	// The request's own queue is authoritative; a payload disagreeing with it
	// is malformed, so the bookmark must not be touched.
	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)

	payload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: testID, QueueName: "monorepo/other"})
	require.NoError(t, err)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, payload)))
}
