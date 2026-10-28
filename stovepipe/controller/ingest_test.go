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

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/extension/counter"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/core/queuestate"
	queuestatemock "github.com/uber/submitqueue/stovepipe/core/queuestate/mock"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	scmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testURI   = "git://repo/monorepo/main/abc123"
)

// ingestMocks bundles the mocks an Ingest test case wires expectations on.
type ingestMocks struct {
	queueEnablementController *queuestatemock.MockTransitionController
	counter                   *countermock.MockCounter
	factory                   *scmock.MockFactory
	sc                        *scmock.MockSourceControl
	reqStore                  *storagemock.MockRequestStore
	uriStore                  *storagemock.MockRequestURIStore
	queueStore                *storagemock.MockQueueStore
	store                     *storagemock.MockStorage
	materializer              *requestlogmock.MockMaterializer
	publisher                 *mqmock.MockPublisher
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

// staticCounterFactory resolves every queue to the same counter, so tests can keep
// setting expectations on one mock regardless of which queue the controller resolves.
type staticCounterFactory struct{ counter counter.Counter }

func (f staticCounterFactory) For(counter.Config) (counter.Counter, error) { return f.counter, nil }

func newIngestController(t *testing.T, ctrl *gomock.Controller) (*IngestController, ingestMocks) {
	t.Helper()

	m := ingestMocks{
		queueEnablementController: queuestatemock.NewMockTransitionController(ctrl),
		counter:                   countermock.NewMockCounter(ctrl),
		factory:                   scmock.NewMockFactory(ctrl),
		sc:                        scmock.NewMockSourceControl(ctrl),
		reqStore:                  storagemock.NewMockRequestStore(ctrl),
		uriStore:                  storagemock.NewMockRequestURIStore(ctrl),
		queueStore:                storagemock.NewMockQueueStore(ctrl),
		materializer:              requestlogmock.NewMockMaterializer(ctrl),
		publisher:                 mqmock.NewMockPublisher(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	m.store = store
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetRequestURIStore().Return(m.uriStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyProcess, Name: "process", Queue: queue},
	})
	require.NoError(t, err)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), staticCounterFactory{counter: m.counter}, m.factory, staticStorageFactory{store: store}, m.materializer, registry, []string{testQueue})
	c.queueEnablementController = m.queueEnablementController
	return c, m
}

func acceptedRequest(id string) entity.Request {
	return entity.Request{
		ID:      id,
		Queue:   testQueue,
		URI:     testURI,
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
}

func expectMaterializeAccepted(m ingestMocks, id string) {
	m.materializer.EXPECT().PersistLog(
		gomock.Any(),
		m.store,
		requestlog.NewRequestStateLog(acceptedRequest(id), entity.RequestOutcomeReasonUnknown),
	).Return(nil)
}

func expectHeadAndEnablement(m ingestMocks) {
	m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
	m.sc.EXPECT().Latest(gomock.Any()).Return(testURI, nil)
	m.queueEnablementController.EXPECT().Initialize(gomock.Any(), testQueue).Return(entity.QueueState{Queue: testQueue, State: entity.QueueEnablementDisabled, Version: 1}, nil)
}

// expectAdvanceLatestRequestID wires Get + Create + Update for queue.latest_request_id.
func expectAdvanceLatestRequestID(m ingestMocks, queue, id string) {
	m.queueStore.EXPECT().Get(gomock.Any(), queue).Return(entity.Queue{}, storage.ErrNotFound)
	m.queueStore.EXPECT().Create(gomock.Any(), entity.Queue{Name: queue, Version: 1}).Return(nil)
	updated := entity.Queue{Name: queue, LatestRequestID: id, Version: 1}
	m.queueStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
}

// expectAdvanceLatestRequestIDNoOp wires Get when latest_request_id is already at id.
func expectAdvanceLatestRequestIDNoOp(m ingestMocks, queue, id string) {
	m.queueStore.EXPECT().Get(gomock.Any(), queue).Return(entity.Queue{
		Name:            queue,
		LatestRequestID: id,
		Version:         1,
	}, nil)
}

func TestPublishProcessCarriesQueueMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newIngestController(t, ctrl)

	var got entityqueue.Message
	m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			got = msg
			return nil
		})

	require.NoError(t, c.publishProcess(context.Background(), "7", testQueue))
	assert.Equal(t, testQueue, got.PartitionKey)
	assert.Equal(t, testQueue, got.Tenant)
	assert.Equal(t, testQueue, got.Metadata[entityqueue.MetadataKeyQueueName])
}

func TestIngestController_Ingest(t *testing.T) {
	tests := []struct {
		name        string
		queue       string
		setup       func(m ingestMocks)
		wantID      string
		wantErr     bool
		wantInvalid bool
	}{
		{
			name:  "happy path mints persists and publishes",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), counterDomainRequest).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testURI, "7").Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				expectMaterializeAccepted(m, "7")
				expectAdvanceLatestRequestID(m, testQueue, "7")
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "7",
		},
		{
			name:  "legacy latest pointer prevents advancement and publish",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), counterDomainRequest).Return(int64(1), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testURI, "1").Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "1").Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), acceptedRequest("1")).Return(nil)
				expectMaterializeAccepted(m, "1")
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: "request/" + testQueue + "/42", Version: 1,
				}, nil)
			},
			wantErr: true,
		},
		{
			name:  "retry rejects legacy latest pointer",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("1", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "1").Return(acceptedRequest("1"), nil)
				expectMaterializeAccepted(m, "1")
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: "request/" + testQueue + "/42", Version: 1,
				}, nil)
			},
			wantErr: true,
		},
		{
			name:  "dedup with existing accepted request republishes without minting",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "3").Return(acceptedRequest("3"), nil)
				expectMaterializeAccepted(m, "3")
				expectAdvanceLatestRequestIDNoOp(m, testQueue, "3")
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "3",
		},
		{
			name:  "heals when uri mapped but request missing",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "3").Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				expectMaterializeAccepted(m, "3")
				expectAdvanceLatestRequestID(m, testQueue, "3")
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "3",
		},
		{
			name:  "dedup race returns winner id and completes",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), counterDomainRequest).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testURI, "7").Return(storage.ErrAlreadyExists)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "3").Return(acceptedRequest("3"), nil)
				expectMaterializeAccepted(m, "3")
				expectAdvanceLatestRequestIDNoOp(m, testQueue, "3")
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "3",
		},
		{
			name:        "empty queue is invalid",
			queue:       "",
			setup:       func(m ingestMocks) {},
			wantErr:     true,
			wantInvalid: true,
		},
		{
			name:        "unconfigured queue is invalid",
			queue:       "monorepo/unconfigured",
			setup:       func(m ingestMocks) {},
			wantErr:     true,
			wantInvalid: true,
		},
		{
			name:  "unknown queue head is invalid",
			queue: testQueue,
			setup: func(m ingestMocks) {
				m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
				m.sc.EXPECT().Latest(gomock.Any()).Return("", sourcecontrol.ErrNotFound)
			},
			wantErr:     true,
			wantInvalid: true,
		},
		{
			name:  "source control infra error is not invalid",
			queue: testQueue,
			setup: func(m ingestMocks) {
				m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
				m.sc.EXPECT().Latest(gomock.Any()).Return("", errors.New("sc unavailable"))
			},
			wantErr: true,
		},
		{
			name:  "counter error",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), errors.New("counter unavailable"))
			},
			wantErr: true,
		},
		{
			name:  "non-positive counter value",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), nil)
			},
			wantErr: true,
		},
		{
			name:  "request store create error",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testURI, gomock.Any()).Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("db down"))
			},
			wantErr: true,
		},
		{
			name:  "publish error",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testURI, gomock.Any()).Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				expectMaterializeAccepted(m, "7")
				expectAdvanceLatestRequestID(m, testQueue, "7")
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(errors.New("queue down"))
			},
			wantErr: true,
		},
		{
			name:  "request log error prevents advancement and publish",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "3").Return(acceptedRequest("3"), nil)
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestStateLog(acceptedRequest("3"), entity.RequestOutcomeReasonUnknown),
				).Return(errors.New("log unavailable"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newIngestController(t, ctrl)
			tt.setup(m)

			result, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: tt.queue})

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantInvalid, IsInvalidRequest(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantID, result.ID)
		})
	}
}

const nextIngestURI = "git://repo/monorepo/main/def456"

type ingestEnablementFixture struct {
	controller         *IngestController
	mocks              ingestMocks
	queue              entity.Queue
	summary            entity.QueueSummary
	logs               map[string]entity.QueueLog
	head               string
	beforeUpdate       func()
	logErr, summaryErr error
}

func newIngestEnablementFixture(t *testing.T) *ingestEnablementFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	c, m := newIngestController(t, ctrl)
	f := &ingestEnablementFixture{controller: c, mocks: m, head: testURI,
		queue:   entity.Queue{Name: testQueue, Version: 1, Enablement: entity.QueueEnablementDisabled, EnablementVersion: 1, EnablementChangedAtMs: 1000},
		summary: entity.QueueSummary{Queue: testQueue, State: entity.QueueEnablementDisabled, QueueStateVersion: 1, StateTimestampMs: 1000, Version: 1},
		logs:    make(map[string]entity.QueueLog),
	}
	f.logs[entity.QueueLogID(1)] = queuelog.NewQueueStateLog(f.queue)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
		if f.queue.Name == "" {
			return entity.Queue{}, storage.ErrNotFound
		}
		return f.queue, nil
	}).AnyTimes()
	m.queueStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, row entity.Queue) error {
		if f.queue.Name != "" {
			return storage.ErrAlreadyExists
		}
		f.queue = row
		return nil
	}).AnyTimes()
	m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, row entity.Queue, oldVersion, newVersion int32) error {
		if hook := f.beforeUpdate; hook != nil {
			f.beforeUpdate = nil
			hook()
		}
		if f.queue.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		row.Version = newVersion
		f.queue = row
		return nil
	}).AnyTimes()
	logs := storagemock.NewMockQueueLogStore(ctrl)
	m.store.EXPECT().GetQueueLogStore().Return(logs).AnyTimes()
	logs.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.QueueLog, error) {
		log, ok := f.logs[id]
		if !ok {
			return entity.QueueLog{}, storage.ErrNotFound
		}
		return log, nil
	}).AnyTimes()
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.QueueLog) error {
		if f.logErr != nil {
			return f.logErr
		}
		if _, ok := f.logs[log.ID]; ok {
			return storage.ErrAlreadyExists
		}
		f.logs[log.ID] = log
		return nil
	}).AnyTimes()
	summaries := storagemock.NewMockQueueSummaryStore(ctrl)
	m.store.EXPECT().GetQueueSummaryStore().Return(summaries).AnyTimes()
	summaries.EXPECT().Get(gomock.Any()).DoAndReturn(func(context.Context) (entity.QueueSummary, error) {
		if f.summary.Version == 0 {
			return entity.QueueSummary{}, storage.ErrNotFound
		}
		return f.summary, nil
	}).AnyTimes()
	summaries.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.QueueSummary) error {
		if f.summaryErr != nil {
			return f.summaryErr
		}
		if f.summary.Version != 0 {
			return storage.ErrAlreadyExists
		}
		f.summary = summary
		return nil
	}).AnyTimes()
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.QueueSummary, oldVersion, newVersion int64) error {
		if f.summaryErr != nil {
			return f.summaryErr
		}
		if f.summary.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		summary.Version = newVersion
		f.summary = summary
		return nil
	}).AnyTimes()
	m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil).AnyTimes()
	m.sc.EXPECT().Latest(gomock.Any()).DoAndReturn(func(context.Context) (string, error) { return f.head, nil }).AnyTimes()
	history := scfake.New(sourcecontrol.Config{QueueName: testQueue}, []string{nextIngestURI, testURI})
	m.sc.EXPECT().IsAncestor(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(history.IsAncestor).AnyTimes()
	f.controller = NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, staticCounterFactory{counter: m.counter}, m.factory, staticStorageFactory{store: m.store}, m.materializer, c.registry, []string{testQueue})
	return f
}

func TestIngestEnablementDoesNotPauseWork(t *testing.T) {
	for _, tc := range []struct {
		name     string
		explicit bool
		state    entity.QueueEnablement
		repeated bool
	}{
		{"ordinary disabled", false, entity.QueueEnablementDisabled, false},
		{"ordinary enabled", false, entity.QueueEnablementEnabled, false},
		{"explicit disable", true, entity.QueueEnablementDisabled, false},
		{"explicit enable", true, entity.QueueEnablementEnabled, false},
		{"repeated disable", true, entity.QueueEnablementDisabled, true},
		{"repeated enable", true, entity.QueueEnablementEnabled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIngestEnablementFixture(t)
			c, m := f.controller, f.mocks
			if (!tc.explicit && tc.state == entity.QueueEnablementEnabled) || (tc.explicit && tc.state == entity.QueueEnablementDisabled) {
				_, err := c.applyEnablementDecision(context.Background(), testQueue, entity.QueueEnablementEnabled, testURI)
				require.NoError(t, err)
			}
			if tc.repeated {
				_, err := c.applyEnablementDecision(context.Background(), testQueue, tc.state, testURI)
				require.NoError(t, err)
				f.head = nextIngestURI
			} else if tc.explicit {
				// The head advances during application; the request must retain the captured boundary.
				f.beforeUpdate = func() { f.head = nextIngestURI }
			}
			uri := f.head
			m.uriStore.EXPECT().GetIDByURI(gomock.Any(), uri).Return("", storage.ErrNotFound)
			m.counter.EXPECT().Next(gomock.Any(), counterDomainRequest).Return(int64(7), nil)
			m.uriStore.EXPECT().Create(gomock.Any(), uri, "7").Return(nil)
			m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(entity.Request{}, storage.ErrNotFound)
			request := acceptedRequest("7")
			request.URI = uri
			m.reqStore.EXPECT().Create(gomock.Any(), request).Return(nil)
			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)).Return(nil)
			m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)

			result, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: testQueue, HasEnablement: tc.explicit, Enablement: tc.state})
			require.NoError(t, err)
			require.Equal(t, "7", result.ID)
			require.Equal(t, tc.state, result.Enablement.State)
			require.Equal(t, f.queue.EnablementState(), result.Enablement)
			require.Equal(t, result.Enablement, f.summary.Snapshot())
			require.Equal(t, "7", f.queue.LatestRequestID)
			if tc.explicit {
				require.Equal(t, nextIngestURI, f.head)
				require.Equal(t, testURI, result.Enablement.EffectiveFromCommitURI)
			}
		})
	}
}

func TestIngestEnablementFailure(t *testing.T) {
	for _, tc := range []struct {
		name            string
		explicit        bool
		invalid         bool
		initializeFails bool
		err             error
	}{
		{"invalid decision", true, true, false, queuestate.ErrInvalidChange},
		{"transition storage unavailable", true, false, false, errors.New("unavailable")},
		{"explicit initialization unavailable", true, false, true, errors.New("unavailable")},
		{"observed state unavailable", false, false, true, errors.New("unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, m := newIngestController(t, gomock.NewController(t))
			m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
			m.sc.EXPECT().Latest(gomock.Any()).Return(testURI, nil)
			desired := entity.QueueEnablementEnabled
			if tc.invalid {
				desired = entity.QueueEnablementUnknown
			} else if tc.initializeFails {
				m.queueEnablementController.EXPECT().Initialize(gomock.Any(), testQueue).Return(entity.QueueState{}, tc.err)
			} else {
				m.queueEnablementController.EXPECT().Initialize(gomock.Any(), testQueue).Return(entity.QueueState{Queue: testQueue, State: entity.QueueEnablementDisabled, Version: 1}, nil)
				m.queueEnablementController.EXPECT().Prepare(gomock.Any(), testQueue, desired, testURI).Return(queuestate.PreparedChange{}, tc.err)
			}
			_, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: testQueue, HasEnablement: tc.explicit, Enablement: desired})
			require.ErrorIs(t, err, tc.err)
		})
	}
}

func TestIngestInitializesQueueEnablement(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, decision := range []struct {
			name     string
			explicit bool
			state    entity.QueueEnablement
			version  int64
		}{
			{"ordinary", false, entity.QueueEnablementDisabled, 1},
			{"enable", true, entity.QueueEnablementEnabled, 2},
			{"disable", true, entity.QueueEnablementDisabled, 1},
		} {
			name := "new/" + decision.name
			if existing {
				name = "existing empty/" + decision.name
			}
			t.Run(name, func(t *testing.T) {
				f := newIngestEnablementFixture(t)
				f.queue = entity.Queue{}
				if existing {
					f.queue = entity.Queue{Name: testQueue, Version: 5, InFlightCount: 3, LastGreenRequestID: "4"}
				}
				f.summary = entity.QueueSummary{}
				clear(f.logs)
				m := f.mocks
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("7", nil).Times(2)
				m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(acceptedRequest("7"), nil).Times(2)
				m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, requestlog.NewRequestStateLog(acceptedRequest("7"), entity.RequestOutcomeReasonUnknown)).Return(nil).Times(2)
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil).Times(2)
				request := entity.IngestRequest{Queue: testQueue, HasEnablement: decision.explicit, Enablement: decision.state}
				result, err := f.controller.Ingest(context.Background(), request)
				require.NoError(t, err)
				require.Equal(t, decision.state, result.Enablement.State)
				require.Equal(t, decision.version, result.Enablement.Version)
				require.Positive(t, result.Enablement.ChangedAtMs)
				require.Equal(t, result.Enablement, f.summary.Snapshot())
				require.Len(t, f.logs, int(decision.version))
				require.Equal(t, "7", f.queue.LatestRequestID)
				if existing {
					require.Equal(t, int32(3), f.queue.InFlightCount)
					require.Equal(t, "4", f.queue.LastGreenRequestID)
				}
				retry, err := f.controller.Ingest(context.Background(), request)
				require.NoError(t, err)
				require.Equal(t, result, retry)
			})
		}
	}
}

func TestIngestRetriesQueueInitialization(t *testing.T) {
	for _, stage := range []string{"history", "summary"} {
		t.Run(stage, func(t *testing.T) {
			f := newIngestEnablementFixture(t)
			f.queue, f.summary = entity.Queue{}, entity.QueueSummary{}
			clear(f.logs)
			if stage == "history" {
				f.logErr = assert.AnError
			} else {
				f.summaryErr = assert.AnError
			}
			request := entity.IngestRequest{Queue: testQueue, HasEnablement: true, Enablement: entity.QueueEnablementEnabled}
			_, err := f.controller.Ingest(context.Background(), request)
			require.ErrorIs(t, err, assert.AnError)
			initial := f.queue.EnablementState()
			require.Equal(t, int64(1), initial.Version)
			f.logErr, f.summaryErr = nil, nil
			m := f.mocks
			m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("7", nil)
			m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(acceptedRequest("7"), nil)
			expectMaterializeAccepted(m, "7")
			m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			result, err := f.controller.Ingest(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, int64(2), result.Enablement.Version)
			require.Equal(t, entity.QueueEnablementEnabled, result.Enablement.State)
			require.Equal(t, initial, f.logs[entity.QueueLogID(1)].Snapshot())
			require.Equal(t, result.Enablement, f.summary.Snapshot())
		})
	}
}

func TestIngestEnablementDecisionsPreserveBoundaries(t *testing.T) {
	ctx := context.Background()
	f := newIngestEnablementFixture(t)
	initial := f.queue.EnablementState()
	disabled, err := f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementDisabled, testURI)
	require.NoError(t, err)
	require.Equal(t, initial, disabled)
	enabled, err := f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementEnabled, testURI)
	require.NoError(t, err)
	require.Equal(t, entity.QueueEnablementEnabled, enabled.State)
	require.Equal(t, int64(2), enabled.Version)
	require.Equal(t, testURI, enabled.EffectiveFromCommitURI)
	f.head = nextIngestURI
	unchanged, err := f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementEnabled, nextIngestURI)
	require.NoError(t, err)
	require.Equal(t, enabled, unchanged)
	disabled, err = f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementDisabled, nextIngestURI)
	require.NoError(t, err)
	require.Equal(t, entity.QueueEnablementDisabled, disabled.State)
	require.Equal(t, nextIngestURI, disabled.EffectiveFromCommitURI)
	require.Equal(t, int64(3), disabled.Version)
	_, err = f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementUnknown, nextIngestURI)
	require.ErrorIs(t, err, queuestate.ErrInvalidChange)
}

func TestIngestEnablementRetriesCompetingChanges(t *testing.T) {
	for _, tc := range []struct {
		name              string
		capturedURI       string
		competingURI      string
		wantInvalidChange bool
	}{
		{"competing boundary before captured head", nextIngestURI, testURI, false},
		{"competing boundary after captured head", testURI, nextIngestURI, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIngestEnablementFixture(t)
			f.head = nextIngestURI
			f.beforeUpdate = func() {
				for _, desired := range []entity.QueueEnablement{entity.QueueEnablementEnabled, entity.QueueEnablementDisabled} {
					_, err := f.controller.applyEnablementDecision(context.Background(), testQueue, desired, tc.competingURI)
					require.NoError(t, err)
				}
			}
			applied, err := f.controller.applyEnablementDecision(context.Background(), testQueue, entity.QueueEnablementEnabled, tc.capturedURI)
			if tc.wantInvalidChange {
				require.ErrorIs(t, err, queuestate.ErrInvalidChange)
				require.Equal(t, int64(3), f.queue.EnablementVersion)
				require.Equal(t, entity.QueueEnablementDisabled, f.queue.Enablement)
				require.Equal(t, nextIngestURI, f.queue.EffectiveFromCommitURI)
				return
			}
			require.NoError(t, err)
			require.Equal(t, entity.QueueEnablementEnabled, applied.State)
			require.Equal(t, int64(4), applied.Version)
			require.Equal(t, tc.capturedURI, applied.EffectiveFromCommitURI)
			require.Equal(t, applied, f.summary.Snapshot())
		})
	}
}

func TestIngestEnablementRepairsAnAppliedTransition(t *testing.T) {
	for _, stage := range []string{"history", "summary"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			f := newIngestEnablementFixture(t)
			failure := errors.New("unavailable")
			f.beforeUpdate = func() {
				if stage == "history" {
					f.logErr = failure
				} else {
					f.summaryErr = failure
				}
			}
			_, err := f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementEnabled, testURI)
			require.ErrorIs(t, err, failure)
			require.Equal(t, int64(2), f.queue.EnablementVersion)
			f.logErr, f.summaryErr = nil, nil
			f.head = nextIngestURI
			applied, err := f.controller.applyEnablementDecision(ctx, testQueue, entity.QueueEnablementEnabled, nextIngestURI)
			require.NoError(t, err)
			require.Equal(t, int64(2), applied.Version)
			require.Equal(t, testURI, applied.EffectiveFromCommitURI)
			require.Equal(t, applied, f.summary.Snapshot())
		})
	}
}
