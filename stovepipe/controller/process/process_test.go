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

package process

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	queueconfigdefault "github.com/uber/submitqueue/stovepipe/extension/queueconfig/default"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testID    = "request/monorepo/main/7"
	testURI   = "git://repo/monorepo/main/abc123"
)

type processMocks struct {
	reqStore   *storagemock.MockRequestStore
	queueStore *storagemock.MockQueueStore
	publisher  *mqmock.MockPublisher
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, processMocks) {
	t.Helper()

	m := processMocks{
		reqStore:   storagemock.NewMockRequestStore(ctrl),
		queueStore: storagemock.NewMockQueueStore(ctrl),
		publisher:  mqmock.NewMockPublisher(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyProcess, Name: "process", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), store, queueconfigdefault.NewStore(), registry, stovepipemq.TopicKeyProcess, "stovepipe-process")
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := mqmock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, testQueue, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func processPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id})
	require.NoError(t, err)
	return b
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

func expectAdmit(m processMocks, id string) {
	updatedQueue := entity.Queue{
		Name:            testQueue,
		LatestRequestID: id,
		InFlightCount:   1,
		Version:         1,
	}
	m.queueStore.EXPECT().Update(gomock.Any(), updatedQueue, int32(1), int32(2)).Return(nil)

	updatedReq := acceptedRequest(id)
	updatedReq.State = entity.RequestStateProcessing
	updatedReq.BuildStrategy = entity.BuildStrategyFull
	m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(nil)
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		setup     func(m processMocks)
		wantErr   bool
		wantRetry bool
	}{
		{
			name: "superseded is no-op",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}, nil)
			},
		},
		{
			name: "processing is no-op until build publish lands",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
				}, nil)
			},
		},
		{
			name: "latest accepted head is admitted",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				expectAdmit(m, testID)
			},
		},
		{
			name: "accepted with empty latest pointer awaits ingest stamp",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:    testQueue,
					Version: 1,
				}, nil)
			},
		},
		{
			name: "latest accepted head reschedules when gate closed",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					Version:         1,
				}, nil)
				m.publisher.EXPECT().
					PublishAfter(gomock.Any(), "process", gomock.Any(), int64(5000)).
					Return(nil)
			},
		},
		{
			name:      "gate reschedule publish error surfaces",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					Version:         1,
				}, nil)
				m.publisher.EXPECT().
					PublishAfter(gomock.Any(), "process", gomock.Any(), int64(5000)).
					Return(errors.New("queue down"))
			},
		},
		{
			name: "older accepted head is superseded",
			id:   "request/monorepo/main/3",
			setup: func(m processMocks) {
				olderID := "request/monorepo/main/3"
				m.reqStore.EXPECT().Get(gomock.Any(), olderID).Return(acceptedRequest(olderID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				updated := acceptedRequest(olderID)
				updated.State = entity.RequestStateSuperseded
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
			},
		},
		{
			name: "supersede retries on version mismatch",
			id:   "request/monorepo/main/3",
			setup: func(m processMocks) {
				olderID := "request/monorepo/main/3"
				m.reqStore.EXPECT().Get(gomock.Any(), olderID).Return(acceptedRequest(olderID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				updated := acceptedRequest(olderID)
				updated.State = entity.RequestStateSuperseded
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				m.reqStore.EXPECT().Get(gomock.Any(), olderID).Return(entity.Request{
					ID: olderID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}, nil)
			},
		},
		{
			name:      "request not found is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)
			},
		},
		{
			name:      "queue not found is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{}, storage.ErrNotFound)
			},
		},
		{
			name:      "request storage error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, errors.New("db down"))
			},
		},
		{
			name:      "malformed payload is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup:     func(m processMocks) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			if tt.setup != nil {
				tt.setup(m)
			}

			id := testID
			if tt.id != "" {
				id = tt.id
			}
			payload := processPayload(t, id)
			if tt.name == "malformed payload is not retryable" {
				payload = []byte("not-json")
			}

			err := c.Process(context.Background(), delivery(t, ctrl, payload))

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
				return
			}
			require.NoError(t, err)
		})
	}
}
