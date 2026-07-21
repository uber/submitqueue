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

package dlq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testID    = "request/monorepo/main/7"
)

type dlqMocks struct {
	reqStore   *storagemock.MockRequestStore
	queueStore *storagemock.MockQueueStore
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, dlqMocks) {
	t.Helper()

	m := dlqMocks{
		reqStore:   storagemock.NewMockRequestStore(ctrl),
		queueStore: storagemock.NewMockQueueStore(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	c := NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), store, TopicKey(stovepipemq.TopicKeyProcess), "stovepipe-process-dlq")
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, testQueue, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(4).AnyTimes()
	d.EXPECT().Metadata().Return(map[string]string{
		"dlq.original_topic": "process",
		"dlq.failure_count":  "3",
		"dlq.last_error":     "boom",
	}).AnyTimes()
	return d
}

func processPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id})
	require.NoError(t, err)
	return b
}

func requestWithState(state entity.RequestState) entity.Request {
	return entity.Request{
		ID:      testID,
		Queue:   testQueue,
		State:   state,
		Version: 2,
	}
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		setup   func(m dlqMocks)
		wantErr bool
	}{
		{
			name: "accepted request is marked failed",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateAccepted), nil)
				updated := requestWithState(entity.RequestStateAccepted)
				updated.State = entity.RequestStateRecordedNotGreen
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
			},
		},
		{
			name: "processing request releases the queue slot before marking failed",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, InFlightCount: 1, Version: 5,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, InFlightCount: 0, Version: 5,
				}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateRecordedNotGreen
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
			},
		},
		{
			name: "already superseded is a no-op",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSuperseded), nil)
			},
		},
		{
			name: "already failed is a no-op",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateRecordedNotGreen), nil)
			},
		},
		{
			name: "request not found is a no-op",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, errs.ErrNotFound)
			},
		},
		{
			name: "request update retries on version mismatch",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateAccepted), nil)
				updated := requestWithState(entity.RequestStateAccepted)
				updated.State = entity.RequestStateRecordedNotGreen
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(errs.ErrVersionMismatch)
			},
			wantErr: true,
		},
		{
			name: "queue update retries on version mismatch then succeeds",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, InFlightCount: 1, Version: 5,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, InFlightCount: 0, Version: 5,
				}, int32(5), int32(6)).Return(errs.ErrVersionMismatch)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, InFlightCount: 1, Version: 6,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, InFlightCount: 0, Version: 6,
				}, int32(6), int32(7)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateRecordedNotGreen
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
			},
		},
		{
			name: "queue already drained is a no-op for slot release",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, InFlightCount: 0, Version: 5,
				}, nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateRecordedNotGreen
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
			},
		},
		{
			name:    "malformed payload is not retryable",
			payload: []byte("not-json"),
			setup:   func(m dlqMocks) {},
			wantErr: true,
		},
		{
			name:    "empty request id is not retryable",
			payload: processPayload(t, ""),
			setup:   func(m dlqMocks) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			tt.setup(m)

			payload := tt.payload
			if payload == nil {
				payload = processPayload(t, testID)
			}

			err := c.Process(context.Background(), delivery(t, ctrl, payload))

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestTopicKey(t *testing.T) {
	assert.Equal(t, consumer.TopicKey("process_dlq"), TopicKey(stovepipemq.TopicKeyProcess))
}
