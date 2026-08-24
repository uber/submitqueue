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
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testID    = "request/monorepo/main/7"
)

func queueContext() context.Context {
	ctx := entityqueue.WithQueueName(context.Background(), testQueue)
	return metrics.WithContextTags(ctx, metrics.NewTag("queue", testQueue))
}

type dlqMocks struct {
	reqStore     *storagemock.MockRequestStore
	queueStore   *storagemock.MockQueueStore
	metricsScope tally.TestScope
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func newController(t *testing.T, ctrl *gomock.Controller) (consumer.Controller, dlqMocks) {
	t.Helper()

	scope := tally.NewTestScope("test", nil)
	m := dlqMocks{
		reqStore:     storagemock.NewMockRequestStore(ctrl),
		queueStore:   storagemock.NewMockQueueStore(ctrl),
		metricsScope: scope,
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	c := NewDLQRequestController(zap.NewNop().Sugar(), scope, staticStorageFactory{store: store}, TopicKey(stovepipemq.TopicKeyProcess), "stovepipe-process-dlq")
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
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
	b, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id, QueueName: testQueue})
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
		name       string
		payload    []byte
		setup      func(m dlqMocks)
		wantErr    bool
		wantMetric string
	}{
		{
			// No queue expectations: an accepted request never claimed a slot,
			// so releasing one here would over-admit against MaxConcurrent.
			name: "accepted request is marked failed without releasing a slot",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateAccepted), nil)
				updated := requestWithState(entity.RequestStateAccepted)
				updated.State = entity.RequestStateFailed
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
				updated.State = entity.RequestStateFailed
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
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateFailed), nil)
			},
		},
		{
			name: "request not found is a no-op",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)
			},
		},
		{
			name: "request update retries on version mismatch",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateAccepted), nil)
				updated := requestWithState(entity.RequestStateAccepted)
				updated.State = entity.RequestStateFailed
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(storage.ErrVersionMismatch)
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
				}, int32(5), int32(6)).Return(storage.ErrVersionMismatch)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, InFlightCount: 1, Version: 6,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, InFlightCount: 0, Version: 6,
				}, int32(6), int32(7)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
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
				updated.State = entity.RequestStateFailed
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
			name:       "empty request id is not retryable",
			payload:    processPayload(t, ""),
			setup:      func(m dlqMocks) {},
			wantErr:    true,
			wantMetric: "test.process_dlq_controller.process_dlq.empty_id_errors+queue=monorepo/main",
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

			err := c.Process(queueContext(), delivery(t, ctrl, payload))
			if tt.wantMetric != "" {
				counter, ok := m.metricsScope.Snapshot().Counters()[tt.wantMetric]
				require.True(t, ok)
				assert.EqualValues(t, 1, counter.Value())
			}

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
