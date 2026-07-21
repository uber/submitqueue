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

package conclude

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// newTestController creates a controller with test dependencies.
// expectLogPublish controls whether the log topic publisher is wired with an
// expectation; tests that don't reach the log publish step pass false so an
// unexpected publish would fail the test.
func newTestController(t *testing.T, ctrl *gomock.Controller, mockStorage *storagemock.MockStorage, expectLogPublish bool) (*Controller, *queuemock.MockPublisher) {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockRequestStore := storagemock.NewMockRequestStore(ctrl)
		mockBatchStore := storagemock.NewMockBatchStore(ctrl)
		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
		mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	}

	mockPub := queuemock.NewMockPublisher(ctrl)
	if expectLogPublish {
		mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	}
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	return NewController(logger, scope, mockStorage, registry, topickey.TopicKeyConclude, "orchestrator-conclude"), mockPub
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, _ := newTestController(t, ctrl, nil, false)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyConclude, controller.TopicKey())
	assert.Equal(t, "orchestrator-conclude", controller.ConsumerGroup())
	assert.Equal(t, "conclude", controller.Name())
}

func TestController_Process(t *testing.T) {
	tests := []struct {
		name             string
		batch            entity.Batch
		setupStore       func(*gomock.Controller) *storagemock.MockStorage
		expectLogPublish bool
		wantErr          bool
		retryable        bool
	}{
		{
			name: "succeeded batch lands requests",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1", "test-queue/2"},
				State:    entity.BatchStateSucceeded,
				Version:  3,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{
					ID:       "test-queue/batch/1",
					Queue:    "test-queue",
					Contains: []string{"test-queue/1", "test-queue/2"},
					State:    entity.BatchStateSucceeded,
					Version:  3,
				}, nil)

				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID: "test-queue/1", Version: 2, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/1", int32(2), int32(3), entity.RequestStateLanded).Return(nil)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/2").Return(entity.Request{
					ID: "test-queue/2", Version: 3, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/2", int32(3), int32(4), entity.RequestStateLanded).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			expectLogPublish: true,
		},
		{
			name: "failed batch errors requests",
			batch: entity.Batch{
				ID:       "test-queue/batch/2",
				Queue:    "test-queue",
				Contains: []string{"test-queue/5"},
				State:    entity.BatchStateFailed,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.Batch{
					ID:       "test-queue/batch/2",
					Queue:    "test-queue",
					Contains: []string{"test-queue/5"},
					State:    entity.BatchStateFailed,
					Version:  2,
				}, nil)

				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/5").Return(entity.Request{
					ID: "test-queue/5", Version: 1, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/5", int32(1), int32(2), entity.RequestStateError).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			expectLogPublish: true,
		},
		{
			name: "cancelled batch cancels requests",
			batch: entity.Batch{
				ID:       "test-queue/batch/3",
				Queue:    "test-queue",
				Contains: []string{"test-queue/10"},
				State:    entity.BatchStateCancelled,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/3").Return(entity.Batch{
					ID:       "test-queue/batch/3",
					Queue:    "test-queue",
					Contains: []string{"test-queue/10"},
					State:    entity.BatchStateCancelled,
					Version:  2,
				}, nil)

				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/10").Return(entity.Request{
					ID: "test-queue/10", Version: 4, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/10", int32(4), int32(5), entity.RequestStateCancelled).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			expectLogPublish: true,
		},
		{
			name: "idempotent retry: request already in target terminal state still publishes log",
			batch: entity.Batch{
				ID:       "test-queue/batch/8",
				Queue:    "test-queue",
				Contains: []string{"test-queue/20"},
				State:    entity.BatchStateSucceeded,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/8").Return(entity.Batch{
					ID:       "test-queue/batch/8",
					Queue:    "test-queue",
					Contains: []string{"test-queue/20"},
					State:    entity.BatchStateSucceeded,
					Version:  2,
				}, nil)

				// Request is already Landed (prior delivery wrote it). UpdateState
				// must NOT be called — gomock will fail the test if it is.
				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/20").Return(entity.Request{
					ID: "test-queue/20", Version: 7, State: entity.RequestStateLanded,
				}, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			expectLogPublish: true,
		},
		{
			name: "divergent terminal state skips reconcile and log publish",
			batch: entity.Batch{
				ID:       "test-queue/batch/9",
				Queue:    "test-queue",
				Contains: []string{"test-queue/30"},
				State:    entity.BatchStateSucceeded,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/9").Return(entity.Batch{
					ID:       "test-queue/batch/9",
					Queue:    "test-queue",
					Contains: []string{"test-queue/30"},
					State:    entity.BatchStateSucceeded,
					Version:  2,
				}, nil)

				// Request is already in a *different* terminal state (Cancelled).
				// Conclude must not write the log entry (the other writer owns it),
				// and must not attempt UpdateState.
				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/30").Return(entity.Request{
					ID: "test-queue/30", Version: 5, State: entity.RequestStateCancelled,
				}, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			expectLogPublish: false,
		},
		{
			name: "non-terminal batch state returns error",
			batch: entity.Batch{
				ID:       "test-queue/batch/4",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/4").Return(entity.Batch{
					ID:       "test-queue/batch/4",
					Queue:    "test-queue",
					Contains: []string{"test-queue/1"},
					State:    entity.BatchStateCreated,
					Version:  1,
				}, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				return mockStorage
			},
			wantErr:   true,
			retryable: false,
		},
		{
			name: "request store get failure returns error",
			batch: entity.Batch{
				ID:       "test-queue/batch/5",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateSucceeded,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/5").Return(entity.Batch{
					ID:       "test-queue/batch/5",
					Queue:    "test-queue",
					Contains: []string{"test-queue/1"},
					State:    entity.BatchStateSucceeded,
					Version:  2,
				}, nil)

				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{}, fmt.Errorf("db connection lost"))

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			wantErr:   true,
			retryable: false,
		},
		{
			name: "request store update failure returns error",
			batch: entity.Batch{
				ID:       "test-queue/batch/6",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateSucceeded,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/6").Return(entity.Batch{
					ID:       "test-queue/batch/6",
					Queue:    "test-queue",
					Contains: []string{"test-queue/1"},
					State:    entity.BatchStateSucceeded,
					Version:  2,
				}, nil)

				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID: "test-queue/1", Version: 2, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/1", int32(2), int32(3), entity.RequestStateLanded).Return(errs.ErrVersionMismatch)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			wantErr:   true,
			retryable: false,
		},
		{
			name: "empty contains list succeeds",
			batch: entity.Batch{
				ID:      "test-queue/batch/7",
				Queue:   "test-queue",
				State:   entity.BatchStateSucceeded,
				Version: 1,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/7").Return(entity.Batch{
					ID:      "test-queue/batch/7",
					Queue:   "test-queue",
					State:   entity.BatchStateSucceeded,
					Version: 1,
				}, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
				return mockStorage
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			var mockStorage *storagemock.MockStorage
			if tt.setupStore != nil {
				mockStorage = tt.setupStore(ctrl)
			}

			controller, _ := newTestController(t, ctrl, mockStorage, tt.expectLogPublish)

			msg := entityqueue.NewMessage(tt.batch.ID, batchIDPayload(t, tt.batch.ID), tt.batch.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err := controller.Process(context.Background(), delivery)

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.retryable, errs.IsRetryable(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("db connection lost"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	controller, _ := newTestController(t, ctrl, mockStorage, false)

	msg := entityqueue.NewMessage("test-queue/batch/1", batchIDPayload(t, "test-queue/batch/1"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, _ := newTestController(t, ctrl, nil, false)

	var _ consumer.Controller = controller
}
