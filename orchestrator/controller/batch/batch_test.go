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

package batch

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	countermock "github.com/uber/submitqueue/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// requestIDPayload serializes a RequestID to JSON bytes for test message payloads.
func requestIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.RequestID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// newSequentialCounter returns a mock counter that returns incrementing values starting at 1.
func newSequentialCounter(ctrl *gomock.Controller) *countermock.MockCounter {
	var seq int64
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, domain string) (int64, error) {
			return atomic.AddInt64(&seq, 1), nil
		},
	).AnyTimes()
	return cnt
}

// testRequest returns a standard test request for batch tests.
func testRequest() entity.Request {
	return entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
}

// newTestController creates a controller with test dependencies.
// If mockStorage is nil, a default MockStorage with an empty batch store is created.
func newTestController(t *testing.T, ctrl *gomock.Controller, cnt *countermock.MockCounter, mockStorage *storagemock.MockStorage, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockBatchStore := storagemock.NewMockBatchStore(ctrl)
		mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockReqStore := storagemock.NewMockRequestStore(ctrl)
		req := testRequest()
		mockReqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil).AnyTimes()

		mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
		mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
		mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
		mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	}

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyScore, Name: "score", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, registry, cnt, mockStorage, consumer.TopicKeyBatch, "orchestrator-batch")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyBatch, controller.TopicKey())
	assert.Equal(t, "orchestrator-batch", controller.ConsumerGroup())
	assert.Equal(t, "batch", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	request := testRequest()
	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(entity.Request{}, fmt.Errorf("db connection lost"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

	msg := queue.NewMessage("test-queue/123", requestIDPayload(t, "test-queue/123"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, fmt.Errorf("publish failed"))

	request := testRequest()
	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_CounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := newTestController(t, ctrl, cnt, nil, nil)

	request := testRequest()
	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_WithDependencies(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := entity.Request{
		ID:           "test-queue/456",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/789/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}

	// Set up storage with active batches to become dependencies.
	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateCreated, Version: 1},
		{ID: "test-queue/batch/2", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 2},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(activeBatches, nil)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	// batch/1 has no existing dependents.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{
		BatchID: "test-queue/batch/1",
		Version: 1,
	}, nil)
	mockBatchDependentStore.EXPECT().UpdateDependents(gomock.Any(), "test-queue/batch/1", int32(1), int32(2), gomock.Any()).Return(nil)
	// batch/2 already has an existing dependent.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.BatchDependent{
		BatchID:    "test-queue/batch/2",
		Dependents: []string{"test-queue/batch/99"},
		Version:    2,
	}, nil)
	mockBatchDependentStore.EXPECT().UpdateDependents(gomock.Any(), "test-queue/batch/2", int32(2), int32(3), gomock.Any()).Return(nil)
	// Create empty reverse index for the new batch.
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	var _ consumer.Controller = controller
}
