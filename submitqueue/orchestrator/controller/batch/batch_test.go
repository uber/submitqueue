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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/extension/counter"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	conflictmock "github.com/uber/submitqueue/submitqueue/extension/conflict/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func batchWithState(batch entity.Batch, state entity.BatchState) entity.Batch {
	batch.State = state
	return batch
}

func requestWithState(request entity.Request, state entity.RequestState) entity.Request {
	request.State = state
	return request
}

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

// newQueueBatchStateStore returns a QueueBatchStateStore mock that accepts any
// record write and lists the given batches as membership records under their
// current state. Callers hydrating candidates must set up the corresponding
// BatchStore.Get expectations themselves.
func newQueueBatchStateStore(ctrl *gomock.Controller, active ...entity.Batch) *storagemock.MockQueueBatchStateStore {
	s := storagemock.NewMockQueueBatchStateStore(ctrl)
	s.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, state entity.BatchState) ([]entity.QueueBatchState, error) {
			var records []entity.QueueBatchState
			for _, b := range active {
				if b.State == state {
					records = append(records, entity.QueueBatchState{Queue: b.Queue, State: state, BatchID: b.ID})
				}
			}
			return records, nil
		},
	).AnyTimes()
	return s
}

// storageFactoryFor returns a storage.Factory mock that resolves any queue to
// the given queue-scoped store aggregate.
func storageFactoryFor(ctrl *gomock.Controller, store storage.Storage) *storagemock.MockFactory {
	f := storagemock.NewMockFactory(ctrl)
	f.EXPECT().For(gomock.Any()).Return(store, nil).AnyTimes()
	return f
}

// staticCounterFactory resolves every queue to the same counter, so tests can keep
// setting expectations on one mock regardless of which queue the controller resolves.
type staticCounterFactory struct{ counter counter.Counter }

func (f staticCounterFactory) For(counter.Config) (counter.Counter, error) { return f.counter, nil }

// testRequest returns a standard test request for batch tests.
func testRequest() entity.Request {
	return entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/service/pull/456/abcdef0123456789abcdef0123456789abcdef01"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
}

// newTestController creates a controller with test dependencies.
// If mockStorage is nil, a default MockStorage with an empty batch store is created.
// If analyzer is nil, the "all" conflict analyzer is used (every active batch becomes a dependency).
// speculatePublishErr, if non-nil, is returned only for publishes to the "speculate" topic; the
// log publish (which the controller emits first) always succeeds, so callers exercising the
// speculate publish-failure path are not short-circuited on the earlier log publish.
func newTestController(t *testing.T, ctrl *gomock.Controller, cnt *countermock.MockCounter, mockStorage *storagemock.MockStorage, analyzer conflict.Analyzer, speculatePublishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockBatchStore := storagemock.NewMockBatchStore(ctrl)
		mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		mockBatchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil).AnyTimes()

		mockReqStore := storagemock.NewMockRequestStore(ctrl)
		req := testRequest()
		mockReqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil).AnyTimes()
		mockReqStore.EXPECT().Update(gomock.Any(), requestWithState(req, entity.RequestStateBatched), req.Version, req.Version+1).Return(nil).AnyTimes()

		mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
		mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
		mockRequestBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
		mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
		mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
		mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
		mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	}

	if analyzer == nil {
		analyzer = all.New()
	}

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			if topic == "speculate" {
				return speculatePublishErr
			}
			return nil
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(analyzer, nil).AnyTimes()

	return NewController(logger, scope, registry, staticCounterFactory{counter: cnt}, storageFactoryFor(ctrl, mockStorage), analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyBatch, controller.TopicKey())
	assert.Equal(t, "orchestrator-batch", controller.ConsumerGroup())
	assert.Equal(t, "batch", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil, nil)

	request := testRequest()
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

// TestController_Process_QueueMismatchRejected asserts a payload whose queue
// disagrees with the request's authoritative queue is rejected without
// touching the counter, the batch store, or the publisher.
func TestController_Process_QueueMismatchRejected(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	// Counter with no EXPECTs — must not be called.
	cnt := countermock.NewMockCounter(ctrl)
	controller := newTestController(t, ctrl, cnt, mockStorage, nil, fmt.Errorf("should not publish"))

	payload, err := entity.RequestID{ID: request.ID, Queue: "some-other-queue"}.ToBytes()
	require.NoError(t, err)
	msg := entityqueue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.Error(t, controller.Process(context.Background(), delivery))
}

// TestController_Process_StampsQueueOnSpeculatePayload asserts the batch ID
// published to speculate carries the batch's queue.
func TestController_Process_StampsQueueOnSpeculatePayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	var speculateMsgs []entityqueue.Message
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			if topic == "speculate" {
				speculateMsgs = append(speculateMsgs, msg)
			}
			return nil
		},
	).AnyTimes()
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockBatchStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()
	mockReqStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(all.New(), nil).AnyTimes()
	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, registry, staticCounterFactory{counter: newSequentialCounter(ctrl)},
		storageFactoryFor(ctrl, mockStorage), analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
	require.Len(t, speculateMsgs, 1)
	bid, err := entity.BatchIDFromBytes(speculateMsgs[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, request.Queue, bid.Queue)
	assert.Equal(t, request.Queue, speculateMsgs[0].PartitionKey)
}

// TestController_Process_PublishesBatchedLog asserts the controller emits a
// "batched" request log carrying the request ID, the post-CAS request version,
// and the batch ID it was placed into.
func TestController_Process_PublishesBatchedLog(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	// Capture messages published to the log topic.
	var logMsgs []entityqueue.Message
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			if topic == "log" {
				logMsgs = append(logMsgs, msg)
			}
			return nil
		},
	).AnyTimes()
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(all.New(), nil).AnyTimes()
	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, registry, staticCounterFactory{counter: newSequentialCounter(ctrl)},
		storageFactoryFor(ctrl, mockStorage), analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))

	require.Len(t, logMsgs, 1)
	logEntry, err := entity.RequestLogFromBytes(logMsgs[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, request.ID, logEntry.RequestID)
	assert.Equal(t, entity.RequestStatusBatched, logEntry.Status)
	assert.Equal(t, request.Version+1, logEntry.RequestVersion)
	assert.Equal(t, "test-queue/batch/1", logEntry.Metadata["batch_id"])
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(entity.Request{}, fmt.Errorf("db connection lost"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage("test-queue/123", requestIDPayload(t, "test-queue/123"), "test-queue", nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_RequestBatchStoreFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	storeErr := errors.New("storage failed")

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1).Return(nil)

	requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	requestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(storeErr)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), store, nil, nil)
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.ErrorIs(t, err, storeErr)
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil, fmt.Errorf("publish failed"))

	request := testRequest()
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_CounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := newTestController(t, ctrl, cnt, nil, nil, nil)

	request := testRequest()
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
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
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/service/pull/789/789abc1234567890abcdef1234567890abcdef12"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}

	// Set up storage with active batches to become dependencies.
	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateCreated, Version: 1},
		{ID: "test-queue/batch/2", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 2},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(activeBatches[0], nil)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(activeBatches[1], nil)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{"test-queue/batch/1", "test-queue/batch/2"},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	// batch/1 has no existing dependents.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{
		BatchID: "test-queue/batch/1",
		Version: 1,
	}, nil)
	mockBatchDependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    "test-queue/batch/1",
		Dependents: []string{"test-queue/batch/1"},
		Version:    1,
	}, int32(1), int32(2)).Return(nil)
	// batch/2 already has an existing dependent.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.BatchDependent{
		BatchID:    "test-queue/batch/2",
		Dependents: []string{"test-queue/batch/99"},
		Version:    2,
	}, nil)
	mockBatchDependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    "test-queue/batch/2",
		Dependents: []string{"test-queue/batch/99", "test-queue/batch/1"},
		Version:    2,
	}, int32(2), int32(3)).Return(nil)
	// Create empty reverse index for the new batch.
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1).Return(nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, activeBatches...)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_AnalyzerSelectsSubset(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	// Two active batches in flight; analyzer picks only one as a conflict.
	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateCreated, Version: 1},
		{ID: "test-queue/batch/2", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 2},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(activeBatches[0], nil)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(activeBatches[1], nil)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{"test-queue/batch/2"},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	// Only batch/2 is selected by the analyzer, so only it gets a reverse-index update.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.BatchDependent{
		BatchID: "test-queue/batch/2",
		Version: 5,
	}, nil)
	mockBatchDependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    "test-queue/batch/2",
		Dependents: []string{"test-queue/batch/1"},
		Version:    5,
	}, int32(5), int32(6)).Return(nil)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1).Return(nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, activeBatches...)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	// Analyzer returns duplicate Conflict entries for the same batch (different
	// conflict types) to prove the controller dedupes by BatchID.
	analyzer := conflictmock.NewMockAnalyzer(ctrl)
	analyzer.EXPECT().Analyze(gomock.Any(), gomock.Any(), gomock.Any()).Return([]entity.Conflict{
		{BatchID: "test-queue/batch/2", Type: entity.ConflictTypeConservative},
		{BatchID: "test-queue/batch/2", Type: entity.ConflictTypeTargetOverlap},
	}, nil)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, analyzer, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_BatchDependentUpdateFailureDoesNotMutateFetchedDependents(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	activeBatch := entity.Batch{
		ID:      "test-queue/batch/99",
		Queue:   "test-queue",
		State:   entity.BatchStateCreated,
		Version: 1,
	}
	dependents := make([]string, 1, 2)
	dependents[0] = "test-queue/batch/98"
	existing := entity.BatchDependent{
		BatchID:    activeBatch.ID,
		Dependents: dependents,
		Version:    4,
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), activeBatch.ID).Return(activeBatch, nil)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), activeBatch.ID).Return(existing, nil)
	mockBatchDependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    activeBatch.ID,
		Dependents: []string{"test-queue/batch/98", "test-queue/batch/1"},
		Version:    existing.Version,
	}, existing.Version, existing.Version+1).Return(errors.New("update failed"))

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1).Return(nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, activeBatch)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.Equal(t, "", dependents[:cap(dependents)][1])
	assert.Equal(t, int32(4), existing.Version)
}

func TestController_Process_AnalyzerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	analyzer := conflictmock.NewMockAnalyzer(ctrl)
	analyzer.EXPECT().Analyze(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("analyzer down"))

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, analyzer, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil, nil)

	var _ consumer.Controller = controller
}

// A request that is halted (terminal OR Cancelling) must be short-circuited
// before the batch controller queries the batch store, allocates a batch ID,
// CAS-claims the request, or publishes. We verify by configuring a batch
// store and counter with NO EXPECTs (gomock fails on any call), a request
// store that only expects the initial Get (no UpdateState), and a publisher
// that returns a sentinel error if invoked.
//
// Cancelling is non-terminal but must halt forward progress: the cancel
// controller has already recorded the cancellation intent on the request and
// owns the terminal write. Any new batch spawned here would be an orphan
// containing a request that is about to become Cancelled.
func TestController_Process_HaltedShortCircuit(t *testing.T) {
	for _, state := range []entity.RequestState{
		entity.RequestStateCancelling,
		entity.RequestStateCancelled,
		entity.RequestStateLanded,
		entity.RequestStateError,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			request := testRequest()
			request.State = state
			request.Version = 7

			// Batch store with no EXPECTs — must not be queried.
			mockBatchStore := storagemock.NewMockBatchStore(ctrl)
			mockReqStore := storagemock.NewMockRequestStore(ctrl)
			mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
			// No UpdateState expected — gomock fails if called.

			mockStorage := storagemock.NewMockStorage(ctrl)
			mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
			mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

			// Counter with no EXPECTs — must not be called.
			cnt := countermock.NewMockCounter(ctrl)

			controller := newTestController(t, ctrl, cnt, mockStorage, nil, fmt.Errorf("should not publish"))

			msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
			delivery := consumermock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}

// Race-lost path: the cancel controller's markCancelling CAS landed first,
// so the batch controller's request-claim CAS (Validated → Batched) fails
// with storage.ErrVersionMismatch. The controller must ack the message (the
// cancel pipeline now owns the request) and must NOT call BatchStore.Create
// or publish to the speculate topic.
//
// This test exercises the race where the halted check at the top of Process
// passed against a stale in-memory copy from the initial Get (the cancel
// controller's CAS landed between our Get and our UpdateState). The CAS
// failure is the safety net that prevents an orphan batch in that window.
func TestController_Process_CASLostToCancel(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	// Create must NOT be called — gomock fails if it is.

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(
		gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1,
	).Return(fmt.Errorf("cas: %w", storage.ErrVersionMismatch))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	// Publisher with no EXPECTs — must not be called.
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ}},
	)
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(all.New(), nil).AnyTimes()
	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, registry, staticCounterFactory{counter: newSequentialCounter(ctrl)},
		storageFactoryFor(ctrl, mockStorage), analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
	assert.Equal(t, entity.RequestStateStarted, request.State)
	assert.Equal(t, int32(1), request.Version)
}

// Race-unexpected-error: any CAS failure other than ErrVersionMismatch (e.g.
// transient storage error) must surface as an error so the message is nacked
// for retry. We must NOT call BatchStore.Create on the way out.
func TestController_Process_CASUnexpectedErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	// Create must NOT be called — gomock fails if it is.

	casErr := fmt.Errorf("db connection lost")
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(
		gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1,
	).Return(casErr)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	// Cause must be preserved for upstream classification.
	assert.True(t, errors.Is(err, casErr))
	assert.Equal(t, entity.RequestStateStarted, request.State)
	assert.Equal(t, int32(1), request.Version)
}

// Recovery path: a re-delivered batch message whose prior attempt CAS'd the
// request to RequestStateBatched but failed before BatchStore.Create. The
// halted check at the top of Process does NOT include Batched (Batched is
// forward-progress, not halted), so we reach the CAS again and re-bump the
// version on the request (Batched → Batched, version+1). The batch is then
// re-created with a new batch ID, which is tolerated per the existing
// duplicate-handling comment on BatchStore.Create.
func TestController_Process_RecoveryAfterPriorCAS(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	request.State = entity.RequestStateBatched
	request.Version = 2 // prior attempt bumped from 1 → 2

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().Update(
		gomock.Any(), requestWithState(request, entity.RequestStateBatched), request.Version, request.Version+1,
	).Return(nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_ReadiesBatchBeforePublishing(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	batch := entity.Batch{
		ID:           "test-queue/batch/7",
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{},
		State:        entity.BatchStateCreating,
		Version:      1,
	}

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), counterDomainBatch).Return(int64(7), nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)

	requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	batchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	publisher := queuemock.NewMockPublisher(ctrl)
	gomock.InOrder(
		requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), int32(1), int32(2)).Return(nil),
		batchStore.EXPECT().Create(gomock.Any(), batch).Return(nil),
		requestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
			RequestID: request.ID,
			BatchID:   batch.ID,
			Version:   1,
		}).Return(nil),
		batchDependentStore.EXPECT().Create(gomock.Any(), entity.BatchDependent{
			BatchID:    batch.ID,
			Dependents: []string{},
			Version:    1,
		}).Return(nil),
		batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateCreated), int32(1), int32(2)).Return(nil),
		publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil),
		publisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil),
	)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(batchDependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()

	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: queue},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue},
	})
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(conflict.Config{QueueName: request.Queue}).Return(all.New(), nil)
	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, registry, staticCounterFactory{counter: cnt}, storageFactoryFor(ctrl, store), analyzerFactory,
		topickey.TopicKeyBatch, "orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	assert.NoError(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_RedeliveryMintsFreshBatchID(t *testing.T) {
	ctrl := gomock.NewController(t)

	firstRequest := testRequest()
	secondRequest := firstRequest
	secondRequest.State = entity.RequestStateBatched
	secondRequest.Version = 2

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), counterDomainBatch).Return(int64(1), nil)
	cnt.EXPECT().Next(gomock.Any(), counterDomainBatch).Return(int64(2), nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), firstRequest.ID).Return(firstRequest, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(firstRequest, entity.RequestStateBatched), int32(1), int32(2)).Return(nil)
	requestStore.EXPECT().Get(gomock.Any(), firstRequest.ID).Return(secondRequest, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(secondRequest, entity.RequestStateBatched), int32(2), int32(3)).Return(nil)

	var createdIDs []string
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, batch entity.Batch) error {
			createdIDs = append(createdIDs, batch.ID)
			assert.Equal(t, entity.BatchStateCreating, batch.State)
			return nil
		},
	).Times(2)
	batchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        firstRequest.Queue,
		Contains:     []string{firstRequest.ID},
		Dependencies: []string{},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)
	batchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           "test-queue/batch/2",
		Queue:        firstRequest.Queue,
		Contains:     []string{firstRequest.ID},
		Dependencies: []string{},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	batchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	batchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	requestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: firstRequest.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)
	requestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: firstRequest.ID,
		BatchID:   "test-queue/batch/2",
		Version:   1,
	}).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(batchDependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, cnt, store, nil, nil)
	msg := entityqueue.NewMessage(firstRequest.ID, requestIDPayload(t, firstRequest.ID), firstRequest.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(2).AnyTimes()

	assert.NoError(t, controller.Process(context.Background(), delivery))
	assert.NoError(t, controller.Process(context.Background(), delivery))
	assert.Equal(t, []string{"test-queue/batch/1", "test-queue/batch/2"}, createdIDs)
}

func TestController_Process_InitializationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateBatched), int32(1), int32(2)).Return(nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	batchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	batchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("storage failed"))

	requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	requestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(batchDependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), store, nil, nil)
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.ErrorContains(t, err, "failed to create batch dependent index")
}

func TestController_PopulateBatch_Errors(t *testing.T) {
	batch := entity.Batch{
		ID:           "test-queue/batch/1",
		Dependencies: []string{"test-queue/batch/0"},
		State:        entity.BatchStateCreating,
		Version:      1,
	}
	storeErr := errors.New("storage failed")

	tests := []struct {
		name     string
		mockFunc func(*storagemock.MockBatchStore, *storagemock.MockBatchDependentStore)
		errMsg   string
	}{
		{
			name: "own reverse index create fails",
			mockFunc: func(_ *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storeErr)
			},
			errMsg: "failed to create batch dependent index",
		},
		{
			name: "dependency get fails",
			mockFunc: func(_ *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				dependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/0").Return(entity.BatchDependent{}, storeErr)
			},
			errMsg: "failed to get batch dependent",
		},
		{
			name: "dependency update fails",
			mockFunc: func(_ *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				dependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/0").Return(entity.BatchDependent{
					BatchID: "test-queue/batch/0",
					Version: 2,
				}, nil)
				dependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
					BatchID:    "test-queue/batch/0",
					Dependents: []string{batch.ID},
					Version:    2,
				}, int32(2), int32(3)).Return(storeErr)
			},
			errMsg: "failed to update batch dependent index",
		},
		{
			name: "created transition fails",
			mockFunc: func(batchStore *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				dependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/0").Return(entity.BatchDependent{
					BatchID:    "test-queue/batch/0",
					Dependents: []string{"test-queue/batch/old"},
					Version:    2,
				}, nil)
				dependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
					BatchID:    "test-queue/batch/0",
					Dependents: []string{"test-queue/batch/old", batch.ID},
					Version:    2,
				}, int32(2), int32(3)).Return(nil)
				batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateCreated), int32(1), int32(2)).Return(storeErr)
			},
			errMsg: "failed to mark batch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
			tt.mockFunc(batchStore, batchDependentStore)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetBatchDependentStore().Return(batchDependentStore).AnyTimes()

			controller := &Controller{metricsScope: tally.NoopScope}
			_, err := controller.populateBatch(context.Background(), store, batch)
			assert.ErrorContains(t, err, tt.errMsg)
		})
	}
}
