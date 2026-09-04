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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/landstrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/extension/counter"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
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
		LandStrategy: landstrategy.StrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
}

func newTestRegistry(t *testing.T, publisher *queuemock.MockPublisher, ctrl *gomock.Controller) consumer.TopicRegistry {
	t.Helper()

	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyDependencyAnalysis, Name: "dependency-analysis", Queue: queue},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue},
	})
	require.NoError(t, err)
	return registry
}

// newTestController creates a controller with test dependencies.
// If mockStorage is nil, a default MockStorage accepting any batch write is created.
// handoffPublishErr, if non-nil, is returned for the hand-off publish.
func newTestController(t *testing.T, ctrl *gomock.Controller, cnt *countermock.MockCounter, mockStorage *storagemock.MockStorage, handoffPublishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockBatchStore := storagemock.NewMockBatchStore(ctrl)
		mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		req := testRequest()
		mockReqStore := storagemock.NewMockRequestStore(ctrl)
		mockReqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil).AnyTimes()

		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
		mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	}

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(handoffPublishErr).AnyTimes()

	return NewController(logger, scope, newTestRegistry(t, mockPub, ctrl), staticCounterFactory{counter: cnt},
		storageFactoryFor(ctrl, mockStorage), topickey.TopicKeyBatch, "orchestrator-batch")
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, request entity.Request, payloadQueue string) *consumermock.MockDelivery {
	t.Helper()

	payload := requestIDPayload(t, request.ID)
	if payloadQueue != "" {
		bytes, err := entity.RequestID{ID: request.ID, Queue: payloadQueue}.ToBytes()
		require.NoError(t, err)
		payload = bytes
	}
	msg := entityqueue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyBatch, controller.TopicKey())
	assert.Equal(t, "orchestrator-batch", controller.ConsumerGroup())
	assert.Equal(t, "batch", controller.Name())

	var _ consumer.Controller = controller
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, testRequest(), "")))
}

// A payload whose queue disagrees with the request's authoritative queue is
// rejected without touching the counter or the batch store.
func TestController_Process_QueueMismatchRejected(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(storagemock.NewMockBatchStore(ctrl)).AnyTimes()

	// Counter with no EXPECTs — must not be called.
	controller := newTestController(t, ctrl, countermock.NewMockCounter(ctrl), mockStorage, nil)
	require.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "some-other-queue")))
}

// The batch ID handed on carries the batch's queue, and the message is
// partitioned by queue so analysis of one queue stays serial.
func TestController_Process_StampsQueueOnHandoffPayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	var handoffs []entityqueue.Message
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil).AnyTimes()
	publisher.EXPECT().Publish(gomock.Any(), "dependency-analysis", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			handoffs = append(handoffs, msg)
			return nil
		},
	)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, newTestRegistry(t, publisher, ctrl),
		staticCounterFactory{counter: newSequentialCounter(ctrl)},
		storageFactoryFor(ctrl, store), topickey.TopicKeyBatch, "orchestrator-batch",
	)

	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))
	require.Len(t, handoffs, 1)
	bid, err := entity.BatchIDFromBytes(handoffs[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, request.Queue, bid.Queue)
	assert.Equal(t, request.Queue, handoffs[0].PartitionKey)
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(entity.Request{}, fmt.Errorf("db connection lost"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(storagemock.NewMockBatchStore(ctrl)).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)
	assert.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))
}

func TestController_Process_BatchStoreFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("storage failed"))
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), store, nil)
	assert.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, fmt.Errorf("publish failed"))
	assert.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, testRequest(), "")))
}

func TestController_Process_CounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))

	controller := newTestController(t, ctrl, cnt, nil, nil)
	assert.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, testRequest(), "")))
}

// A halted request must never spawn a batch. Cancelling is non-terminal but
// still halts: cancel owns the request's outcome from that point.
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

			// Batch store and counter with no EXPECTs — neither may be touched.
			mockReqStore := storagemock.NewMockRequestStore(ctrl)
			mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

			mockStorage := storagemock.NewMockStorage(ctrl)
			mockStorage.EXPECT().GetBatchStore().Return(storagemock.NewMockBatchStore(ctrl)).AnyTimes()
			mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

			controller := newTestController(t, ctrl, countermock.NewMockCounter(ctrl), mockStorage,
				fmt.Errorf("should not publish"))

			require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))
		})
	}
}

// The batch is durable before it is handed off: the next stage reloads it by
// ID, so a hand-off that overtook its own write would find nothing.
func TestController_Process_WritesBatchBeforeHandoff(t *testing.T) {
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
	publisher := queuemock.NewMockPublisher(ctrl)
	gomock.InOrder(
		batchStore.EXPECT().Create(gomock.Any(), batch).Return(nil),
		publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil),
		publisher.EXPECT().Publish(gomock.Any(), "dependency-analysis", gomock.Any()).Return(nil),
	)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, newTestRegistry(t, publisher, ctrl),
		staticCounterFactory{counter: cnt}, storageFactoryFor(ctrl, store),
		topickey.TopicKeyBatch, "orchestrator-batch",
	)

	assert.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))
}

// The batch leaves this stage unresolved and unclaimed: dependency analysis
// owns both.
func TestController_Process_CreatesBatchInCreatingWithoutDependencies(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	var created []entity.Batch
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, batch entity.Batch) error {
			created = append(created, batch)
			return nil
		},
	)

	// A request store that only answers Get — a claim here would fail the test.
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), store, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))

	require.Len(t, created, 1)
	assert.Equal(t, entity.BatchStateCreating, created[0].State)
	assert.Empty(t, created[0].Dependencies)
	assert.Equal(t, []string{request.ID}, created[0].Contains)
}

// The request is told a batch is being built for it. "batching" rather than
// "batched": the batch has no dependencies yet and may still be discarded in
// favour of another.
func TestController_Process_PublishesBatchingStatus(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	var logs []entity.RequestLog
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			if topic == "log" {
				entry, err := entity.RequestLogFromBytes(msg.Payload)
				require.NoError(t, err)
				logs = append(logs, entry)
			}
			return nil
		},
	).AnyTimes()

	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, newTestRegistry(t, publisher, ctrl),
		staticCounterFactory{counter: newSequentialCounter(ctrl)},
		storageFactoryFor(ctrl, store), topickey.TopicKeyBatch, "orchestrator-batch",
	)

	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, request, "")))

	require.Len(t, logs, 1)
	assert.Equal(t, request.ID, logs[0].RequestID)
	assert.Equal(t, entity.RequestStatusBatching, logs[0].Status)
	assert.Equal(t, "test-queue/batch/1", logs[0].Metadata["batch_id"])
}
