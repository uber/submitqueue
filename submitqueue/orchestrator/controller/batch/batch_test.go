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
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/service/pull/456/abcdef0123456789abcdef0123456789abcdef01"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
}

// newTestController creates a controller with test dependencies.
// If mockStorage is nil, a default MockStorage with an empty batch store is created.
// If analyzer is nil, the "all" conflict analyzer is used (every active batch becomes a dependency).
// scorePublishErr, if non-nil, is returned only for publishes to the "score" topic; the
// log publish (which the controller emits first) always succeeds, so callers exercising the
// score publish-failure path are not short-circuited on the earlier log publish.
func newTestController(t *testing.T, ctrl *gomock.Controller, cnt *countermock.MockCounter, mockStorage *storagemock.MockStorage, analyzer conflict.Analyzer, scorePublishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockBatchStore := storagemock.NewMockBatchStore(ctrl)
		mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockReqStore := storagemock.NewMockRequestStore(ctrl)
		req := testRequest()
		mockReqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil).AnyTimes()
		mockReqStore.EXPECT().UpdateState(gomock.Any(), req.ID, req.Version, req.Version+1, entity.RequestStateBatched).Return(nil).AnyTimes()

		mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
		mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
		mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
		mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	}

	var requestBatch entity.RequestBatch
	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, requestID string) (entity.RequestBatch, error) {
			if requestBatch.RequestID == "" {
				return entity.RequestBatch{}, fmt.Errorf("request batch not found: %w", storage.ErrNotFound)
			}
			return requestBatch, nil
		},
	).AnyTimes()
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, assignment entity.RequestBatch) error {
			if requestBatch.RequestID != "" {
				return fmt.Errorf("request batch exists: %w", storage.ErrAlreadyExists)
			}
			requestBatch = assignment
			return nil
		},
	).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()

	if analyzer == nil {
		analyzer = all.New()
	}

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			if topic == "score" {
				return scorePublishErr
			}
			return nil
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeyScore, Name: "score", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(analyzer, nil).AnyTimes()

	return NewController(logger, scope, registry, cnt, mockStorage, analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch")
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
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

// TestController_Process_PublishesBatchedLog asserts the controller emits a
// "batched" request log carrying the request ID, the post-CAS request version,
// and the batch ID it was placed into.
func TestController_Process_PublishesBatchedLog(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().UpdateState(gomock.Any(), request.ID, request.Version, request.Version+1, entity.RequestStateBatched).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Get(gomock.Any(), request.ID).Return(
		entity.RequestBatch{}, fmt.Errorf("missing: %w", storage.ErrNotFound),
	)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
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
			{Key: topickey.TopicKeyScore, Name: "score", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(all.New(), nil).AnyTimes()
	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, registry, newSequentialCounter(ctrl),
		mockStorage, analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
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
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage("test-queue/123", requestIDPayload(t, "test-queue/123"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_RedeliveryReusesBatchAndReplaysFanout(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	batchedRequest := request
	batchedRequest.State = entity.RequestStateBatched
	batchedRequest.Version++
	requestBatch := entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}
	batch := entity.Batch{
		ID:       requestBatch.BatchID,
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	gomock.InOrder(
		mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil),
		mockReqStore.EXPECT().UpdateState(
			gomock.Any(), request.ID, request.Version, batchedRequest.Version, entity.RequestStateBatched,
		).Return(nil),
		mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(batchedRequest, nil),
	)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	gomock.InOrder(
		mockRequestBatchStore.EXPECT().Get(gomock.Any(), request.ID).Return(
			entity.RequestBatch{}, fmt.Errorf("missing: %w", storage.ErrNotFound),
		),
		mockRequestBatchStore.EXPECT().Create(gomock.Any(), requestBatch).Return(nil),
		mockRequestBatchStore.EXPECT().Get(gomock.Any(), request.ID).Return(requestBatch, nil),
	)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	gomock.InOrder(
		mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), request.Queue, entity.DependencyBatchStates()).Return(nil, nil),
		mockBatchStore.EXPECT().Create(gomock.Any(), batch).Return(nil),
		mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil),
	)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	gomock.InOrder(
		mockBatchDependentStore.EXPECT().Create(gomock.Any(), entity.BatchDependent{
			BatchID:    batch.ID,
			Dependents: []string{},
			Version:    1,
		}).Return(nil),
		mockBatchDependentStore.EXPECT().Create(gomock.Any(), entity.BatchDependent{
			BatchID:    batch.ID,
			Dependents: []string{},
			Version:    1,
		}).Return(fmt.Errorf("exists: %w", storage.ErrAlreadyExists)),
	)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()

	var logMessages []entityqueue.Message
	var scoreMessages []entityqueue.Message
	publishErr := fmt.Errorf("publish failed")
	mockPub := queuemock.NewMockPublisher(ctrl)
	gomock.InOrder(
		mockPub.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).DoAndReturn(
			func(ctx context.Context, topic string, msg entityqueue.Message) error {
				logMessages = append(logMessages, msg)
				return nil
			},
		),
		mockPub.EXPECT().Publish(gomock.Any(), "score", gomock.Any()).DoAndReturn(
			func(ctx context.Context, topic string, msg entityqueue.Message) error {
				scoreMessages = append(scoreMessages, msg)
				return publishErr
			},
		),
		mockPub.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).DoAndReturn(
			func(ctx context.Context, topic string, msg entityqueue.Message) error {
				logMessages = append(logMessages, msg)
				return nil
			},
		),
		mockPub.EXPECT().Publish(gomock.Any(), "score", gomock.Any()).DoAndReturn(
			func(ctx context.Context, topic string, msg entityqueue.Message) error {
				scoreMessages = append(scoreMessages, msg)
				return nil
			},
		),
	)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyScore, Name: "score", Queue: mockQ},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
	})
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(all.New(), nil)
	controller := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		registry,
		newSequentialCounter(ctrl),
		mockStorage,
		analyzerFactory,
		topickey.TopicKeyBatch,
		"orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.ErrorIs(t, controller.Process(context.Background(), delivery), publishErr)
	require.NoError(t, controller.Process(context.Background(), delivery))

	require.Len(t, logMessages, 2)
	assert.Equal(t, logMessages[0].ID, logMessages[1].ID)
	assert.Equal(t, logMessages[0].Payload, logMessages[1].Payload)
	assert.Equal(t, logMessages[0].PartitionKey, logMessages[1].PartitionKey)

	require.Len(t, scoreMessages, 2)
	assert.Equal(t, scoreMessages[0].ID, scoreMessages[1].ID)
	assert.Equal(t, scoreMessages[0].Payload, scoreMessages[1].Payload)
	assert.Equal(t, scoreMessages[0].PartitionKey, scoreMessages[1].PartitionKey)
}

func TestController_Process_CounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := newTestController(t, ctrl, cnt, nil, nil, nil)

	request := testRequest()
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
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
	mockReqStore.EXPECT().UpdateState(
		gomock.Any(), request.ID, request.Version, request.Version+1, entity.RequestStateBatched,
	).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_RedeliveryDoesNotDuplicateDependent(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	request.State = entity.RequestStateBatched
	request.Version = 2
	requestBatch := entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/3",
		Version:   1,
	}
	batch := entity.Batch{
		ID:           requestBatch.BatchID,
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{"test-queue/batch/2"},
		State:        entity.BatchStateCreated,
		Version:      1,
	}

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Get(gomock.Any(), request.ID).Return(requestBatch, nil)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(
		fmt.Errorf("exists: %w", storage.ErrAlreadyExists),
	)
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.BatchDependent{
		BatchID:    "test-queue/batch/2",
		Dependents: []string{batch.ID},
		Version:    4,
	}, nil)
	// No UpdateDependents expectation. The existing edge is already the
	// reconciled postcondition.

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)
	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(2).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestController_EnsureDependentVersionConflictReturnsForRedelivery(t *testing.T) {
	ctrl := gomock.NewController(t)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{
		BatchID: "test-queue/batch/1",
		Version: 3,
	}, nil)
	dependentStore.EXPECT().UpdateDependents(
		gomock.Any(),
		"test-queue/batch/1",
		int32(3),
		int32(4),
		[]string{"test-queue/batch/2"},
	).Return(fmt.Errorf("cas: %w", storage.ErrVersionMismatch))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchDependentStore().Return(dependentStore)

	controller := &Controller{
		metricsScope: tally.NoopScope,
		store:        store,
	}

	err := controller.ensureDependent(context.Background(), "test-queue/batch/1", "test-queue/batch/2")
	require.ErrorIs(t, err, storage.ErrVersionMismatch)
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
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(activeBatches, nil)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	// Only batch/2 is selected by the analyzer, so only it gets a reverse-index update.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.BatchDependent{
		BatchID: "test-queue/batch/2",
		Version: 5,
	}, nil)
	mockBatchDependentStore.EXPECT().UpdateDependents(gomock.Any(), "test-queue/batch/2", int32(5), int32(6), gomock.Any()).Return(nil)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().UpdateState(
		gomock.Any(), request.ID, request.Version, request.Version+1, entity.RequestStateBatched,
	).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
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
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_AnalyzerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(nil, nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().UpdateState(
		gomock.Any(), request.ID, request.Version, request.Version+1, entity.RequestStateBatched,
	).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	analyzer := conflictmock.NewMockAnalyzer(ctrl)
	analyzer.EXPECT().Analyze(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("analyzer down"))

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, analyzer, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
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

			// The batch store has no expectations and must not be queried.
			mockBatchStore := storagemock.NewMockBatchStore(ctrl)
			mockReqStore := storagemock.NewMockRequestStore(ctrl)
			mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
			// No UpdateState call is expected; gomock fails if it is called.

			mockStorage := storagemock.NewMockStorage(ctrl)
			mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
			mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

			// The counter has no expectations and must not be called.
			cnt := countermock.NewMockCounter(ctrl)

			controller := newTestController(t, ctrl, cnt, mockStorage, nil, fmt.Errorf("should not publish"))

			msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}

// A request CAS conflict must return the version mismatch so consumer
// redelivery can reload and classify the latest durable state.
func TestController_Process_VersionConflictReturnsForRedelivery(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	gomock.InOrder(
		mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil),
		mockReqStore.EXPECT().UpdateState(
			gomock.Any(), request.ID, request.Version, request.Version+1, entity.RequestStateBatched,
		).Return(fmt.Errorf("cas: %w", storage.ErrVersionMismatch)),
	)

	mockRequestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockRequestBatchStore.EXPECT().Get(gomock.Any(), request.ID).Return(
		entity.RequestBatch{}, fmt.Errorf("missing: %w", storage.ErrNotFound),
	)
	mockRequestBatchStore.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: request.ID,
		BatchID:   "test-queue/batch/1",
		Version:   1,
	}).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockRequestBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	// Publisher with no expectations must not be called.
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyScore, Name: "score", Queue: mockQ}},
	)
	require.NoError(t, err)

	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, registry, newSequentialCounter(ctrl),
		mockStorage, analyzerFactory, topickey.TopicKeyBatch, "orchestrator-batch",
	)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.ErrorIs(t, err, storage.ErrVersionMismatch)
}

// A non-conflict CAS error must be preserved so the consumer can classify it.
func TestController_Process_CASUnexpectedErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	casErr := fmt.Errorf("db connection lost")

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	mockReqStore.EXPECT().UpdateState(
		gomock.Any(), request.ID, request.Version, request.Version+1, entity.RequestStateBatched,
	).Return(casErr)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errors.Is(err, casErr))
}

// A batched request without a request_batch row can recover the one active
// legacy batch that already contains it. The request version is not bumped.
func TestController_Process_RecoveryAfterPriorCAS(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := testRequest()
	request.State = entity.RequestStateBatched
	request.Version = 2
	batch := entity.Batch{
		ID:       "test-queue/batch/7",
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), request.Queue, entity.ActiveBatchStates()).Return([]entity.Batch{batch}, nil)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()
	mockStorage.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}
