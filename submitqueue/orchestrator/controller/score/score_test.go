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

package score

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	scorermock "github.com/uber/submitqueue/submitqueue/extension/scorer/mock"
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

// testBatch returns a standard test batch for score tests.
func testBatch() entity.Batch {
	return entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/1"},
		State:    entity.BatchStateCreated,
		Version:  1,
	}
}

// testRequest returns a standard test request for score tests.
func testRequest() entity.Request {
	return entity.Request{
		ID:    "test-queue/1",
		Queue: "test-queue",
		Change: entity.Change{
			URIs: []string{"github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"},
		},
		State:   entity.RequestStateStarted,
		Version: 1,
	}
}

// newMockStorage creates a MockStorage with a MockBatchStore and MockRequestStore.
func newMockStorage(ctrl *gomock.Controller, batch entity.Batch, request entity.Request) *storagemock.MockStorage {
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	mockBatchStore.EXPECT().UpdateScoreAndState(gomock.Any(), batch.ID, batch.Version, batch.Version+1, gomock.Any(), entity.BatchStateScored).Return(nil).AnyTimes()

	mockRequestStore := storagemock.NewMockRequestStore(ctrl)
	mockRequestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
	return store
}

// newTestController creates a controller with test dependencies.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, scorer *scorermock.MockScorer, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, scorer, registry, consumer.TopicKeyScore, "orchestrator-score")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch()
	request := testRequest()
	store := newMockStorage(ctrl, batch, request)
	mockScorer := scorermock.NewMockScorer(ctrl)
	controller := newTestController(t, ctrl, store, mockScorer, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyScore, controller.TopicKey())
	assert.Equal(t, "orchestrator-score", controller.ConsumerGroup())
	assert.Equal(t, "score", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	request := testRequest()
	store := newMockStorage(ctrl, batch, request)

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), request.Change).Return(0.85, nil)

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_MultipleRequests_MinScore(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/1", "test-queue/2"},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	request1 := entity.Request{
		ID:      "test-queue/1",
		Queue:   "test-queue",
		Change:  entity.Change{URIs: []string{"github://uber/repo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		State:   entity.RequestStateStarted,
		Version: 1,
	}
	request2 := entity.Request{
		ID:      "test-queue/2",
		Queue:   "test-queue",
		Change:  entity.Change{URIs: []string{"github://uber/repo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
		State:   entity.RequestStateStarted,
		Version: 1,
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// Expect the multiplicative score (0.9 * 0.6 = 0.54) to be persisted
	mockBatchStore.EXPECT().UpdateScoreAndState(gomock.Any(), batch.ID, batch.Version, batch.Version+1, 0.54, entity.BatchStateScored).Return(nil)

	mockRequestStore := storagemock.NewMockRequestStore(ctrl)
	mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(request1, nil)
	mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/2").Return(request2, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), request1.Change).Return(0.9, nil)
	mockScorer.EXPECT().Score(gomock.Any(), request2.Change).Return(0.6, nil)

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("db connection lost"))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	mockScorer := scorermock.NewMockScorer(ctrl)
	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := queue.NewMessage("test-queue/batch/1", batchIDPayload(t, "test-queue/batch/1"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_ScorerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	mockRequestStore := storagemock.NewMockRequestStore(ctrl)
	mockRequestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), request.Change).Return(0.0, fmt.Errorf("no bucket matches value 99"))

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestController_Process_UpdateScoreFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	request := testRequest()

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	mockBatchStore.EXPECT().UpdateScoreAndState(gomock.Any(), batch.ID, batch.Version, batch.Version+1, gomock.Any(), entity.BatchStateScored).Return(fmt.Errorf("version mismatch"))

	mockRequestStore := storagemock.NewMockRequestStore(ctrl)
	mockRequestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), request.Change).Return(0.85, nil)

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	request := testRequest()
	store := newMockStorage(ctrl, batch, request)

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), request.Change).Return(0.85, nil)

	controller := newTestController(t, ctrl, store, mockScorer, fmt.Errorf("publish failed"))

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch()
	request := testRequest()
	store := newMockStorage(ctrl, batch, request)
	mockScorer := scorermock.NewMockScorer(ctrl)
	controller := newTestController(t, ctrl, store, mockScorer, nil)

	var _ consumer.Controller = controller
}

// A batch already in a terminal state (e.g. cancelled while the score message
// was in flight) must be short-circuited: no scoring, no UpdateScoreAndState,
// and no fan-out — score owns no terminal write, so it owns no recovery; the
// controller that wrote the terminal state already published to conclude, and
// speculate's terminal self-heal republishes on redelivery.
func TestController_Process_TerminalShortCircuit(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateCancelled,
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := testBatch()
			batch.State = state

			// Batch store: only Get; no UpdateScoreAndState (gomock fails if called).
			mockBatchStore := storagemock.NewMockBatchStore(ctrl)
			mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

			mockStorage := storagemock.NewMockStorage(ctrl)
			mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

			// Scorer with no EXPECTs — must not be called.
			mockScorer := scorermock.NewMockScorer(ctrl)

			logger := zaptest.NewLogger(t).Sugar()
			scope := tally.NoopScope

			// Publisher with no EXPECTs — must not be called (no fan-out on terminal).
			mockPub := queuemock.NewMockPublisher(ctrl)

			mockQ := queuemock.NewMockQueue(ctrl)
			mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

			registry, err := consumer.NewTopicRegistry(
				[]consumer.TopicConfig{
					{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
					{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
					{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ},
				},
			)
			require.NoError(t, err)

			controller := NewController(logger, scope, mockStorage, mockScorer, registry, consumer.TopicKeyScore, "orchestrator-score")

			msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}

// A batch in BatchStateCancelling must be silently acked: no scoring, no
// UpdateScoreAndState, and crucially NO conclude publish (speculate owns the
// terminal write to Cancelled and the downstream dependent / conclude
// publishes — conclude would also error on a non-terminal Cancelling batch).
func TestController_Process_CancellingShortCircuit(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	batch.State = entity.BatchStateCancelling

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	// Scorer with no EXPECTs — must not be called.
	mockScorer := scorermock.NewMockScorer(ctrl)

	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	// Publisher with no EXPECTs — must not be called (no fan-out for Cancelling).
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	controller := NewController(logger, scope, mockStorage, mockScorer, registry, consumer.TopicKeyScore, "orchestrator-score")

	msg := queue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}
