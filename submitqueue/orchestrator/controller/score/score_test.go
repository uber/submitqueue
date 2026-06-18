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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/change"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
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
		Change: change.Change{
			URIs: []string{"github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"},
		},
		State:   entity.RequestStateStarted,
		Version: 1,
	}
}

// mockChangeStore returns a MockChangeStore that serves one self-owned ChangeRecord
// per URI for each request, mirroring what start.claimURIs + validate enrichment
// would have persisted. The score controller reads these via GetByURI to assemble
// the batch's changes.
func mockChangeStore(ctrl *gomock.Controller, requests ...entity.Request) *storagemock.MockChangeStore {
	cs := storagemock.NewMockChangeStore(ctrl)
	for _, req := range requests {
		for _, uri := range req.Change.URIs {
			rec := entity.ChangeRecord{
				URI:       uri,
				RequestID: req.ID,
				Queue:     req.Queue,
				Details:   entity.ChangeDetails{ChangedFiles: []entity.ChangedFile{{Path: "f.go", LinesAdded: 5}}},
				Version:   1,
			}
			cs.EXPECT().GetByURI(gomock.Any(), req.Queue, uri).Return([]entity.ChangeRecord{rec}, nil).AnyTimes()
		}
	}
	return cs
}

func expectScoreMembership(ctrl *gomock.Controller, store *storagemock.MockStorage, batch entity.Batch) {
	membershipStore := storagemock.NewMockBatchStateMembershipStore(ctrl)
	membershipStore.EXPECT().Add(gomock.Any(), batch.Queue, entity.BatchStateScored, batch.ID).Return(nil).AnyTimes()
	membershipStore.EXPECT().Remove(gomock.Any(), batch.Queue, batch.State, batch.ID).Return(nil).AnyTimes()
	store.EXPECT().GetBatchStateMembershipStore().Return(membershipStore).AnyTimes()
}

// newMockStorage creates a MockStorage with a MockBatchStore, MockRequestStore, and MockChangeStore.
func newMockStorage(ctrl *gomock.Controller, batch entity.Batch, request entity.Request) *storagemock.MockStorage {
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	mockBatchStore.EXPECT().UpdateScoreAndState(gomock.Any(), batch.ID, batch.Version, batch.Version+1, gomock.Any(), entity.BatchStateScored).Return(nil).AnyTimes()

	mockRequestStore := storagemock.NewMockRequestStore(ctrl)
	mockRequestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	expectScoreMembership(ctrl, store, batch)
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
	store.EXPECT().GetChangeStore().Return(mockChangeStore(ctrl, request)).AnyTimes()
	return store
}

// newTestController creates a controller with test dependencies.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, scorer *scorermock.MockScorer, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	scorerFactory := scorermock.NewMockFactory(ctrl)
	scorerFactory.EXPECT().For(gomock.Any()).Return(scorer, nil).AnyTimes()

	return NewController(logger, scope, store, scorerFactory, registry, topickey.TopicKeyScore, "orchestrator-score")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch()
	request := testRequest()
	store := newMockStorage(ctrl, batch, request)
	mockScorer := scorermock.NewMockScorer(ctrl)
	controller := newTestController(t, ctrl, store, mockScorer, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyScore, controller.TopicKey())
	assert.Equal(t, "orchestrator-score", controller.ConsumerGroup())
	assert.Equal(t, "score", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	request := testRequest()
	store := newMockStorage(ctrl, batch, request)

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.85, nil)

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

// TestController_Process_BatchLevelScore verifies the controller hands the batch
// identity to the scorer and persists the single score it returns. Resolving the
// batch's changes is the scorer's concern (via the changeset resolver), not the
// controller's.
func TestController_Process_BatchLevelScore(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/1", "test-queue/2"},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// The single batch-level score is persisted.
	mockBatchStore.EXPECT().UpdateScoreAndState(gomock.Any(), batch.ID, batch.Version, batch.Version+1, 0.7, entity.BatchStateScored).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	expectScoreMembership(ctrl, store, batch)

	// The controller passes the batch identity to the scorer and persists its score.
	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, b entity.Batch) (float64, error) {
			assert.Equal(t, batch.ID, b.ID)
			return 0.7, nil
		},
	)

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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

	msg := entityqueue.NewMessage("test-queue/batch/1", batchIDPayload(t, "test-queue/batch/1"), "test-queue", nil)
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
	mockRequestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	expectScoreMembership(ctrl, store, batch)
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
	store.EXPECT().GetChangeStore().Return(mockChangeStore(ctrl, request)).AnyTimes()

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.0, fmt.Errorf("no bucket matches value 99"))

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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
	mockRequestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	expectScoreMembership(ctrl, store, batch)
	store.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
	store.EXPECT().GetChangeStore().Return(mockChangeStore(ctrl, request)).AnyTimes()

	mockScorer := scorermock.NewMockScorer(ctrl)
	mockScorer.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.85, nil)

	controller := newTestController(t, ctrl, store, mockScorer, nil)

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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
	mockScorer.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.85, nil)

	controller := newTestController(t, ctrl, store, mockScorer, fmt.Errorf("publish failed"))

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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
					{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
					{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: mockQ},
					{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
				},
			)
			require.NoError(t, err)

			scorerFactory := scorermock.NewMockFactory(ctrl)
			scorerFactory.EXPECT().For(gomock.Any()).Return(mockScorer, nil).AnyTimes()
			controller := NewController(logger, scope, mockStorage, scorerFactory, registry, topickey.TopicKeyScore, "orchestrator-score")

			msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
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
			{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
			{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	scorerFactory := scorermock.NewMockFactory(ctrl)
	scorerFactory.EXPECT().For(gomock.Any()).Return(mockScorer, nil).AnyTimes()
	controller := NewController(logger, scope, mockStorage, scorerFactory, registry, topickey.TopicKeyScore, "orchestrator-score")

	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}
