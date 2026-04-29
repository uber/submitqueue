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

package speculate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// testBatch returns a standard test batch with the given state and dependencies.
func testBatch(state entity.BatchState, deps ...string) entity.Batch {
	return entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        "test-queue",
		Dependencies: deps,
		State:        state,
		Version:      1,
	}
}

// newTestController wires a controller with a registry covering all topics the
// speculate controller may publish to. The publisher returns publishErr (or nil).
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, publishErr error) *Controller {
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
			{Key: consumer.TopicKeyBuild, Name: "build", Queue: mockQ},
			{Key: consumer.TopicKeyMerge, Name: "merge", Queue: mockQ},
			{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, registry, consumer.TopicKeySpeculate, "orchestrator-speculate")
}

// runProcess builds a delivery for batchID and invokes Process once.
func runProcess(t *testing.T, ctrl *gomock.Controller, controller *Controller, batchID string) error {
	msg := queue.NewMessage(batchID, batchIDPayload(t, batchID), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return controller.Process(context.Background(), delivery)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	controller := newTestController(t, ctrl, store, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeySpeculate, controller.TopicKey())
	assert.Equal(t, "orchestrator-speculate", controller.ConsumerGroup())
	assert.Equal(t, "speculate", controller.Name())

	var _ consumer.Controller = controller
}

// startSpeculation: Created/Scored should publish to build and CAS to Speculating with newVersion = oldVersion+1.
func TestController_Process_StartSpeculation(t *testing.T) {
	tests := []struct {
		name  string
		state entity.BatchState
	}{
		{name: "from_created", state: entity.BatchStateCreated},
		{name: "from_scored", state: entity.BatchStateScored},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batch := testBatch(tt.state)

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			controller := newTestController(t, ctrl, store, nil)
			require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
		})
	}
}

// tryFinalize: Speculating with no deps should publish to merge and CAS to Merging.
func TestController_Process_FinalizeNoDeps(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateSpeculating)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
}

// tryFinalize: Speculating with all deps Succeeded should publish to merge and CAS to Merging.
func TestController_Process_FinalizeAllDepsSucceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	depA := entity.Batch{ID: "test-queue/batch/0a", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 5}
	depB := entity.Batch{ID: "test-queue/batch/0b", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 3}
	batch := testBatch(entity.BatchStateSpeculating, depA.ID, depB.ID)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), depA.ID).Return(depA, nil)
	batchStore.EXPECT().Get(gomock.Any(), depB.ID).Return(depB, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
}

// tryFinalize: Speculating with a dep still in flight is a no-op (no publish, no state change).
func TestController_Process_WaitingOnDep(t *testing.T) {
	ctrl := gomock.NewController(t)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
	// No UpdateState expected — gomock will fail if it is called.

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
}

// tryFinalize: a dep in a non-succeeding terminal state must fail the batch
// (Speculating → Failed) and publish to conclude. Otherwise the batch livelocks.
func TestController_Process_FailedDepFailsBatch(t *testing.T) {
	tests := []struct {
		name     string
		depState entity.BatchState
	}{
		{name: "dep_failed", depState: entity.BatchStateFailed},
		{name: "dep_cancelled", depState: entity.BatchStateCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: tt.depState, Version: 1}
			batch := testBatch(entity.BatchStateSpeculating, dep.ID)
			batch.Contains = []string{"test-queue/req/1", "test-queue/req/2"}

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
			batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateFailed).Return(nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			controller := newTestController(t, ctrl, store, nil)
			require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
		})
	}
}

// Merging is owned by the merge controller — speculate is a no-op for it.
func TestController_Process_MergingNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateMerging)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// No UpdateState expected.

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
}

// Terminal states re-fan-out to conclude for self-healing in case a previous
// publish was lost. State must not change (no UpdateState).
func TestController_Process_TerminalSelfHeals(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
		entity.BatchStateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batch := testBatch(state)

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			// No UpdateState expected.

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			// Require exactly one publish to the conclude topic for self-healing.
			mockPub := queuemock.NewMockPublisher(ctrl)
			mockPub.EXPECT().Publish(gomock.Any(), "conclude", gomock.Any()).Return(nil).Times(1)

			mockQ := queuemock.NewMockQueue(ctrl)
			mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

			registry, err := consumer.NewTopicRegistry(
				[]consumer.TopicConfig{
					{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
				},
			)
			require.NoError(t, err)

			logger := zaptest.NewLogger(t).Sugar()
			controller := NewController(logger, tally.NoopScope, store, registry, consumer.TopicKeySpeculate, "orchestrator-speculate")

			require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
		})
	}
}

// An unrecognized state must surface as an error so the message is nacked
// instead of silently acked — silently acking would drop the event.
func TestController_Process_UnrecognizedState(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateUnknown)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.Error(t, runProcess(t, ctrl, controller, batch.ID))
}

// Storage failure on the primary batch fetch surfaces as an error and is not
// retryable per the controller default (plain fmt.Errorf).
func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("db connection lost"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	err := runProcess(t, ctrl, controller, "test-queue/batch/1")
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

// Publish failure must not advance the batch state.
func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateScored)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// No UpdateState expected — publish fails before we get there.

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, fmt.Errorf("publish failed"))
	require.Error(t, runProcess(t, ctrl, controller, batch.ID))
}

// Malformed payload: deserialize error.
func TestController_Process_BadPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	controller := newTestController(t, ctrl, store, nil)

	msg := queue.NewMessage("anything", []byte("not-json"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.Error(t, controller.Process(context.Background(), delivery))
}
