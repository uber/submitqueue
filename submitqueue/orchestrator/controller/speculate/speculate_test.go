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
	"github.com/uber/submitqueue/core/errs"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
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
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
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
	msg := entityqueue.NewMessage(batchID, batchIDPayload(t, batchID), "test-queue", nil)
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

// tryFinalize: a failed dep must fail the batch (Speculating → Failed) and
// publish to conclude. Otherwise the batch livelocks.
func TestController_Process_FailedDepFailsBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateFailed, Version: 1}
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
}

// tryFinalize: a cancelled dep is treated as out-of-the-way — it will never
// land and can no longer conflict. The dep is dropped from the chain and the
// batch advances to Merging as if the cancelled dep had succeeded.
func TestController_Process_CancelledDepSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	depCancelled := entity.Batch{ID: "test-queue/batch/0a", Queue: "test-queue", State: entity.BatchStateCancelled, Version: 2}
	depSucceeded := entity.Batch{ID: "test-queue/batch/0b", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 5}
	batch := testBatch(entity.BatchStateSpeculating, depCancelled.ID, depSucceeded.ID)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), depCancelled.ID).Return(depCancelled, nil)
	batchStore.EXPECT().Get(gomock.Any(), depSucceeded.ID).Return(depSucceeded, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
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
// publish was lost. State must not change (no UpdateState). The Cancelled
// terminal also re-fans-out dependents and is covered separately in
// TestController_Process_CancelledTerminalSelfHealsDependents.
func TestController_Process_TerminalSelfHeals(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
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

// Cancelled is terminal: redelivery must re-fan-out dependents (so a crash
// between the terminal CAS and the dependent publish does not strand them)
// AND re-publish to conclude. State must not change (no UpdateState; no
// build cancel). The BuildStore must not be touched on this self-heal path.
func TestController_Process_CancelledTerminalSelfHealsDependents(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateCancelled)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// No UpdateState expected.

	depStore := storagemock.NewMockBatchDependentStore(ctrl)
	depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{"test-queue/batch/2", "test-queue/batch/3"},
		Version:    1,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(depStore).AnyTimes()
	// BuildStore must NOT be touched on the terminal self-heal path.

	type pubRec struct {
		topic string
		msgID string
	}
	var records []pubRec
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			records = append(records, pubRec{topic: topic, msgID: msg.ID})
			return nil
		}).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t).Sugar()
	controller := NewController(logger, tally.NoopScope, store, registry, consumer.TopicKeySpeculate, "orchestrator-speculate")

	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))

	assert.Equal(t, []pubRec{
		{topic: "speculate", msgID: "test-queue/batch/2"},
		{topic: "speculate", msgID: "test-queue/batch/3"},
		{topic: "conclude", msgID: batch.ID},
	}, records)
}

// Cancelling drives the terminal-cancellation flow: cancel any in-flight
// build, CAS the batch to Cancelled, fan out dependents, publish to
// conclude. Validates the full happy-path order with a running build and
// a couple of dependents. Order matters: dependents must publish AFTER the
// terminal CAS so the woken dependents observe the dep as Cancelled (and
// drop it from their chain) rather than as still-Cancelling (which would
// leave them waiting on a state nobody is going to nudge).
func TestController_Process_CancellingTerminalFlow(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateCancelling)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{
		ID: batch.ID, BatchID: batch.ID, Status: entity.BuildStatusRunning,
	}, nil)
	buildStore.EXPECT().UpdateStatus(gomock.Any(), batch.ID, entity.BuildStatusCancelled).Return(nil)

	depStore := storagemock.NewMockBatchDependentStore(ctrl)
	depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{"test-queue/batch/2", "test-queue/batch/3"},
		Version:    1,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(depStore).AnyTimes()

	type pubRec struct {
		topic string
		msgID string
	}
	var records []pubRec
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			records = append(records, pubRec{topic: topic, msgID: msg.ID})
			return nil
		}).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t).Sugar()
	controller := NewController(logger, tally.NoopScope, store, registry, consumer.TopicKeySpeculate, "orchestrator-speculate")

	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))

	assert.Equal(t, []pubRec{
		{topic: "speculate", msgID: "test-queue/batch/2"},
		{topic: "speculate", msgID: "test-queue/batch/3"},
		{topic: "conclude", msgID: batch.ID},
	}, records)
}

// If the build for the batch has already reached a terminal status (e.g. CI
// finished naturally between the cancel intent and the speculate pickup), the
// cancellation must not re-flip it — UpdateStatus must never fire. The rest
// of the flow (terminal batch CAS, dependent fan-out, conclude) still runs.
func TestController_Process_CancellingBuildAlreadyTerminal(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateCancelling)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{
		ID: batch.ID, BatchID: batch.ID, Status: entity.BuildStatusSucceeded,
	}, nil)
	// No UpdateStatus expected — the build is already terminal.

	depStore := storagemock.NewMockBatchDependentStore(ctrl)
	depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID: batch.ID, Version: 1,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(depStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
}

// If no Build entity exists for the batch (e.g. cancel arrived before
// speculation started building), the BuildStore.Get NotFound must be
// tolerated and the rest of the cancellation flow must continue.
func TestController_Process_CancellingNoBuildYet(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateCancelling)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{}, storage.ErrNotFound)
	// No UpdateStatus expected.

	depStore := storagemock.NewMockBatchDependentStore(ctrl)
	depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID: batch.ID, Version: 1,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(depStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)
	require.NoError(t, runProcess(t, ctrl, controller, batch.ID))
}

// A batch whose BatchDependent row exists with an empty Dependents list must
// still drive itself to terminal and publish to conclude. This is the normal
// "no dependents" path: the batch controller creates the row with an empty
// list at batch creation time and it stays empty if no later batch conflicts.
func TestController_Process_CancellingNoDependents(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateCancelling)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{}, storage.ErrNotFound)

	depStore := storagemock.NewMockBatchDependentStore(ctrl)
	depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{BatchID: batch.ID, Dependents: []string{}, Version: 1}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(depStore).AnyTimes()

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
}

// storage.ErrVersionMismatch on the terminal CAS must surface as an error
// with the underlying sentinel in the chain so the base controller can
// classify it as retryable. The dependent fan-out and conclude publish must
// NOT run if the terminal CAS failed — on redelivery the self-heal branch
// will pick up the (now-terminal) state and complete the fan-out.
func TestController_Process_CancellingTerminalCASVersionMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := testBatch(entity.BatchStateCancelling)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).
		Return(storage.ErrVersionMismatch)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	// BatchDependentStore must NOT be touched — terminal CAS failed before fan-out.

	// No publish expected (terminal CAS failed before fan-out).
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
			{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	logger := zaptest.NewLogger(t).Sugar()
	controller := NewController(logger, tally.NoopScope, store, registry, consumer.TopicKeySpeculate, "orchestrator-speculate")

	err = runProcess(t, ctrl, controller, batch.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
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

	msg := entityqueue.NewMessage("anything", []byte("not-json"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.Error(t, controller.Process(context.Background(), delivery))
}
