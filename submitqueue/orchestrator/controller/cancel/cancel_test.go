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

package cancel

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func batchWithState(batch entity.Batch, state entity.BatchState) entity.Batch {
	batch.State = state
	return batch
}

// cancelPayload serializes a CancelRequest to JSON bytes for test message payloads.
func cancelPayload(t *testing.T, id, reason string) []byte {
	payload, err := entity.CancelRequest{ID: id, Reason: reason}.ToBytes()
	require.NoError(t, err)
	return payload
}

// newRegistry builds a registry that registers a no-op publisher for every
// topic the cancel controller may publish to (speculate on the batch path,
// log on the unbatched-request path). Returns the registry plus the
// publisher mock so callers may attach EXPECTs or read captured messages.
func newRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	t.Helper()
	pub := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	reg, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyCancel, Name: "cancel", Queue: q},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: q},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: q},
	})
	require.NoError(t, err)
	return reg, pub
}

func newController(t *testing.T, store storage.Storage, registry consumer.TopicRegistry) *Controller {
	return NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, registry, topickey.TopicKeyCancel, "orchestrator-cancel")
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte, partitionKey string) consumer.Delivery {
	msg := entityqueue.NewMessage("cancel-msg", payload, partitionKey, nil)
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func expectBatchLookup(
	ctrl *gomock.Controller,
	store *storagemock.MockStorage,
	batchStore *storagemock.MockBatchStore,
	requestID string,
	batches ...entity.Batch,
) {
	associations := make([]entity.RequestBatch, 0, len(batches))
	for _, batch := range batches {
		associations = append(associations, entity.RequestBatch{
			RequestID: requestID,
			BatchID:   batch.ID,
			Version:   1,
		})
		batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	}

	requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	requestBatchStore.EXPECT().GetByRequestID(gomock.Any(), requestID).Return(associations, nil)
	store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	controller := newController(t, store, registry)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyCancel, controller.TopicKey())
	assert.Equal(t, "orchestrator-cancel", controller.ConsumerGroup())
	assert.Equal(t, "cancel", controller.Name())

	var _ consumer.Controller = controller
}

func TestProcess_AlreadyTerminal_NoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	// No Publish expectations: idempotent re-delivery does nothing.
	_ = pub

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Queue: "q", State: entity.RequestStateCancelled, Version: 5,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.NoError(t, err)
}

func TestProcess_RequestNotFound_Retryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newRegistry(t, ctrl)

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestProcess_CancelsUnbatchedRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)

	var publishedTopics []string
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, _ entityqueue.Message) error {
			publishedTopics = append(publishedTopics, topic)
			return nil
		}).AnyTimes()

	reqStore := storagemock.NewMockRequestStore(ctrl)
	started := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2}
	cancelling := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateCancelling, Version: 3}
	// Two-step transition: mark Cancelling, then — after TerminateRequest re-reads
	// the request — the terminal Cancelled CAS.
	gomock.InOrder(
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(started, nil),
		reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).Return(nil),
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(cancelling, nil),
		reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateCancelled).Return(nil),
	)

	batchStore := storagemock.NewMockBatchStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, "q/1")

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", "user changed mind"), "q/1"))
	require.NoError(t, err)

	// Only the request log entry should have been published.
	assert.Equal(t, []string{"log"}, publishedTopics)
}

// TestProcess_AlreadyCancelling_SkipsMarkCancelling exercises the idempotent
// re-delivery path for the unbatched-request flow: the prior pass already
// wrote RequestStateCancelling, so the mark-cancelling CAS must be skipped
// and we proceed straight to the batch lookup + terminal CAS.
func TestProcess_AlreadyCancelling_SkipsMarkCancelling(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	reqStore := storagemock.NewMockRequestStore(ctrl)
	cancelling := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateCancelling, Version: 3}
	// Get is called twice: the initial load and TerminateRequest's re-read. Both
	// see Cancelling (the prior pass already recorded intent).
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(cancelling, nil).Times(2)
	// Only the terminal CAS — the mark-cancelling step is a no-op.
	reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateCancelled).Return(nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, "q/1")

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.NoError(t, err)
}

// TestProcess_MarkCancellingVersionMismatch_Retryable covers the case where the
// first CAS (mark-cancelling) loses to a concurrent writer. The underlying
// storage.ErrVersionMismatch must be preserved in the error chain so its
// intrinsic retryable classification survives; the next pass re-fetches and
// re-evaluates (possibly observing a terminal state and acking).
func TestProcess_MarkCancellingVersionMismatch_Retryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	_ = pub

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2,
	}, nil)
	reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).
		Return(storage.ErrVersionMismatch)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
}

func TestProcess_UnbatchedVersionMismatch_Retryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	_ = pub

	reqStore := storagemock.NewMockRequestStore(ctrl)
	started := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2}
	cancelling := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateCancelling, Version: 3}
	gomock.InOrder(
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(started, nil),
		reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).Return(nil),
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(cancelling, nil),
		reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateCancelled).
			Return(storage.ErrVersionMismatch),
	)

	batchStore := storagemock.NewMockBatchStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, "q/1")

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
}

func TestProcess_UnbatchedRequestDiverged_Acks(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	_ = pub

	started := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2}
	landed := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateLanded, Version: 4}
	reqStore := storagemock.NewMockRequestStore(ctrl)
	gomock.InOrder(
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(started, nil),
		reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).Return(nil),
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(landed, nil),
	)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "q", gomock.Any()).Return(nil, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.NoError(t, err)
}

func TestProcess_UnbatchedRequestDisappears_Retryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)
	_ = pub

	started := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2}
	reqStore := storagemock.NewMockRequestStore(ctrl)
	gomock.InOrder(
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(started, nil),
		reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).Return(nil),
		reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, storage.ErrNotFound),
	)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "q", gomock.Any()).Return(nil, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

// TestProcess_BatchPath_HandsOffToSpeculate asserts the entire batch path:
// the request intent CAS runs, the batch intent CAS to Cancelling runs, and
// exactly one publish lands on the speculate topic with the batch ID as the
// message ID. The controller does NOT perform a terminal batch CAS, does
// NOT publish to conclude, and does NOT emit a per-request log on this path
// (the gateway already wrote the Cancelling intent log; conclude writes the
// terminal log when it reconciles request state).
func TestProcess_BatchPath_HandsOffToSpeculate(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)

	type pubRec struct {
		topic string
		msgID string
	}
	var records []pubRec
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			records = append(records, pubRec{topic: topic, msgID: msg.ID})
			return nil
		}).AnyTimes()

	req := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2}
	batch := entity.Batch{
		ID:       "q/batch/1",
		Queue:    "q",
		Contains: []string{"q/1", "q/2"},
		State:    entity.BatchStateSpeculating,
		Version:  3,
	}

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(req, nil)
	reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).Return(nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	// Single batch CAS: intent only. No terminal CAS.
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateCancelling), int32(3), int32(4)).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, req.ID, batch)
	// BatchDependentStore and BuildStore must NOT be touched — speculate owns those now.

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", "stop"), "q/1"))
	require.NoError(t, err)

	assert.Equal(t, []pubRec{{topic: "speculate", msgID: "q/batch/1"}}, records)
}

func TestProcess_CancelsEveryApplicableBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newRegistry(t, ctrl)

	request := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateBatched, Version: 2}
	batch1 := entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{request.ID}, State: entity.BatchStateCreated, Version: 1}
	batch2 := entity.Batch{ID: "q/batch/2", Queue: "q", Contains: []string{request.ID}, State: entity.BatchStateSpeculating, Version: 3}
	terminalBatch := entity.Batch{ID: "q/batch/3", Queue: "q", Contains: []string{request.ID}, State: entity.BatchStateSucceeded, Version: 4}

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(2), int32(3), entity.RequestStateCancelling).Return(nil)

	var operations []string
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch1, entity.BatchStateCancelling), int32(1), int32(2)).DoAndReturn(
		func(context.Context, entity.Batch, int32, int32) error {
			operations = append(operations, "update:"+batch1.ID)
			return nil
		},
	)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch2, entity.BatchStateCancelling), int32(3), int32(4)).DoAndReturn(
		func(context.Context, entity.Batch, int32, int32) error {
			operations = append(operations, "update:"+batch2.ID)
			return nil
		},
	)
	publisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			operations = append(operations, "publish:"+msg.ID)
			return nil
		},
	).Times(2)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, request.ID, terminalBatch, batch2, batch1)

	controller := newController(t, store, registry)
	assert.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, request.ID, ""), request.ID)))
	assert.Equal(t, []string{
		"update:q/batch/1",
		"publish:q/batch/1",
		"update:q/batch/2",
		"publish:q/batch/2",
	}, operations)
}

func TestProcess_BatchFailureDoesNotPreventLaterCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newRegistry(t, ctrl)

	request := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateBatched, Version: 2}
	batch1 := entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{request.ID}, State: entity.BatchStateCreated, Version: 1}
	batch2 := entity.Batch{ID: "q/batch/2", Queue: "q", Contains: []string{request.ID}, State: entity.BatchStateCreated, Version: 2}
	storeErr := fmt.Errorf("storage failed")

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(2), int32(3), entity.RequestStateCancelling).Return(nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch1, entity.BatchStateCancelling), int32(1), int32(2)).Return(storeErr)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch2, entity.BatchStateCancelling), int32(2), int32(3)).Return(nil)
	publisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			assert.Equal(t, batch2.ID, msg.ID)
			return nil
		},
	)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, request.ID, batch2, batch1)

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, request.ID, ""), request.ID))
	assert.ErrorContains(t, err, "failed to mark batch q/batch/1 as cancelling")
}

func TestProcess_NonCancellableBatchSuppressesRequestCancellation(t *testing.T) {
	tests := []struct {
		name  string
		state entity.BatchState
	}{
		{name: "merging", state: entity.BatchStateMerging},
		{name: "succeeded", state: entity.BatchStateSucceeded},
		{name: "failed", state: entity.BatchStateFailed},
		{name: "cancelled", state: entity.BatchStateCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			registry, _ := newRegistry(t, ctrl)

			request := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateBatched, Version: 2}
			batch := entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{request.ID}, State: tt.state, Version: 4}

			requestStore := storagemock.NewMockRequestStore(ctrl)
			requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
			requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(2), int32(3), entity.RequestStateCancelling).Return(nil)

			batchStore := storagemock.NewMockBatchStore(ctrl)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			expectBatchLookup(ctrl, store, batchStore, request.ID, batch)

			controller := newController(t, store, registry)
			assert.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, request.ID, ""), request.ID)))
		})
	}
}

func TestProcess_BatchedWithoutMatchCancelsRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil)

	request := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateBatched, Version: 2}
	requestStore := storagemock.NewMockRequestStore(ctrl)
	gomock.InOrder(
		requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil),
		requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(2), int32(3), entity.RequestStateCancelling).Return(nil),
		requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(3), int32(4), entity.RequestStateCancelled).Return(nil),
	)

	batchStore := storagemock.NewMockBatchStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, request.ID)

	controller := newController(t, store, registry)
	assert.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, request.ID, "stop"), request.ID)))
}

func TestProcess_CreatingBatchDoesNotSuppressRequestCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil)

	request := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateBatched, Version: 2}
	batch := entity.Batch{
		ID:       "q/batch/1",
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreating,
		Version:  1,
	}

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(2), int32(3), entity.RequestStateCancelling).Return(nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), request.ID, int32(3), int32(4), entity.RequestStateCancelled).Return(nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, request.ID, batch)

	controller := newController(t, store, registry)
	assert.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, request.ID, "stop"), request.ID)))
}

// TestProcess_BatchAlreadyCancelling_RepublishesToSpeculate covers the
// idempotent redelivery path on the batch flow: a prior pass wrote the
// intent CAS but the speculate publish failed. The cancel controller must
// observe the Cancelling batch via findBatches, skip the
// intent CAS, and re-publish the batch ID to TopicKeySpeculate so the
// speculate controller can pick the work up.
func TestProcess_BatchAlreadyCancelling_RepublishesToSpeculate(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newRegistry(t, ctrl)

	var publishedTopics []string
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, _ entityqueue.Message) error {
			publishedTopics = append(publishedTopics, topic)
			return nil
		}).AnyTimes()

	// Request is already in RequestStateCancelling from the prior pass; the
	// batch is in BatchStateCancelling from the same prior pass.
	req := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateCancelling, Version: 3}
	batch := entity.Batch{
		ID:       "q/batch/1",
		Queue:    "q",
		Contains: []string{"q/1"},
		State:    entity.BatchStateCancelling,
		Version:  4,
	}

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(req, nil)
	// No request UpdateState — already in Cancelling.

	batchStore := storagemock.NewMockBatchStore(ctrl)
	// No batch Update — already in Cancelling.

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, req.ID, batch)

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.NoError(t, err)

	assert.Equal(t, []string{"speculate"}, publishedTopics)
}

// TestProcess_BatchIntentVersionMismatch_Retryable covers the case where the
// intent CAS (mark batch Cancelling) loses to a concurrent batch state
// transition (e.g. speculate just advanced it). storage.ErrVersionMismatch
// must be preserved so its intrinsic retryable classification survives.
func TestProcess_BatchIntentVersionMismatch_Retryable(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newRegistry(t, ctrl)

	req := entity.Request{ID: "q/1", Queue: "q", State: entity.RequestStateStarted, Version: 2}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{"q/1"}, State: entity.BatchStateCreated, Version: 1}

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(req, nil)
	reqStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateCancelling).Return(nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateCancelling), int32(1), int32(2)).
		Return(storage.ErrVersionMismatch)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchLookup(ctrl, store, batchStore, req.ID, batch)

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
}

func TestProcess_DeserializeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newRegistry(t, ctrl)

	store := storagemock.NewMockStorage(ctrl)
	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, []byte("not json"), "q/1"))
	require.Error(t, err)
}

func TestProcess_RequestStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newRegistry(t, ctrl)

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, fmt.Errorf("db down"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	controller := newController(t, store, registry)
	err := controller.Process(context.Background(), newDelivery(t, ctrl, cancelPayload(t, "q/1", ""), "q/1"))
	require.Error(t, err)
}

func TestFindBatches(t *testing.T) {
	request := entity.Request{ID: "q/1", Queue: "q"}
	batch1 := entity.Batch{ID: "q/batch/1", Contains: []string{request.ID}}
	batch2 := entity.Batch{ID: "q/batch/2", Contains: []string{"q/other", request.ID}}
	storeErr := fmt.Errorf("storage failed")

	tests := []struct {
		name     string
		mockFunc func(*storagemock.MockRequestBatchStore, *storagemock.MockBatchStore)
		want     []entity.Batch
		errMsg   string
	}{
		{
			name: "association lookup failure",
			mockFunc: func(store *storagemock.MockRequestBatchStore, _ *storagemock.MockBatchStore) {
				store.EXPECT().GetByRequestID(gomock.Any(), request.ID).Return(nil, storeErr)
			},
			errMsg: "failed to get batch associations",
		},
		{
			name: "stale association is ignored",
			mockFunc: func(store *storagemock.MockRequestBatchStore, batchStore *storagemock.MockBatchStore) {
				store.EXPECT().GetByRequestID(gomock.Any(), request.ID).Return([]entity.RequestBatch{{
					RequestID: request.ID,
					BatchID:   "q/batch/missing",
					Version:   1,
				}}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "q/batch/missing").Return(entity.Batch{}, storage.ErrNotFound)
			},
		},
		{
			name: "batch lookup failure",
			mockFunc: func(store *storagemock.MockRequestBatchStore, batchStore *storagemock.MockBatchStore) {
				store.EXPECT().GetByRequestID(gomock.Any(), request.ID).Return([]entity.RequestBatch{{
					RequestID: request.ID,
					BatchID:   batch1.ID,
					Version:   1,
				}}, nil)
				batchStore.EXPECT().Get(gomock.Any(), batch1.ID).Return(entity.Batch{}, storeErr)
			},
			errMsg: "failed to get associated batch",
		},
		{
			name: "batches are sorted",
			mockFunc: func(store *storagemock.MockRequestBatchStore, batchStore *storagemock.MockBatchStore) {
				store.EXPECT().GetByRequestID(gomock.Any(), request.ID).Return([]entity.RequestBatch{
					{RequestID: request.ID, BatchID: batch2.ID, Version: 1},
					{RequestID: request.ID, BatchID: batch1.ID, Version: 1},
				}, nil)
				batchStore.EXPECT().Get(gomock.Any(), batch2.ID).Return(batch2, nil)
				batchStore.EXPECT().Get(gomock.Any(), batch1.ID).Return(batch1, nil)
			},
			want: []entity.Batch{batch1, batch2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batchStore := storagemock.NewMockBatchStore(ctrl)
			requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
			tt.mockFunc(requestBatchStore, batchStore)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestBatchStore().Return(requestBatchStore)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			controller := &Controller{metricsScope: tally.NoopScope, store: store}
			got, err := controller.findBatches(context.Background(), request)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
