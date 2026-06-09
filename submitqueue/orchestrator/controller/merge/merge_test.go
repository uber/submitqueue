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

package merge

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/core/errs"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
	pushermock "github.com/uber/submitqueue/submitqueue/extension/pusher/mock"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
)

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, batchID, partitionKey string) *queuemock.MockDelivery {
	msg := entityqueue.NewMessage(batchID, batchIDPayload(t, batchID), partitionKey, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

// newRegistry returns a registry where conclude and speculate accept any publish.
func newRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error) consumer.TopicRegistry {
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ entityqueue.Message) error { return publishErr },
	).AnyTimes()
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
		{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
	})
	require.NoError(t, err)
	return registry
}

// newPusherFactory wraps a Pusher in a factory that returns it for any entityqueue.
func newPusherFactory(ctrl *gomock.Controller, p pusher.Pusher) pusher.Factory {
	f := pushermock.NewMockFactory(ctrl)
	f.EXPECT().For(gomock.Any()).Return(p, nil).AnyTimes()
	return f
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, pushermock.NewMockPusher(ctrl)),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	require.NotNil(t, c)
	assert.Equal(t, consumer.TopicKeyMerge, c.TopicKey())
	assert.Equal(t, "orchestrator-merge", c.ConsumerGroup())
	assert.Equal(t, "merge", c.Name())
	var _ consumer.Controller = c
}

func TestController_Process_SuccessfulMerge(t *testing.T) {
	ctrl := gomock.NewController(t)

	const reqID = "test-queue/1"
	const batchID = "test-queue/batch/1"

	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{reqID},
		State:    entity.BatchStateMerging,
		Version:  4,
	}
	change := entity.Change{URIs: []string{"github://o/r/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
	request := entity.Request{
		ID:      reqID,
		Queue:   "test-queue",
		Change:  change,
		State:   entity.RequestStateProcessing,
		Version: 2,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batchID, int32(4), int32(5), entity.BatchStateSucceeded).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), reqID).Return(request, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	mockPusher := pushermock.NewMockPusher(ctrl)
	mockPusher.EXPECT().Push(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, changes []entity.Change) (pusher.Result, error) {
			require.Len(t, changes, 1)
			assert.Equal(t, change, changes[0])
			return pusher.Result{Outcomes: []pusher.ChangeOutcome{{
				Change:     change,
				Status:     pusher.OutcomeStatusCommitted,
				CommitSHAs: []string{"deadbeef"},
			}}}, nil
		},
	)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, mockPusher),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.NoError(t, err)
}

func TestController_Process_PassesAllChangesInBatchOrder(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/multi"
	requestIDs := []string{"test-queue/1", "test-queue/2", "test-queue/3"}
	changes := []entity.Change{
		{URIs: []string{"github://o/r/pull/1/1111111111111111111111111111111111111111"}},
		{URIs: []string{"github://o/r/pull/2/2222222222222222222222222222222222222222"}},
		{URIs: []string{"github://o/r/pull/3/3333333333333333333333333333333333333333"}},
	}

	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: requestIDs,
		State:    entity.BatchStateMerging,
		Version:  1,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batchID, int32(1), int32(2), entity.BatchStateSucceeded).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	for i, rid := range requestIDs {
		requestStore.EXPECT().Get(gomock.Any(), rid).Return(entity.Request{
			ID: rid, Change: changes[i],
		}, nil)
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	mockPusher := pushermock.NewMockPusher(ctrl)
	mockPusher.EXPECT().Push(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, got []entity.Change) (pusher.Result, error) {
			assert.Equal(t, changes, got, "changes must be in batch.Contains order")
			outcomes := make([]pusher.ChangeOutcome, len(got))
			for i, ch := range got {
				outcomes[i] = pusher.ChangeOutcome{
					Change:     ch,
					Status:     pusher.OutcomeStatusCommitted,
					CommitSHAs: []string{fmt.Sprintf("sha-%d", i)},
				}
			}
			return pusher.Result{Outcomes: outcomes}, nil
		},
	)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, mockPusher),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.NoError(t, err)
}

func TestController_Process_PushConflictMarksBatchFailed(t *testing.T) {
	ctrl := gomock.NewController(t)

	const reqID = "test-queue/2"
	const batchID = "test-queue/batch/2"

	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{reqID},
		State:    entity.BatchStateMerging,
		Version:  3,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batchID, int32(3), int32(4), entity.BatchStateFailed).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), reqID).Return(entity.Request{
		ID: reqID, Change: entity.Change{URIs: []string{"github://o/r/pull/2/2222222222222222222222222222222222222222"}},
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	mockPusher := pushermock.NewMockPusher(ctrl)
	mockPusher.EXPECT().Push(gomock.Any(), gomock.Any()).Return(
		pusher.Result{},
		fmt.Errorf("apply: %w", pusher.ErrConflict),
	)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, mockPusher),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.NoError(t, err, "conflict ack-s the message; failure is recorded on the batch")
}

func TestController_Process_PushInfraFailureReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)

	const reqID = "test-queue/3"
	const batchID = "test-queue/batch/3"

	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{reqID},
		State:    entity.BatchStateMerging,
		Version:  1,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), reqID).Return(entity.Request{
		ID: reqID, Change: entity.Change{URIs: []string{"github://o/r/pull/3/3333333333333333333333333333333333333333"}},
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	mockPusher := pushermock.NewMockPusher(ctrl)
	mockPusher.EXPECT().Push(gomock.Any(), gomock.Any()).Return(
		pusher.Result{},
		fmt.Errorf("ssh: connection refused"),
	)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, mockPusher),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.Error(t, err)
}

func TestController_Process_TerminalBatchSkipsPushButFansOut(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
		entity.BatchStateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			const batchID = "test-queue/batch/4"

			batch := entity.Batch{
				ID:      batchID,
				Queue:   "test-queue",
				State:   state,
				Version: 7,
			}

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			// Push must NOT be called for an already-terminal batch.
			mockPusher := pushermock.NewMockPusher(ctrl)

			c := NewController(
				zaptest.NewLogger(t).Sugar(),
				tally.NoopScope,
				store,
				newRegistry(t, ctrl, nil),
				newPusherFactory(ctrl, mockPusher),
				consumer.TopicKeyMerge,
				"orchestrator-merge",
			)

			err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
			require.NoError(t, err)
		})
	}
}

// BatchStateCancelling must be silently acked: no push, and crucially no
// fan-out (no publish to conclude or speculate). The cancel controller owns
// the terminal write and the downstream publishes; conclude would error on
// a non-terminal Cancelling batch.
func TestController_Process_CancellingShortCircuit(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/4c"

	batch := entity.Batch{
		ID:      batchID,
		Queue:   "test-queue",
		State:   entity.BatchStateCancelling,
		Version: 7,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	// Pusher and publisher with no EXPECTs — neither must be called.
	mockPusher := pushermock.NewMockPusher(ctrl)
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: consumer.TopicKeyConclude, Name: "conclude", Queue: mockQ},
		{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
	})
	require.NoError(t, err)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		registry,
		newPusherFactory(ctrl, mockPusher),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err = c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.NoError(t, err)
}

func TestController_Process_BatchStoreGetFailureNotRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/5"

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(entity.Batch{}, fmt.Errorf("db connection lost"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, pushermock.NewMockPusher(ctrl)),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, "test-queue"))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_RequestStoreFailurePropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	const reqID = "test-queue/6"
	const batchID = "test-queue/batch/6"

	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{reqID},
		State:    entity.BatchStateMerging,
		Version:  1,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), reqID).Return(entity.Request{}, fmt.Errorf("db connection lost"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, nil),
		newPusherFactory(ctrl, pushermock.NewMockPusher(ctrl)),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.Error(t, err)
}

func TestController_Process_PublishFailureSurfaces(t *testing.T) {
	ctrl := gomock.NewController(t)

	const reqID = "test-queue/7"
	const batchID = "test-queue/batch/7"

	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{reqID},
		State:    entity.BatchStateMerging,
		Version:  2,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), batchID, int32(2), int32(3), entity.BatchStateSucceeded).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), reqID).Return(entity.Request{
		ID: reqID, Change: entity.Change{URIs: []string{"github://o/r/pull/7/7777777777777777777777777777777777777777"}},
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	mockPusher := pushermock.NewMockPusher(ctrl)
	mockPusher.EXPECT().Push(gomock.Any(), gomock.Any()).Return(
		pusher.Result{Outcomes: []pusher.ChangeOutcome{{
			Status: pusher.OutcomeStatusCommitted, CommitSHAs: []string{"abc"},
		}}}, nil,
	)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		newRegistry(t, ctrl, fmt.Errorf("queue down")),
		newPusherFactory(ctrl, mockPusher),
		consumer.TopicKeyMerge,
		"orchestrator-merge",
	)

	err := c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.Error(t, err)
}
