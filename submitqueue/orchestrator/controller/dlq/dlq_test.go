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

package dlq

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newQueueBatchStateStore returns a QueueBatchStateStore mock that accepts any
// membership-record write; these tests never list record buckets.
// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

// noBatchAssociations answers the owning-batch lookup with nothing, i.e. no
// batch ever enrolled the request.
func noBatchAssociations(ctrl *gomock.Controller) *storagemock.MockRequestBatchStore {
	s := storagemock.NewMockRequestBatchStore(ctrl)
	s.EXPECT().GetByRequestID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return s
}

func newQueueBatchStateStore(ctrl *gomock.Controller) *storagemock.MockQueueBatchStateStore {
	s := storagemock.NewMockQueueBatchStateStore(ctrl)
	s.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s
}

func batchWithState(batch entity.Batch, state entity.BatchState) entity.Batch {
	batch.State = state
	return batch
}

func requestWithState(request entity.Request, state entity.RequestState) entity.Request {
	request.State = state
	return request
}

// failRequest

func TestFailRequest_TerminalStates(t *testing.T) {
	tests := []struct {
		state   entity.RequestState
		wantLog bool
	}{
		{state: entity.RequestStateLanded},
		{state: entity.RequestStateError, wantLog: true},
		{state: entity.RequestStateCancelled},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			requestStore := storagemock.NewMockRequestStore(ctrl)
			requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
				ID: "q/1", Version: 5, State: tt.state,
			}, nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
			registry := consumer.TopicRegistry{}
			if tt.wantLog {
				registry = newTestLogRegistry(t, ctrl, 1, func(l entity.RequestLog) error {
					assert.Equal(t, "q/1", l.RequestID)
					assert.Equal(t, entity.RequestStatusError, l.Status)
					assert.Equal(t, int32(5), l.RequestVersion)
					return nil
				})
			}

			err := failRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", "", nil)
			require.NoError(t, err)
		})
	}
}

// TestFailRequest_CancellingTransitionsToError verifies that a request stuck in
// the non-terminal Cancelling state is reconciled to Error. If failRequest
// short-circuited on Cancelling the request would remain in-progress forever,
// because the cancel pipeline that owns the Cancelling → Cancelled transition
// has itself died (that's why we're in the DLQ).
func TestFailRequest_CancellingTransitionsToError(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "q/1", Version: 7, State: entity.RequestStateCancelling,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(7), int32(8)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(l entity.RequestLog) error {
		assert.Equal(t, "q/1", l.RequestID)
		assert.Equal(t, entity.RequestStatusError, l.Status)
		assert.Equal(t, int32(8), l.RequestVersion)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", "", nil)
	require.NoError(t, err)
}

func TestFailRequest_TransitionsToError(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateValidated,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(3), int32(4)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(l entity.RequestLog) error {
		assert.Equal(t, "q/1", l.RequestID)
		assert.Equal(t, entity.RequestStatusError, l.Status)
		assert.Equal(t, int32(4), l.RequestVersion)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", "", nil)
	require.NoError(t, err)
}

// TestFailRequest_LogPublishErrorPropagates verifies that a terminal log
// publish failure is surfaced so the always-retryable processor redelivers the
// DLQ message.
func TestFailRequest_LogPublishErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateValidated,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(3), int32(4)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return fmt.Errorf("publish boom")
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", "", nil)
	require.Error(t, err)
}

func TestFailRequest_NotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/1", "", nil)
	require.NoError(t, err)
}

func TestFailRequest_GenericGetErrorIsNonRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, fmt.Errorf("boom"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/1", "", nil)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

// failBatch

func TestFailBatch_TransitionsAndFansOut(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batch := entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: []string{"q/1", "q/2"},
		State: entity.BatchStateMerging, Version: 4,
	}
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateFailed), int32(4), int32(5)).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request1 := entity.Request{
		ID: "q/1", Version: 2, State: entity.RequestStateProcessing,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request1, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request1, entity.RequestStateError), int32(2), int32(3)).Return(nil)
	request2 := entity.Request{
		ID: "q/2", Version: 1, State: entity.RequestStateProcessing,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/2").Return(request2, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request2, entity.RequestStateError), int32(1), int32(2)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 2, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	_, err := failBatch(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/batch/1", "", nil)
	require.NoError(t, err)
}

func TestFailBatch_FailedFansOutForRepair(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: []string{"q/1"},
		State: entity.BatchStateFailed, Version: 5,
	}, nil)
	// no batchStore.Update expected

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "q/1", Version: 2, State: entity.RequestStateProcessing,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(2), int32(3)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	_, err := failBatch(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/batch/1", "", nil)
	require.NoError(t, err)
}

func TestFailBatch_DifferentTerminalOutcomeSkipsFanOut(t *testing.T) {
	for _, state := range []entity.BatchState{entity.BatchStateSucceeded, entity.BatchStateCancelled} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{
				ID: "q/batch/1", Queue: "q", Contains: []string{"q/1"}, State: state, Version: 5,
			}, nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			_, err := failBatch(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/batch/1", "", nil)
			require.NoError(t, err)
		})
	}
}

// TestFailBatch_CancellingTransitionsToFailed verifies that a batch stuck in
// the non-terminal Cancelling state is reconciled to Failed and its member
// requests are driven from Cancelling to Error. Same rationale as
// TestFailRequest_CancellingTransitionsToError: the cancel pipeline that owns
// the Cancelling → Cancelled transition has died, so DLQ must converge the
// batch and its members to a terminal state.
func TestFailBatch_CancellingTransitionsToFailed(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batch := entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: []string{"q/1"},
		State: entity.BatchStateCancelling, Version: 6,
	}
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateFailed), int32(6), int32(7)).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateCancelling,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(3), int32(4)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	_, err := failBatch(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/batch/1", "", nil)
	require.NoError(t, err)
}

func TestFailBatch_NotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	_, err := failBatch(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/batch/1", "", nil)
	require.NoError(t, err)
}

// TopicKey

func TestDLQTopicKey(t *testing.T) {
	assert.Equal(t, "start_dlq", string(TopicKey("start")))
	assert.Equal(t, "buildsignal_dlq", string(TopicKey("buildsignal")))
}

// Helper to build a tally scope shared across tests.
func testScope() tally.Scope {
	return tally.NoopScope
}
