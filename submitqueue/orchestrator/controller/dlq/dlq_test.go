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
	requestcore "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// reconcileRequest

func TestReconcileRequest_TerminalStates(t *testing.T) {
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

			err := reconcileRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", testErrorOutcome())
			require.NoError(t, err)
		})
	}
}

// TestReconcileRequest_CancellingTransitionsToError verifies that a request stuck in
// the non-terminal Cancelling state is reconciled to Error. If reconcileRequest
// short-circuited on Cancelling the request would remain in-progress forever,
// because the cancel pipeline that owns the Cancelling → Cancelled transition
// has itself died (that's why we're in the DLQ).
func TestReconcileRequest_CancellingTransitionsToError(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 7, State: entity.RequestStateCancelling,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(7), int32(8), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(l entity.RequestLog) error {
		assert.Equal(t, "q/1", l.RequestID)
		assert.Equal(t, entity.RequestStatusError, l.Status)
		assert.Equal(t, int32(8), l.RequestVersion)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := reconcileRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", testErrorOutcome())
	require.NoError(t, err)
}

func TestReconcileRequest_TransitionsToError(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateValidated,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(l entity.RequestLog) error {
		assert.Equal(t, "q/1", l.RequestID)
		assert.Equal(t, entity.RequestStatusError, l.Status)
		assert.Equal(t, int32(4), l.RequestVersion)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := reconcileRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", testErrorOutcome())
	require.NoError(t, err)
}

// TestReconcileRequest_LogPublishErrorPropagates verifies that a terminal log
// publish failure is surfaced so the always-retryable processor redelivers the
// DLQ message.
func TestReconcileRequest_LogPublishErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateValidated,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return fmt.Errorf("publish boom")
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := reconcileRequest(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/1", testErrorOutcome())
	require.Error(t, err)
}

func TestReconcileRequest_NotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := reconcileRequest(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/1", testErrorOutcome())
	require.NoError(t, err)
}

func TestReconcileRequest_GenericGetErrorIsNonRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, fmt.Errorf("boom"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := reconcileRequest(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/1", testErrorOutcome())
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

// failBatch

func TestFailBatch_TransitionsAndFansOut(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: []string{"q/1", "q/2"},
		State: entity.BatchStateMerging, Version: 4,
	}, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), "q/batch/1", int32(4), int32(5), entity.BatchStateFailed).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 2, State: entity.RequestStateProcessing,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateError).Return(nil)
	requestStore.EXPECT().Get(gomock.Any(), "q/2").Return(entity.Request{
		ID: "q/2", Version: 1, State: entity.RequestStateProcessing,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/2", int32(1), int32(2), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 2, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failBatch(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/batch/1", nil)
	require.NoError(t, err)
}

func TestFailBatch_FailedFansOutForRepair(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: []string{"q/1"},
		State: entity.BatchStateFailed, Version: 5,
	}, nil)
	// no batchStore.UpdateState expected

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 2, State: entity.RequestStateProcessing,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failBatch(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/batch/1", nil)
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
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			err := failBatch(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/batch/1", nil)
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
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: []string{"q/1"},
		State: entity.BatchStateCancelling, Version: 6,
	}, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), "q/batch/1", int32(6), int32(7), entity.BatchStateFailed).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateCancelling,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failBatch(context.Background(), store, registry, zaptest.NewLogger(t).Sugar(), "q/batch/1", nil)
	require.NoError(t, err)
}

func TestFailBatch_NotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	err := failBatch(context.Background(), store, consumer.TopicRegistry{}, zaptest.NewLogger(t).Sugar(), "q/batch/1", nil)
	require.NoError(t, err)
}

func TestConcludeBatch_PreservesOutcome(t *testing.T) {
	tests := []struct {
		state      entity.BatchState
		wantState  entity.RequestState
		wantStatus entity.RequestStatus
		wantErr    bool
	}{
		{state: entity.BatchStateSucceeded, wantState: entity.RequestStateLanded, wantStatus: entity.RequestStatusLanded},
		{state: entity.BatchStateFailed, wantState: entity.RequestStateError, wantStatus: entity.RequestStatusError},
		{state: entity.BatchStateCancelled, wantState: entity.RequestStateCancelled, wantStatus: entity.RequestStatusCancelled},
		{state: entity.BatchStateMerging, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{
				ID: "q/batch/1", Contains: []string{"q/1"}, State: tt.state,
			}, nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore)
			registry := consumer.TopicRegistry{}
			if !tt.wantErr {
				requestStore := storagemock.NewMockRequestStore(ctrl)
				requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
					ID: "q/1", State: entity.RequestStateProcessing, Version: 2,
				}, nil)
				requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(2), int32(3), tt.wantState).Return(nil)
				store.EXPECT().GetRequestStore().Return(requestStore)
				registry = newTestLogRegistry(t, ctrl, 1, func(log entity.RequestLog) error {
					assert.Equal(t, tt.wantStatus, log.Status)
					assert.Equal(t, map[string]string{"batch_id": "q/batch/1"}, log.Metadata)
					return nil
				})
			}

			err := concludeBatch(
				context.Background(),
				store,
				registry,
				zaptest.NewLogger(t).Sugar(),
				"q/batch/1",
				nil,
			)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func testErrorOutcome() requestcore.TerminalOutcome {
	return requestcore.TerminalOutcome{State: entity.RequestStateError}
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
