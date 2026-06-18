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
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// failRequest

func TestFailRequest_TerminalSkipsUpdateButLogsAnyway(t *testing.T) {
	// Truly-terminal states skip the UpdateState CAS but still get a log entry
	// written. The log write runs unconditionally so that a previous attempt
	// which flipped the state but then failed to insert the log is repaired on
	// redelivery. Cancelling is intentionally NOT in this list — see
	// TestFailRequest_CancellingTransitionsToError. DLQ must drive every
	// non-terminal state to a terminal one or the request stays "in progress"
	// forever from the gateway's perspective.
	terminalStates := []entity.RequestState{
		entity.RequestStateLanded,
		entity.RequestStateError,
		entity.RequestStateCancelled,
	}
	for _, state := range terminalStates {
		state := state
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			requestStore := storagemock.NewMockRequestStore(ctrl)
			requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
				ID: "q/1", Version: 5, State: state,
			}, nil)
			// no UpdateState expected — state is already terminal

			logStore := storagemock.NewMockRequestLogStore(ctrl)
			logStore.EXPECT().Insert(gomock.Any(), gomock.AssignableToTypeOf(entity.RequestLog{})).DoAndReturn(func(_ context.Context, l entity.RequestLog) error {
				assert.Equal(t, "q/1", l.RequestID)
				assert.Equal(t, entity.RequestStatusError, l.Status)
				assert.Equal(t, int32(5), l.RequestVersion)
				return nil
			})

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
			store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

			err := failRequest(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/1")
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
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 7, State: entity.RequestStateCancelling,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(7), int32(8), entity.RequestStateError).Return(nil)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.AssignableToTypeOf(entity.RequestLog{})).DoAndReturn(func(_ context.Context, l entity.RequestLog) error {
		assert.Equal(t, "q/1", l.RequestID)
		assert.Equal(t, entity.RequestStatusError, l.Status)
		assert.Equal(t, int32(8), l.RequestVersion)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	err := failRequest(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/1")
	require.NoError(t, err)
}

func TestFailRequest_TransitionsToError(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateValidated,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateError).Return(nil)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.AssignableToTypeOf(entity.RequestLog{})).DoAndReturn(func(_ context.Context, l entity.RequestLog) error {
		assert.Equal(t, "q/1", l.RequestID)
		assert.Equal(t, entity.RequestStatusError, l.Status)
		assert.Equal(t, int32(4), l.RequestVersion)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	err := failRequest(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/1")
	require.NoError(t, err)
}

// TestFailRequest_LogInsertErrorPropagates verifies that a failure to append
// the terminal Error row to the request log is surfaced as an error so the
// always-retryable processor will redeliver the DLQ message. We cannot ack a
// half-reconciled request — the state has flipped to Error but the gateway's
// log-store-derived status would still report the prior non-terminal status.
func TestFailRequest_LogInsertErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 3, State: entity.RequestStateValidated,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(3), int32(4), entity.RequestStateError).Return(nil)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(fmt.Errorf("log store boom"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	err := failRequest(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/1")
	require.Error(t, err)
}

func TestFailRequest_NotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/1")
	require.NoError(t, err)
}

func TestFailRequest_GenericGetErrorIsNonRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{}, fmt.Errorf("boom"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	err := failRequest(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/1")
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

// failBatch

func expectBatchFailedTransition(ctrl *gomock.Controller, store *storagemock.MockStorage, batchID, queue string, oldState entity.BatchState) {
	membershipStore := storagemock.NewMockBatchStateMembershipStore(ctrl)
	membershipStore.EXPECT().Remove(gomock.Any(), queue, oldState, batchID).Return(nil).AnyTimes()
	store.EXPECT().GetBatchStateMembershipStore().Return(membershipStore).AnyTimes()
}

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

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchFailedTransition(ctrl, store, "q/batch/1", "q", entity.BatchStateMerging)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	err := failBatch(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/batch/1")
	require.NoError(t, err)
}

func TestFailBatch_AlreadyTerminalFansOutOnly(t *testing.T) {
	// Already-terminal batch: skip the CAS but still drive the fan-out so a
	// prior crashed attempt that updated the batch but not the requests still
	// reconciles to a clean terminal state.
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

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchFailedTransition(ctrl, store, "q/batch/1", "q", entity.BatchStateCancelling)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	err := failBatch(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/batch/1")
	require.NoError(t, err)
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

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	expectBatchFailedTransition(ctrl, store, "q/batch/1", "q", entity.BatchStateCancelling)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	err := failBatch(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/batch/1")
	require.NoError(t, err)
}

func TestFailBatch_NotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.Batch{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	err := failBatch(context.Background(), store, zaptest.NewLogger(t).Sugar(), "q/batch/1")
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
