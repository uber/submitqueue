// Copyright (c) 2026 Uber Technologies, Inc.
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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	queue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	orchstoragemock "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func concludeDelivery(t *testing.T, ctrl *gomock.Controller, reason string) consumer.Delivery {
	t.Helper()
	payload, err := sqmq.MarshalID(sqmq.TopicKeyConclude, "9", "q")
	require.NoError(t, err)
	message := queue.NewMessage("conclude", payload, "q", map[string]string{topickey.MetadataKeyFailureReason: reason})
	message.Tenant = "q"
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(message).AnyTimes()
	delivery.EXPECT().Metadata().Return(map[string]string{"dlq.last_error": "conclude database unavailable"}).AnyTimes()
	return delivery
}

func TestDLQConcludeController_TerminalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		batchState   entity.BatchState
		requestState entity.RequestState
		status       entity.RequestStatus
		lastError    string
	}{
		{entity.BatchStateSucceeded, entity.RequestStateLanded, entity.RequestStatusLanded, ""},
		{entity.BatchStateFailed, entity.RequestStateError, entity.RequestStatusError, "merge conflict"},
		{entity.BatchStateCancelled, entity.RequestStateCancelled, entity.RequestStatusCancelled, ""},
	} {
		t.Run(string(tt.batchState), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			batch := entity.Batch{ID: "9", Queue: "q", State: tt.batchState, Version: 3, Contains: []string{"1"}}
			batches := storagemock.NewMockBatchStore(ctrl)
			batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			memberships := storagemock.NewMockQueueBatchStateStore(ctrl)
			memberships.EXPECT().Put(gomock.Any(), entity.QueueBatchState{Queue: "q", State: tt.batchState, BatchID: "9"}).Return(nil)
			request := entity.Request{ID: "1", Queue: "q", State: entity.RequestStateProcessing, Version: 2}
			requests := storagemock.NewMockRequestStore(ctrl)
			requests.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
			requests.EXPECT().Update(gomock.Any(), requestWithState(request, tt.requestState), int32(2), int32(3)).Return(nil)
			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batches)
			store.EXPECT().GetQueueBatchStateStore().Return(memberships)
			store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
			registry := newTestLogRegistry(t, ctrl, 1, func(log entity.RequestLog) error {
				assert.Equal(t, tt.status, log.Status)
				assert.Equal(t, tt.lastError, log.LastError)
				assert.Equal(t, "9", log.Metadata["batch_id"])
				return nil
			})
			controller := NewDLQConcludeController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store}, registry, TopicKey(topickey.TopicKeyConclude), "conclude-dlq")
			require.NoError(t, controller.Process(context.Background(), concludeDelivery(t, ctrl, "merge conflict")))
		})
	}
}

func TestDLQConcludeController_ReplaysPartialFanout(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := entity.Batch{ID: "9", Queue: "q", State: entity.BatchStateFailed, Version: 3, Contains: []string{"1", "2", "3"}}
	batches := storagemock.NewMockBatchStore(ctrl)
	batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).Times(2)
	memberships := storagemock.NewMockQueueBatchStateStore(ctrl)
	memberships.EXPECT().Put(gomock.Any(), entity.QueueBatchState{Queue: "q", State: batch.State, BatchID: "9"}).Return(nil).Times(2)
	requests := storagemock.NewMockRequestStore(ctrl)
	for _, id := range []string{"1", "2"} {
		request := entity.Request{ID: id, Queue: "q", State: entity.RequestStateProcessing, Version: 1}
		var persisted entity.Request
		gomock.InOrder(
			requests.EXPECT().Get(gomock.Any(), id).Return(request, nil),
			requests.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).DoAndReturn(
				func(_ context.Context, updated entity.Request, _, version int32) error {
					persisted = updated
					persisted.Version = version
					return nil
				}),
			requests.EXPECT().Get(gomock.Any(), id).DoAndReturn(func(context.Context, string) (entity.Request, error) { return persisted, nil }),
		)
	}
	requests.EXPECT().Get(gomock.Any(), "3").Return(entity.Request{ID: "3", Queue: "q", State: entity.RequestStateCancelled, Version: 7}, nil)
	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batches).AnyTimes()
	store.EXPECT().GetQueueBatchStateStore().Return(memberships).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
	var logs []entity.RequestLog
	publishErr := errors.New("log unavailable")
	registry := newTestLogRegistry(t, ctrl, 4, func(log entity.RequestLog) error {
		logs = append(logs, log)
		if len(logs) == 2 {
			return publishErr
		}
		return nil
	})
	controller := NewDLQConcludeController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store}, registry, TopicKey(topickey.TopicKeyConclude), "conclude-dlq")
	require.ErrorIs(t, controller.Process(context.Background(), concludeDelivery(t, ctrl, "merge conflict")), publishErr)
	require.NoError(t, controller.Process(context.Background(), concludeDelivery(t, ctrl, "later infrastructure error")))
	require.Len(t, logs, 4)
	for i, requestID := range []string{"1", "2", "1", "2"} {
		assert.Equal(t, requestID, logs[i].RequestID)
		assert.Equal(t, entity.RequestStatusError, logs[i].Status)
		assert.Equal(t, int32(2), logs[i].RequestVersion)
	}
}
