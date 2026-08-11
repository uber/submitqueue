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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func newSpeculateController(registry consumer.TopicRegistry, store *storagemock.MockStorage, t *testing.T) consumer.Controller {
	return NewDLQSpeculateController(
		zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store},
		registry, TopicKey(topickey.TopicKeySpeculate), "orchestrator-speculate-dlq",
	)
}

// speculateRegistry captures the batch IDs republished to the speculate topic,
// and counts log publishes so the fan-out stays exercised.
func speculateRegistry(t *testing.T, ctrl *gomock.Controller, logPublishes int, captured *[]string, logs *[]entity.RequestLog) consumer.TopicRegistry {
	logPublisher := queuemock.NewMockPublisher(ctrl)
	logPublisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, message entityqueue.Message) error {
			entry, err := entity.RequestLogFromBytes(message.Payload)
			require.NoError(t, err)
			if logs != nil {
				*logs = append(*logs, entry)
			}
			return nil
		},
	).Times(logPublishes)
	logQueue := queuemock.NewMockQueue(ctrl)
	logQueue.EXPECT().Publisher().Return(logPublisher).Times(logPublishes)

	specPublisher := queuemock.NewMockPublisher(ctrl)
	specPublisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, message entityqueue.Message) error {
			bid, err := entity.BatchIDFromBytes(message.Payload)
			require.NoError(t, err)
			*captured = append(*captured, bid.ID)
			return nil
		},
	).AnyTimes()
	specQueue := queuemock.NewMockQueue(ctrl)
	specQueue.EXPECT().Publisher().Return(specPublisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyLog, Name: "log", Queue: logQueue},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: specQueue},
	})
	require.NoError(t, err)
	return registry
}

// A speculate run covers the whole queue, so the batch that failed is often not
// the batch the message names. The subjects on the failure are what say which,
// and failing the named one regardless would terminate an innocent batch while
// leaving the real culprit running.
func TestDLQSpeculateController_Process_Attribution(t *testing.T) {
	tests := []struct {
		name            string
		recordedFailure failure.Failure
		failed          bool
		wantFailedBatch string
		wantAttribution string
	}{
		{
			name:            "batch subject blames that batch, not the message's",
			recordedFailure: failure.New("read failed", entity.BatchSubject("q/batch/other")),
			failed:          true,
			wantFailedBatch: "q/batch/other",
			wantAttribution: "batch",
		},
		{
			name:            "queue subject falls back to the message's batch",
			recordedFailure: failure.New("speculator failed", entity.QueueSubject("q")),
			failed:          true,
			wantFailedBatch: "q/batch/named",
			wantAttribution: "queue",
		},
		{
			name:            "no subjects falls back and says so",
			recordedFailure: failure.New("exceeded retry limit"),
			failed:          true,
			wantFailedBatch: "q/batch/named",
			wantAttribution: "unattributed",
		},
		{
			name:            "no recorded failure at all falls back",
			wantFailedBatch: "q/batch/named",
			wantAttribution: "unattributed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			blamed := entity.Batch{
				ID: tt.wantFailedBatch, Queue: "q", Contains: []string{"q/1"},
				State: entity.BatchStateSpeculating, Version: 2,
			}
			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), tt.wantFailedBatch).Return(blamed, nil)
			batchStore.EXPECT().Update(gomock.Any(), batchWithState(blamed, entity.BatchStateFailed), int32(2), int32(3)).Return(nil)

			request := entity.Request{ID: "q/1", Version: 1, State: entity.RequestStateProcessing}
			requestStore := storagemock.NewMockRequestStore(ctrl)
			requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
			requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).Return(nil)

			// The queue drained with the blamed batch, so no re-trigger.
			queueBatchState := storagemock.NewMockQueueBatchStateStore(ctrl)
			queueBatchState.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			queueBatchState.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			queueBatchState.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
			store.EXPECT().GetQueueBatchStateStore().Return(queueBatchState).AnyTimes()

			var republished []string
			var logs []entity.RequestLog
			registry := speculateRegistry(t, ctrl, 1, &republished, &logs)
			c := newSpeculateController(registry, store, t)

			payload, err := entity.BatchID{ID: "q/batch/named", Queue: "q"}.ToBytes()
			require.NoError(t, err)

			delivery := newMockDeliveryWithFailure(ctrl, payload, tt.recordedFailure, tt.failed)
			require.NoError(t, c.Process(context.Background(), delivery))

			// The attribution is recorded where a user can see it, so a
			// fallback is never mistaken for a confident answer.
			require.Len(t, logs, 1)
			assert.Equal(t, tt.wantAttribution, logs[0].Metadata["dlq.attribution"])
			assert.Equal(t, "validate", logs[0].Metadata["dlq.original_topic"])
		})
	}
}

// The dead letter that consumed the queue's last edge has to leave one behind,
// or every other batch admitted to speculating sits there with nothing to drive
// it — the stranding this reconciler exists to stop.
func TestDLQSpeculateController_Process_RetriggersQueue(t *testing.T) {
	ctrl := gomock.NewController(t)

	failedBatch := entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: nil,
		State: entity.BatchStateSpeculating, Version: 1,
	}
	// Two still live afterwards; the lower ID is chosen so the wake-up is
	// reproducible despite ListByStates promising no order.
	liveA := entity.Batch{ID: "q/batch/2", Queue: "q", State: entity.BatchStateSpeculating, Version: 1}
	liveB := entity.Batch{ID: "q/batch/3", Queue: "q", State: entity.BatchStateSpeculating, Version: 1}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(failedBatch, nil)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(failedBatch, entity.BatchStateFailed), int32(1), int32(2)).Return(nil)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/3").Return(liveB, nil).AnyTimes()
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/2").Return(liveA, nil).AnyTimes()

	queueBatchState := storagemock.NewMockQueueBatchStateStore(ctrl)
	queueBatchState.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	queueBatchState.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	queueBatchState.EXPECT().List(gomock.Any(), entity.BatchStateSpeculating).Return([]entity.QueueBatchState{
		{Queue: "q", State: entity.BatchStateSpeculating, BatchID: "q/batch/3"},
		{Queue: "q", State: entity.BatchStateSpeculating, BatchID: "q/batch/2"},
	}, nil).AnyTimes()
	queueBatchState.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetQueueBatchStateStore().Return(queueBatchState).AnyTimes()

	var republished []string
	registry := speculateRegistry(t, ctrl, 0, &republished, nil)
	c := newSpeculateController(registry, store, t)

	payload, err := entity.BatchID{ID: "q/batch/1", Queue: "q"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDeliveryWithFailure(ctrl, payload, failure.New("boom"), true)
	require.NoError(t, c.Process(context.Background(), delivery))

	assert.Equal(t, []string{"q/batch/2"}, republished)
}

// Redelivery of a reconcile already done must publish nothing. Republishing
// unconditionally would hand the queue a fresh message every time this message
// came back, and a permanently failing queue would never stop.
func TestDLQSpeculateController_Process_NoRetriggerWithoutProgress(t *testing.T) {
	ctrl := gomock.NewController(t)

	already := entity.Batch{
		ID: "q/batch/1", Queue: "q", Contains: nil,
		State: entity.BatchStateFailed, Version: 4,
	}
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(already, nil)

	queueBatchState := storagemock.NewMockQueueBatchStateStore(ctrl)
	queueBatchState.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	queueBatchState.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	// Live batches remain, so only the progress guard can be what stops it.
	queueBatchState.EXPECT().List(gomock.Any(), entity.BatchStateSpeculating).Return([]entity.QueueBatchState{
		{Queue: "q", State: entity.BatchStateSpeculating, BatchID: "q/batch/2"},
	}, nil).AnyTimes()
	queueBatchState.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetQueueBatchStateStore().Return(queueBatchState).AnyTimes()

	var republished []string
	registry := speculateRegistry(t, ctrl, 0, &republished, nil)
	c := newSpeculateController(registry, store, t)

	payload, err := entity.BatchID{ID: "q/batch/1", Queue: "q"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDeliveryWithFailure(ctrl, payload, failure.New("boom"), true)
	require.NoError(t, c.Process(context.Background(), delivery))

	assert.Empty(t, republished, "an already-failed batch is not progress")
}

func TestDLQSpeculateController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	c := newSpeculateController(consumer.TopicRegistry{}, store, t)

	assert.Equal(t, "speculate_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("speculate_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-speculate-dlq", c.ConsumerGroup())
}

func TestDLQSpeculateController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	c := newSpeculateController(consumer.TopicRegistry{}, store, t)

	delivery := newMockDelivery(ctrl, []byte("garbage"))
	require.Error(t, c.Process(context.Background(), delivery))
}

func TestDLQSpeculateController_Process_EmptyIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	c := newSpeculateController(consumer.TopicRegistry{}, store, t)

	payload, err := entity.BatchID{ID: ""}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.Error(t, c.Process(context.Background(), delivery))
}
