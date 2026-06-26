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

	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
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

func newController(t *testing.T, store *storagemock.MockStorage, registry consumer.TopicRegistry) *Controller {
	return NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		registry,
		runwaymq.TopicKeyMerge,
		topickey.TopicKeyMerge,
		"orchestrator-merge",
	)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)

	require.NotNil(t, c)
	assert.Equal(t, topickey.TopicKeyMerge, c.TopicKey())
	assert.Equal(t, "orchestrator-merge", c.ConsumerGroup())
	assert.Equal(t, "merge", c.Name())
	var _ consumer.Controller = c
}

func TestProcess_PublishesFullPayloadToRunway(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/1"
	req1 := entity.Request{
		ID:           "test-queue/1",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://uber/repo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		LandStrategy: mergestrategy.MergeStrategySquashRebase,
	}
	req2 := entity.Request{
		ID:           "test-queue/2",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://uber/repo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
	}
	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{req1.ID, req2.ID},
		State:    entity.BatchStateMerging,
		Version:  4,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), req1.ID).Return(req1, nil)
	reqStore.EXPECT().Get(gomock.Any(), req2.ID).Return(req2, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	var gotTopic string
	var gotPayload []byte
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			gotTopic = topic
			gotPayload = msg.Payload
			return nil
		},
	)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)
	require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))

	// Full payload published to runway, keyed by the batch id (the correlation id).
	assert.Equal(t, "merge", gotTopic)
	got := &runwaymq.MergeRequest{}
	require.NoError(t, runwaymq.Unmarshal(gotPayload, got))
	assert.Equal(t, batch.ID, got.Id)
	assert.Equal(t, batch.Queue, got.QueueName)
	require.Len(t, got.Steps, 2)
	// One step per member request, in Contains order, attributed by request id.
	assert.Equal(t, req1.ID, got.Steps[0].StepId)
	require.Len(t, got.Steps[0].Changes, 1)
	assert.Equal(t, req1.Change.URIs, got.Steps[0].Changes[0].Uris)
	assert.Equal(t, strategypb.Strategy_SQUASH_REBASE, got.Steps[0].Strategy)
	assert.Equal(t, req2.ID, got.Steps[1].StepId)
	require.Len(t, got.Steps[1].Changes, 1)
	assert.Equal(t, req2.Change.URIs, got.Steps[1].Changes[0].Uris)
	assert.Equal(t, strategypb.Strategy_REBASE, got.Steps[1].Strategy)
}

func TestProcess_HaltedBatchSkips(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
		entity.BatchStateCancelled,
		entity.BatchStateCancelling,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			const batchID = "test-queue/batch/halted"
			batch := entity.Batch{ID: batchID, Queue: "test-queue", State: state, Version: 7}

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)

			// No request-store reads and no publish for a halted batch: gomock
			// fails if GetRequestStore or Publish is touched.
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			pub := queuemock.NewMockPublisher(ctrl)
			q := queuemock.NewMockQueue(ctrl)
			q.EXPECT().Publisher().Return(pub).AnyTimes()
			registry, err := consumer.NewTopicRegistry(
				[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q}},
			)
			require.NoError(t, err)

			c := newController(t, store, registry)
			require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))
		})
	}
}

func TestProcess_PublishFailureReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/2"
	req := entity.Request{ID: "test-queue/1", Queue: "test-queue", LandStrategy: mergestrategy.MergeStrategyRebase}
	batch := entity.Batch{ID: batchID, Queue: "test-queue", Contains: []string{req.ID}, State: entity.BatchStateMerging, Version: 1}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)
	err = c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue))
	require.Error(t, err)
}

func TestProcess_BatchStoreGetFailureNotRetryable(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/3"

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(entity.Batch{}, fmt.Errorf("db connection lost"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	q := queuemock.NewMockQueue(ctrl)
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)
	err = c.Process(context.Background(), newDelivery(t, ctrl, batchID, "test-queue"))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}
