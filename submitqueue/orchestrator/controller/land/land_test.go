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

package land

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	strategypb "github.com/uber/submitqueue/api/base/landstrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/landstrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
)

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, batchID, partitionKey string) *consumermock.MockDelivery {
	msg := entityqueue.NewMessage(batchID, batchIDPayload(t, batchID), partitionKey, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

func newController(t *testing.T, store *storagemock.MockStorage, registry consumer.TopicRegistry) *Controller {
	return NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		staticStorageFactory{store: store},
		registry,
		runwaymq.TopicKeyLand,
		topickey.TopicKeyLand,
		"orchestrator-land",
	)
}

// publishes records what a controller run published, in order and by topic. The
// controller writes to two topics now — the request-log fan-out and the runway
// land request — so tests need to tell them apart and to see which came first.
type publishes struct {
	inOrder []string
	byTopic map[string][]entityqueue.Message
}

// newRegistry builds a registry carrying both topics this controller publishes
// to, recording every message. failTopic names a topic whose publishes fail;
// empty means they all succeed.
func newRegistry(t *testing.T, ctrl *gomock.Controller, failTopic string) (consumer.TopicRegistry, *publishes) {
	rec := &publishes{byTopic: map[string][]entityqueue.Message{}}

	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			if topic == failTopic {
				return fmt.Errorf("enqueue failed")
			}
			rec.inOrder = append(rec.inOrder, topic)
			rec.byTopic[topic] = append(rec.byTopic[topic], msg)
			return nil
		},
	).AnyTimes()

	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyLand, Name: "runway-land", Queue: q},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: q},
	})
	require.NoError(t, err)
	return registry, rec
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyLand, Name: "runway-land", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)

	require.NotNil(t, c)
	assert.Equal(t, topickey.TopicKeyLand, c.TopicKey())
	assert.Equal(t, "orchestrator-land", c.ConsumerGroup())
	assert.Equal(t, "land", c.Name())
	var _ consumer.Controller = c
}

func TestProcess_PublishesFullPayloadToRunway(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/1"
	req1 := entity.Request{
		ID:           "test-queue/1",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/repo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		LandStrategy: landstrategy.StrategySquashRebase,
	}
	req2 := entity.Request{
		ID:           "test-queue/2",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/repo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
		LandStrategy: landstrategy.StrategyRebase,
	}
	batch := entity.Batch{
		ID:       batchID,
		Queue:    "test-queue",
		Contains: []string{req1.ID, req2.ID},
		State:    entity.BatchStateLanding,
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
			if topic != "runway-land" {
				return nil
			}
			gotTopic = topic
			gotPayload = msg.Payload
			return nil
		},
	).AnyTimes()
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyLand, Name: "runway-land", Queue: q},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: q},
	})
	require.NoError(t, err)

	c := newController(t, store, registry)
	require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))

	// Full payload published to runway, keyed by the batch id (the correlation id).
	assert.Equal(t, "runway-land", gotTopic)
	got := &runwaymq.LandRequest{}
	require.NoError(t, runwaymq.Unmarshal(gotPayload, got))
	assert.Equal(t, batch.ID, got.Id)
	assert.Equal(t, batch.Queue, got.QueueName)
	require.Len(t, got.Steps, 2)
	// One step per member request, in Contains order, attributed by request id.
	assert.Equal(t, req1.ID, got.Steps[0].StepId)
	require.NotNil(t, got.Steps[0].Change)
	assert.Equal(t, req1.Change.URIs, got.Steps[0].Change.Uris)
	assert.Equal(t, strategypb.Strategy_SQUASH_REBASE, got.Steps[0].Strategy)
	assert.Equal(t, req2.ID, got.Steps[1].StepId)
	require.NotNil(t, got.Steps[1].Change)
	assert.Equal(t, req2.Change.URIs, got.Steps[1].Change.Uris)
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

			// No request-store reads and no publish for a halted batch: the
			// members are told nothing and runway is not asked to land.
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

			registry, rec := newRegistry(t, ctrl, "")

			c := newController(t, store, registry)
			require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))
			assert.Empty(t, rec.inOrder)
		})
	}
}

// TestProcess_ReportsLandingBeforeDispatch covers the request-log half of this
// stage: every member of the batch is told it is landing, and it is told before
// the land request goes out. The ordering is what makes a lost log entry
// recoverable — a failure here nacks with nothing announced to runway, and the
// redelivery re-publishes under the same occurrence, which the queue dedupes.
func TestProcess_ReportsLandingBeforeDispatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	const batchID = "test-queue/batch/3"
	req1 := entity.Request{ID: "test-queue/1", Queue: "test-queue", LandStrategy: landstrategy.StrategyRebase}
	req2 := entity.Request{ID: "test-queue/2", Queue: "test-queue", LandStrategy: landstrategy.StrategyRebase}
	batch := entity.Batch{
		ID: batchID, Queue: "test-queue",
		Contains: []string{req1.ID, req2.ID},
		State:    entity.BatchStateLanding, Version: 2,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), req1.ID).Return(req1, nil)
	reqStore.EXPECT().Get(gomock.Any(), req2.ID).Return(req2, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	registry, rec := newRegistry(t, ctrl, "")
	c := newController(t, store, registry)
	require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))

	// One entry per member, then the dispatch.
	assert.Equal(t, []string{"log", "log", "runway-land"}, rec.inOrder)

	logs := rec.byTopic["log"]
	require.Len(t, logs, 2)
	for i, requestID := range []string{req1.ID, req2.ID} {
		entry, err := entity.RequestLogFromBytes(logs[i].Payload)
		require.NoError(t, err)
		assert.Equal(t, requestID, entry.RequestID)
		assert.Equal(t, entity.RequestStatusLanding, entry.Status)
		assert.Equal(t, batch.ID, entry.Metadata["batch_id"])
		// Partitioned per request so its entries stay ordered, and scoped to the
		// batch so a redelivery of this message dedupes.
		assert.Equal(t, requestID, logs[i].PartitionKey)
		assert.Equal(t, requestID+"/landing/"+batch.ID, logs[i].ID)
	}
}

func TestProcess_PublishFailureReturnsError(t *testing.T) {
	tests := []struct {
		name      string
		failTopic string
	}{
		{name: "runway dispatch fails", failTopic: "runway-land"},
		{name: "request log fails", failTopic: "log"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			const batchID = "test-queue/batch/2"
			req := entity.Request{ID: "test-queue/1", Queue: "test-queue", LandStrategy: landstrategy.StrategyRebase}
			batch := entity.Batch{ID: batchID, Queue: "test-queue", Contains: []string{req.ID}, State: entity.BatchStateLanding, Version: 1}

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batchID).Return(batch, nil)
			reqStore := storagemock.NewMockRequestStore(ctrl)
			reqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil).AnyTimes()

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

			registry, rec := newRegistry(t, ctrl, tt.failTopic)

			c := newController(t, store, registry)
			require.Error(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))

			// A failed log publish must stop the run before runway hears about
			// the land, so the redelivery can repair the log entry.
			if tt.failTopic == "log" {
				assert.Empty(t, rec.byTopic["runway-land"])
			}
		})
	}
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
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyLand, Name: "runway-land", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)
	err = c.Process(context.Background(), newDelivery(t, ctrl, batchID, "test-queue"))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}
