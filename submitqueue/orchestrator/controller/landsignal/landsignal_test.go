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

package landsignal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
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

const (
	testBatchID = "test-queue/batch/1"
	testQueue   = "test-queue"
)

func resultPayload(t *testing.T, res runwaymq.LandResult) []byte {
	payload, err := runwaymq.Marshal(&res)
	require.NoError(t, err)
	return payload
}

func newDelivery(ctrl *gomock.Controller, msg entityqueue.Message) *consumermock.MockDelivery {
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

// recordingRegistry returns a registry whose conclude and speculate topics
// share one publisher that records the topic names it is asked to publish to.
func recordingRegistry(t *testing.T, ctrl *gomock.Controller, got *[]string) consumer.TopicRegistry {
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, _ entityqueue.Message) error {
			*got = append(*got, topic)
			return nil
		},
	).AnyTimes()
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: q},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: q},
	})
	require.NoError(t, err)
	return registry
}

func newController(t *testing.T, store *storagemock.MockStorage, registry consumer.TopicRegistry) *Controller {
	return NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		staticStorageFactory{store: store},
		registry,
		runwaymq.TopicKeyLandSignal,
		"orchestrator-landsignal",
	)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	assert.Equal(t, consumer.TopicKey(runwaymq.TopicKeyLandSignal), c.TopicKey())
	assert.Equal(t, "orchestrator-landsignal", c.ConsumerGroup())
	assert.Equal(t, "landsignal", c.Name())
	var _ consumer.Controller = c
}

func TestProcess_LandedAdvancesBatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batch := entity.Batch{
		ID:           testBatchID,
		Queue:        testQueue,
		Contains:     []string{"test-queue/1"},
		Dependencies: []string{"test-queue/batch/0"},
		State:        entity.BatchStateLanding,
		Version:      1,
	}
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateSucceeded), int32(1), int32(2)).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.LandResult{
		Id:      testBatchID,
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps:   []*runwaymq.StepResult{{StepId: "test-queue/1", Outputs: []*runwaymq.StepOutput{{Id: "deadbeef"}}}},
	}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, c.Process(context.Background(), newDelivery(ctrl, msg)))

	// Fans the batch out to conclude (requests pick up the outcome) and
	// speculate (dependents re-plan).
	assert.ElementsMatch(t, []string{"conclude", "speculate"}, got)
}

// The fan-out after a land must not reuse the bare batch ID.
//
// The batch's own announcement to speculate uses exactly that ID, and the
// queue deduplicates on (topic, partition key, message ID) against every row it
// has not collected yet, consumed ones included — a window with no upper bound
// on a busy partition. Reusing the ID here made the wake-up that lets
// dependents re-plan a silent no-op, acked as a success, with nothing to retry
// it.
func TestProcess_FanoutDoesNotCollideWithTheBatchAnnouncement(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batch := entity.Batch{
		ID:           testBatchID,
		Queue:        testQueue,
		Contains:     []string{"test-queue/1"},
		Dependencies: []string{"test-queue/batch/0"},
		State:        entity.BatchStateLanding,
		Version:      1,
	}
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	byTopic := map[string]string{}
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			byTopic[topic] = msg.ID
			return nil
		},
	).AnyTimes()
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: q},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: q},
	})
	require.NoError(t, err)

	res := runwaymq.LandResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, newController(t, store, registry).Process(context.Background(), newDelivery(ctrl, msg)))

	assert.NotEqual(t, testBatchID, byTopic["speculate"],
		"the announcement the batch controller published already holds this ID")
	assert.NotEqual(t, testBatchID, byTopic["conclude"])
	assert.NotEqual(t, byTopic["speculate"], byTopic["conclude"])
}

func TestProcess_NotLandedMarksBatchFailed(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batch := entity.Batch{
		ID:           testBatchID,
		Queue:        testQueue,
		Contains:     []string{"test-queue/1"},
		Dependencies: []string{"test-queue/batch/0"},
		State:        entity.BatchStateLanding,
		Version:      3,
	}
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(batch, nil)
	// The failed transition records only the terminal state; the reason travels
	// to conclude on the message, not on the batch.
	batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateFailed), int32(3), int32(4)).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	byTopic := map[string]entityqueue.Message{}
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			byTopic[topic] = msg
			return nil
		},
	).AnyTimes()
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: q},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: q},
	})
	require.NoError(t, err)

	res := runwaymq.LandResult{Id: testBatchID, Outcome: runwaypb.Outcome_FAILED, Reason: "conflict in foo.go"}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	// Not-landed is an expected terminal outcome, so Process acks (no error).
	require.NoError(t, newController(t, store, registry).Process(context.Background(), newDelivery(ctrl, msg)))

	require.Contains(t, byTopic, "conclude")
	require.Contains(t, byTopic, "speculate")
	// The land failure reason rides the conclude message so conclude can stamp it on
	// the request's terminal log; the speculate wake-up carries none.
	assert.Equal(t, "conflict in foo.go", byTopic["conclude"].Metadata[topickey.MetadataKeyFailureReason])
	assert.Empty(t, byTopic["speculate"].Metadata[topickey.MetadataKeyFailureReason])
}

func TestProcess_CancellingShortCircuit(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(
		entity.Batch{ID: testBatchID, Queue: testQueue, State: entity.BatchStateCancelling, Version: 4}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	// No Update and no fan-out: gomock fails if either runs.
	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.LandResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, c.Process(context.Background(), newDelivery(ctrl, msg)))
	assert.Empty(t, got)
}

func TestProcess_TerminalReFansOut(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Already terminal (a prior delivery won): no state write, but re-fan-out in
	// case the earlier attempt missed the downstream publishes.
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(
		entity.Batch{ID: testBatchID, Queue: testQueue, State: entity.BatchStateSucceeded, Version: 5}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.LandResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, c.Process(context.Background(), newDelivery(ctrl, msg)))
	assert.ElementsMatch(t, []string{"conclude", "speculate"}, got)
}

func TestProcess_DeserializeErrorRejects(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	msg := entityqueue.NewMessage(testBatchID, []byte("garbage"), testQueue, nil)
	require.Error(t, c.Process(context.Background(), newDelivery(ctrl, msg)))
}

func TestProcess_StorageErrorRejects(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(entity.Batch{}, assert.AnError)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.LandResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.Error(t, c.Process(context.Background(), newDelivery(ctrl, msg)))
}
