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

package mergesignal

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
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testBatchID = "test-queue/batch/1"
	testQueue   = "test-queue"
)

func resultPayload(t *testing.T, res runwaymq.MergeResult) []byte {
	payload, err := runwaymq.Marshal(&res)
	require.NoError(t, err)
	return payload
}

func newDelivery(ctrl *gomock.Controller, msg entityqueue.Message) *queuemock.MockDelivery {
	d := queuemock.NewMockDelivery(ctrl)
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
		store,
		registry,
		runwaymq.TopicKeyMergeSignal,
		"orchestrator-mergesignal",
	)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	assert.Equal(t, consumer.TopicKey(runwaymq.TopicKeyMergeSignal), c.TopicKey())
	assert.Equal(t, "orchestrator-mergesignal", c.ConsumerGroup())
	assert.Equal(t, "mergesignal", c.Name())
	var _ consumer.Controller = c
}

func TestProcess_MergedAdvancesBatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(
		entity.Batch{ID: testBatchID, Queue: testQueue, State: entity.BatchStateMerging, Version: 1}, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), testBatchID, int32(1), int32(2), entity.BatchStateSucceeded).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.MergeResult{
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

func TestProcess_NotMergedMarksBatchFailed(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(
		entity.Batch{ID: testBatchID, Queue: testQueue, State: entity.BatchStateMerging, Version: 3}, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), testBatchID, int32(3), int32(4), entity.BatchStateFailed).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.MergeResult{Id: testBatchID, Outcome: runwaypb.Outcome_FAILED, Reason: "conflict in foo.go"}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	// Not-merged is an expected terminal outcome, so Process acks (no error).
	require.NoError(t, c.Process(context.Background(), newDelivery(ctrl, msg)))

	assert.ElementsMatch(t, []string{"conclude", "speculate"}, got)
}

func TestProcess_CancellingShortCircuit(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), testBatchID).Return(
		entity.Batch{ID: testBatchID, Queue: testQueue, State: entity.BatchStateCancelling, Version: 4}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	// No UpdateState and no fan-out: gomock fails if either runs.
	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.MergeResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
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
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.MergeResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, c.Process(context.Background(), newDelivery(ctrl, msg)))
	assert.ElementsMatch(t, []string{"conclude", "speculate"}, got)
}

func TestProcess_DeserializeErrorRejects(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
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
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()

	var got []string
	c := newController(t, store, recordingRegistry(t, ctrl, &got))

	res := runwaymq.MergeResult{Id: testBatchID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testBatchID, resultPayload(t, res), testQueue, nil)
	require.Error(t, c.Process(context.Background(), newDelivery(ctrl, msg)))
}
