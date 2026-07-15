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
	"strings"
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
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "runway-merge", Queue: q}},
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
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/repo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		LandStrategy: mergestrategy.MergeStrategySquashRebase,
	}
	req2 := entity.Request{
		ID:           "test-queue/2",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/repo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
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
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "runway-merge", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)
	require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batchID, batch.Queue)))

	// Full payload published to runway, keyed by the batch id (the correlation id).
	assert.Equal(t, "runway-merge", gotTopic)
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
				[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "runway-merge", Queue: q}},
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
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "runway-merge", Queue: q}},
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
		[]consumer.TopicConfig{{Key: runwaymq.TopicKeyMerge, Name: "runway-merge", Queue: q}},
	)
	require.NoError(t, err)

	c := newController(t, store, registry)
	err = c.Process(context.Background(), newDelivery(t, ctrl, batchID, "test-queue"))
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

// gateStorage wires the storage shape the dependency-outcome gate needs: the
// batch, its one dependency, and its speculation tree carrying a single
// Passed path based on that dependency.
func gateStorage(ctrl *gomock.Controller, batch, dep entity.Batch, tree entity.SpeculationTree) *storagemock.MockStorage {
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
	treeStore := storagemock.NewMockSpeculationTreeStore(ctrl)
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationTreeStore().Return(treeStore).AnyTimes()
	return store
}

// gateFixtures returns a Merging batch with one dependency in depState, that
// dependency, and a tree whose single Passed path is based on it.
func gateFixtures(depState entity.BatchState) (entity.Batch, entity.Batch, entity.SpeculationTree) {
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: depState, Version: 3}
	batch := entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        "test-queue",
		Contains:     []string{"test-queue/1"},
		Dependencies: []string{dep.ID},
		State:        entity.BatchStateMerging,
		Version:      2,
	}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 4,
		Paths: []entity.SpeculationPathInfo{{
			ID:     "test-queue/batch/1/path/0",
			Path:   entity.SpeculationPath{Base: []string{dep.ID}, Head: batch.ID},
			Status: entity.SpeculationPathStatusPassed,
		}},
	}
	return batch, dep, tree
}

// TestProcess_GateConfirmedPublishesToRunway: a landed base confirms the
// path's assumptions, so the gate opens and the full merge request is handed
// to runway.
func TestProcess_GateConfirmedPublishesToRunway(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch, dep, tree := gateFixtures(entity.BatchStateSucceeded)
	store := gateStorage(ctrl, batch, dep, tree)

	req := entity.Request{ID: "test-queue/1", Queue: "test-queue", LandStrategy: mergestrategy.MergeStrategyRebase}
	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), req.ID).Return(req, nil)
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
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q},
		{Key: topickey.TopicKeyMerge, Name: "submitqueue-merge", Queue: q},
	})
	require.NoError(t, err)

	c := newController(t, store, registry)
	require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batch.ID, batch.Queue)))

	assert.Equal(t, "merge", gotTopic)
	got := &runwaymq.MergeRequest{}
	require.NoError(t, runwaymq.Unmarshal(gotPayload, got))
	assert.Equal(t, batch.ID, got.Id)
	require.Len(t, got.Steps, 1)
}

// TestProcess_GateUnsettledBaseWaits: a base still in Merging — or in
// transient Cancelling on its way to a terminal state — keeps the gate
// closed but the attempt alive: the trigger is re-published to this stage's
// own topic after WaitDelayMs with a freshly minted message ID (reusing the
// consumed ID would be silently deduped and end the cycle). Nothing reaches
// runway.
func TestProcess_GateUnsettledBaseWaits(t *testing.T) {
	for _, depState := range []entity.BatchState{
		entity.BatchStateMerging,
		entity.BatchStateCancelling,
	} {
		t.Run(string(depState), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch, dep, tree := gateFixtures(depState)
			store := gateStorage(ctrl, batch, dep, tree)
			// No request-store reads: the merge request is never built while waiting.

			pub := queuemock.NewMockPublisher(ctrl)
			pub.EXPECT().
				PublishAfter(gomock.Any(), "submitqueue-merge", gomock.Any(), WaitDelayMs).
				DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message, _ int64) error {
					assert.True(t, strings.HasPrefix(msg.ID, batch.ID+"/mergewait/"), "re-check message must mint a fresh ID, got %q", msg.ID)
					bid, err := entity.BatchIDFromBytes(msg.Payload)
					require.NoError(t, err)
					assert.Equal(t, batch.ID, bid.ID)
					assert.Equal(t, batch.Queue, msg.PartitionKey)
					return nil
				})
			// No Publish expectation: a runway hand-off while waiting fails the test.
			q := queuemock.NewMockQueue(ctrl)
			q.EXPECT().Publisher().Return(pub).AnyTimes()
			registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
				{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q},
				{Key: topickey.TopicKeyMerge, Name: "submitqueue-merge", Queue: q},
			})
			require.NoError(t, err)

			c := newController(t, store, registry)
			require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batch.ID, batch.Queue)))
		})
	}
}

// TestProcess_GateRefutedDropsAndNudgesSpeculate: a failed base refutes
// every path, so the trigger is dropped — no runway hand-off, no re-check —
// and speculate is nudged directly with a minted message ID so its Merging
// supervision reliably runs and fails the batch (the terminal fan-out wake
// publishes under the bare batch ID, which publish dedup can swallow).
func TestProcess_GateRefutedDropsAndNudgesSpeculate(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch, dep, tree := gateFixtures(entity.BatchStateFailed)
	store := gateStorage(ctrl, batch, dep, tree)

	// The only publish is the speculate nudge: no runway hand-off, no
	// delayed re-check.
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().
		Publish(gomock.Any(), "speculate", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			assert.True(t, strings.HasPrefix(msg.ID, batch.ID+"/mergerefuted/"), "nudge must mint a fresh ID, got %q", msg.ID)
			bid, err := entity.BatchIDFromBytes(msg.Payload)
			require.NoError(t, err)
			assert.Equal(t, batch.ID, bid.ID)
			return nil
		})
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyMerge, Name: "merge", Queue: q},
		{Key: topickey.TopicKeyMerge, Name: "submitqueue-merge", Queue: q},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: q},
	})
	require.NoError(t, err)

	c := newController(t, store, registry)
	require.NoError(t, c.Process(context.Background(), newDelivery(t, ctrl, batch.ID, batch.Queue)))
}
