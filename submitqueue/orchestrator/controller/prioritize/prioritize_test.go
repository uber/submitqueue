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

package prioritize

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	prioritizermock "github.com/uber/submitqueue/submitqueue/extension/speculation/prioritizer/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// testHarness wires a Controller against mocked storage, prioritizer, build
// runner, and a build topic queue so tests can assert what got persisted and
// published.
type testHarness struct {
	controller *Controller
	batchStore *storagemock.MockBatchStore
	treeStore  *storagemock.MockSpeculationTreeStore
	prio       *prioritizermock.MockPrioritizer
	buildPub   *queuemock.MockPublisher
}

func newTestHarness(t *testing.T, ctrl *gomock.Controller) *testHarness {
	prio := prioritizermock.NewMockPrioritizer(ctrl)
	prioFactory := prioritizermock.NewMockFactory(ctrl)
	prioFactory.EXPECT().For(gomock.Any()).Return(prio, nil).AnyTimes()

	buildPub := queuemock.NewMockPublisher(ctrl)
	buildQ := queuemock.NewMockQueue(ctrl)
	buildQ.EXPECT().Publisher().Return(buildPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyPrioritize, Name: "prioritize", Queue: buildQ},
		{Key: topickey.TopicKeyBuild, Name: "build", Queue: buildQ},
	})
	require.NoError(t, err)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	treeStore := storagemock.NewMockSpeculationTreeStore(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationTreeStore().Return(treeStore).AnyTimes()

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		prioFactory,
		registry,
		topickey.TopicKeyPrioritize,
		"orchestrator-prioritize",
	)
	return &testHarness{
		controller: c,
		batchStore: batchStore,
		treeStore:  treeStore,
		prio:       prio,
		buildPub:   buildPub,
	}
}

// queueDelivery builds a delivery whose payload is a QueueID, matching the
// on-queue contract for this stage.
func queueDelivery(t *testing.T, ctrl *gomock.Controller, queue string) consumer.Delivery {
	t.Helper()
	payload, err := entity.QueueID{Name: queue}.ToBytes()
	require.NoError(t, err)
	msg := entityqueue.NewMessage(queue, payload, queue, nil)
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func TestController_Identity(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	assert.Equal(t, "prioritize", h.controller.Name())
	assert.Equal(t, topickey.TopicKeyPrioritize, h.controller.TopicKey())
	assert.Equal(t, "orchestrator-prioritize", h.controller.ConsumerGroup())

	var _ consumer.Controller = h.controller
}

// TestController_Process_NoInFlightBatches verifies an empty queue (no
// Speculating or Cancelling batches) just acks without touching the
// prioritizer or publishing anything.
func TestController_Process_NoInFlightBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return(nil, nil)

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_TreeMissingSkipsBatch verifies a batch with no
// speculation tree yet (not yet speculated on) is skipped rather than
// erroring the whole round.
func TestController_Process_TreeMissingSkipsBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(entity.SpeculationTree{}, storage.ErrNotFound)

	// No prioritizer, no publish expected — the harness mocks fail the test if called unexpectedly.
	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_PromoteApplied verifies a Promote decision on a
// Selected path transitions it to Prioritized, persists the tree with the
// correct old/new version pair, and republishes the batch to build.
func TestController_Process_PromoteApplied(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Base: nil, Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 3,
		Paths:   []entity.SpeculationPathInfo{{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected, Score: 0.9}},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return([]entity.PathDecision{
		{PathID: "q/batch/1/path/0", Action: entity.SpeculationPathActionPromote},
	}, nil)
	h.treeStore.EXPECT().
		Update(gomock.Any(), "q/batch/1", int32(3), int32(4), gomock.AssignableToTypeOf([]entity.SpeculationPathInfo{})).
		DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
			require.Len(t, paths, 1)
			assert.Equal(t, entity.SpeculationPathStatusPrioritized, paths[0].Status)
			return nil
		})
	h.buildPub.EXPECT().
		Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			bid, err := entity.BatchIDFromBytes(msg.Payload)
			require.NoError(t, err)
			assert.Equal(t, "q/batch/1", bid.ID)
			assert.Equal(t, "q", msg.PartitionKey)
			return nil
		})

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_IllegalDecisionSkipped verifies a decision that does
// not match any loaded path is dropped without an Update call, and nothing
// is published (no path reached Prioritized).
func TestController_Process_IllegalDecisionSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected}},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	// Cancel on a Selected path is not a legal transition: skip it.
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return([]entity.PathDecision{
		{PathID: "q/batch/1/path/0", Action: entity.SpeculationPathActionCancel},
	}, nil)
	// No treeStore.Update, no publish expected.

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_CancellingBatchRoutedNotRanked verifies a
// Cancelling batch's tree is loaded for routing only: none of its paths are
// offered to the prioritizer (even statuses that would normally be
// candidates), no decision is applied to it, but republishBuilds still
// publishes a build message for it because its tree carries a persisted
// Cancelling intent the build stage must enact.
func TestController_Process_CancellingBatchRoutedNotRanked(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	speculating := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	cancelling := entity.Batch{ID: "q/batch/2", Queue: "q", State: entity.BatchStateCancelling}

	specTree := entity.SpeculationTree{
		BatchID: speculating.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: "q/batch/1/path/0", Path: entity.SpeculationPath{Head: speculating.ID}, Status: entity.SpeculationPathStatusSelected},
		},
	}
	// The Cancelling batch's tree carries a Building path (a would-be
	// candidate had the batch not been cancelled) alongside its persisted
	// Cancelling intent.
	cxlTree := entity.SpeculationTree{
		BatchID: cancelling.ID,
		Version: 4,
		Paths: []entity.SpeculationPathInfo{
			{ID: "q/batch/2/path/0", Path: entity.SpeculationPath{Head: cancelling.ID}, Status: entity.SpeculationPathStatusBuilding, BuildID: "runner-2"},
			{ID: "q/batch/2/path/1", Path: entity.SpeculationPath{Head: cancelling.ID}, Status: entity.SpeculationPathStatusCancelling, BuildID: "runner-3"},
		},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{speculating, cancelling}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), speculating.ID).Return(specTree, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), cancelling.ID).Return(cxlTree, nil)

	// Only the Speculating batch's path is a candidate; the Cancelling
	// batch's paths must not appear, whatever their status. Return a rogue
	// decision naming the Cancelling batch's Building path — it was never a
	// candidate, so it must be skipped as illegal rather than applied.
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, candidates []entity.SpeculationPathInfo) ([]entity.PathDecision, error) {
			require.Len(t, candidates, 1)
			assert.Equal(t, "q/batch/1/path/0", candidates[0].ID)
			return []entity.PathDecision{
				{PathID: "q/batch/2/path/0", Action: entity.SpeculationPathActionCancel},
			}, nil
		})

	// The rogue decision is dropped -> no tree Update on either batch.
	// republishBuilds fires for the Cancelling batch only: the Speculating
	// tree has no Prioritized-no-build or Cancelling path.
	h.buildPub.EXPECT().
		Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			bid, err := entity.BatchIDFromBytes(msg.Payload)
			require.NoError(t, err)
			assert.Equal(t, cancelling.ID, bid.ID)
			return nil
		})

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_CancelBuildingCapturesIntent verifies Cancel on a
// Building path only flips it to Cancelling in the tree — intent capture,
// never a runner call from this controller — and that a build message is
// republished for the batch so the build stage enacts the cancel.
func TestController_Process_CancelBuildingCapturesIntent(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 2,
		Paths: []entity.SpeculationPathInfo{
			{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusBuilding, BuildID: "build-1"},
		},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return([]entity.PathDecision{
		{PathID: "q/batch/1/path/0", Action: entity.SpeculationPathActionCancel},
	}, nil)
	h.treeStore.EXPECT().
		Update(gomock.Any(), "q/batch/1", int32(2), int32(3), gomock.AssignableToTypeOf([]entity.SpeculationPathInfo{})).
		DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
			require.Len(t, paths, 1)
			assert.Equal(t, entity.SpeculationPathStatusCancelling, paths[0].Status)
			return nil
		})
	// The Cancelling path is a persisted intent the build stage must enact,
	// so a build message is republished for the batch.
	h.buildPub.EXPECT().Publish(gomock.Any(), "build", gomock.Any()).Return(nil)

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_CancelPrioritizedNoRunnerCall verifies Cancel on a
// Prioritized path (no build started yet) drops straight to Cancelled —
// there is no in-flight work, so no intent to hand to the build stage and no
// build republish.
func TestController_Process_CancelPrioritizedNoRunnerCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 5,
		Paths: []entity.SpeculationPathInfo{
			{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusPrioritized},
		},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return([]entity.PathDecision{
		{PathID: "q/batch/1/path/0", Action: entity.SpeculationPathActionCancel},
	}, nil)
	h.treeStore.EXPECT().
		Update(gomock.Any(), "q/batch/1", int32(5), int32(6), gomock.AssignableToTypeOf([]entity.SpeculationPathInfo{})).
		DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
			require.Len(t, paths, 1)
			assert.Equal(t, entity.SpeculationPathStatusCancelled, paths[0].Status)
			return nil
		})

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

// TestController_Process_VersionMismatchErrors verifies ErrVersionMismatch
// from the tree Update surfaces as an error (nack; the round is recomputed
// on redelivery).
func TestController_Process_VersionMismatchErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected}},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return([]entity.PathDecision{
		{PathID: "q/batch/1/path/0", Action: entity.SpeculationPathActionPromote},
	}, nil)
	h.treeStore.EXPECT().
		Update(gomock.Any(), "q/batch/1", int32(1), int32(2), gomock.Any()).
		Return(storage.ErrVersionMismatch)

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, storage.ErrVersionMismatch))
}

// TestController_Process_PrioritizerErrors verifies a Prioritize failure
// surfaces as an error without touching storage further.
func TestController_Process_PrioritizerErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected}},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return(nil, errors.New("policy boom"))

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.Error(t, err)
}

// TestController_Process_RepublishesPreExistingPrioritized verifies a batch
// whose tree already has a Prioritized path with no BuildID gets republished
// to build even when the prioritizer returns zero decisions this round —
// self-healing a dropped build message.
func TestController_Process_RepublishesPreExistingPrioritized(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	path := entity.SpeculationPath{Head: "q/batch/1"}
	batch := entity.Batch{ID: "q/batch/1", Queue: "q", State: entity.BatchStateSpeculating}
	tree := entity.SpeculationTree{
		BatchID: "q/batch/1",
		Version: 4,
		Paths: []entity.SpeculationPathInfo{
			{ID: "q/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusPrioritized, BuildID: ""},
		},
	}

	h.batchStore.EXPECT().
		GetByQueueAndStates(gomock.Any(), "q", []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateCancelling}).
		Return([]entity.Batch{batch}, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), "q/batch/1").Return(tree, nil)
	h.prio.EXPECT().Prioritize(gomock.Any(), gomock.Any()).Return(nil, nil)
	// No treeStore.Update expected: nothing changed this round.
	h.buildPub.EXPECT().
		Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			bid, err := entity.BatchIDFromBytes(msg.Payload)
			require.NoError(t, err)
			assert.Equal(t, "q/batch/1", bid.ID)
			return nil
		})

	err := h.controller.Process(context.Background(), queueDelivery(t, ctrl, "q"))
	require.NoError(t, err)
}

func TestController_Process_MalformedPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	msg := entityqueue.NewMessage("bad", []byte(`{"invalid"`), "q", nil)
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()

	err := h.controller.Process(context.Background(), d)
	require.Error(t, err)
}
