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

package speculate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// quietSpeculator proposes nothing, which is what tests of the message-level
// branches want: the run happens but changes no paths. It records the batches
// it was offered, so a test can assert that a run reached them at all.
type quietSpeculator struct {
	saw []entity.Batch
}

func (s *quietSpeculator) Speculate(_ context.Context, batches []entity.Batch, _ []entity.SpeculationPathSet) ([]entity.Speculation, error) {
	s.saw = append(s.saw, batches...)
	return nil, nil
}

// staticSpeculatorFactory returns a fixed Speculator for any queue.
type staticSpeculatorFactory struct{ s speculator.Speculator }

func (f staticSpeculatorFactory) For(speculator.Config) (speculator.Speculator, error) {
	return f.s, nil
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

// listsInFlight makes the queue read return exactly these batches: each is
// filed under its state's membership bucket and hydrated back through the
// batch store. With no arguments the run is quiet — the queue lists no
// in-flight batches, so the run returns before reading any path set or asking
// the Speculator; run_test.go covers the run itself.
func (h *procHarness) listsInFlight(batches ...entity.Batch) {
	h.queueStates.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, state entity.BatchState) ([]entity.QueueBatchState, error) {
			var records []entity.QueueBatchState
			for _, b := range batches {
				if b.State == state {
					records = append(records, entity.QueueBatchState{Queue: b.Queue, State: state, BatchID: b.ID})
				}
			}
			return records, nil
		},
	).AnyTimes()
	for _, b := range batches {
		h.batches.EXPECT().Get(gomock.Any(), b.ID).Return(b, nil).AnyTimes()
	}
}

func batchIDPayload(t *testing.T, id string) []byte {
	t.Helper()
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

func testBatch(state entity.BatchState, deps ...string) entity.Batch {
	return entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        "test-queue",
		Dependencies: deps,
		State:        state,
		Version:      1,
	}
}

// procHarness wires a controller and records which topics were published to.
type procHarness struct {
	controller  *Controller
	batches     *storagemock.MockBatchStore
	queueStates *storagemock.MockQueueBatchStateStore
	pathSets    *storagemock.MockSpeculationPathSetStore
	pathBuilds  *storagemock.MockPathBuildStore
	builds      *storagemock.MockBuildStore
	spec        *quietSpeculator
	published   []string
}

func newProcHarness(t *testing.T, ctrl *gomock.Controller, publishErr error) *procHarness {
	t.Helper()
	h := &procHarness{spec: &quietSpeculator{}}

	h.batches = storagemock.NewMockBatchStore(ctrl)
	h.queueStates = storagemock.NewMockQueueBatchStateStore(ctrl)
	h.queueStates.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	h.queueStates.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	h.pathSets = storagemock.NewMockSpeculationPathSetStore(ctrl)
	h.pathBuilds = storagemock.NewMockPathBuildStore(ctrl)
	h.builds = storagemock.NewMockBuildStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(h.batches).AnyTimes()
	store.EXPECT().GetQueueBatchStateStore().Return(h.queueStates).AnyTimes()
	store.EXPECT().GetSpeculationPathSetStore().Return(h.pathSets).AnyTimes()
	store.EXPECT().GetPathBuildStore().Return(h.pathBuilds).AnyTimes()
	store.EXPECT().GetBuildStore().Return(h.builds).AnyTimes()

	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, _ entityqueue.Message) error {
			if publishErr != nil {
				return publishErr
			}
			h.published = append(h.published, topic)
			return nil
		},
	).AnyTimes()
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyBuild, Name: "build", Queue: q},
		{Key: topickey.TopicKeyMerge, Name: "submitqueue-merge", Queue: q},
		{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: q},
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: q},
	})
	require.NoError(t, err)

	h.controller = NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, staticStorageFactory{store: store},
		staticSpeculatorFactory{s: h.spec}, registry,
		topickey.TopicKeySpeculate, "orchestrator-speculate",
	)
	return h
}

func (h *procHarness) process(t *testing.T, ctrl *gomock.Controller, batchID string) error {
	t.Helper()
	msg := entityqueue.NewMessage(batchID, batchIDPayload(t, batchID), "test-queue", nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return h.controller.Process(context.Background(), d)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newProcHarness(t, ctrl, nil)

	assert.Equal(t, topickey.TopicKeySpeculate, h.controller.TopicKey())
	assert.Equal(t, "orchestrator-speculate", h.controller.ConsumerGroup())
	assert.Equal(t, "speculate", h.controller.Name())

	var _ consumer.Controller = h.controller
}

// A Created batch is admitted so the Speculator can act on it, and must not
// reach an outcome on the same message — nothing has been built yet.
func TestProcess_AdmitsCreatedBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newProcHarness(t, ctrl, nil)
	batch := testBatch(entity.BatchStateCreated)

	h.batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: batch.ID, state: entity.BatchStateSpeculating}, int32(1), int32(2)).
		Return(nil)
	h.listsInFlight()

	require.NoError(t, h.process(t, ctrl, batch.ID))
	assert.Empty(t, h.published, "a batch cannot merge on the message that admitted it")
}

// A terminal batch re-publishes to conclude so a lost publish is repaired, and
// re-plans its queue so dependents see an outcome that was recorded after the
// run which produced it had already taken its snapshot.
func TestProcess_TerminalSelfHeals(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
		entity.BatchStateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newProcHarness(t, ctrl, nil)
			batch := testBatch(state)

			h.batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			h.listsInFlight()

			require.NoError(t, h.process(t, ctrl, batch.ID))
			assert.Equal(t, []string{"conclude"}, h.published)
		})
	}
}

// A terminal batch re-plans its queue rather than only reconciling itself. The
// run that finalized it computed every dependent against a snapshot taken
// before the transition, so without this a dependent whose own builds have all
// finished would never learn the outcome.
func TestProcess_TerminalReplansQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newProcHarness(t, ctrl, nil)
	batch := testBatch(entity.BatchStateSucceeded)

	dependent := entity.Batch{
		ID: "test-queue/batch/2", Queue: "test-queue",
		State: entity.BatchStateSpeculating, Dependencies: []string{batch.ID}, Version: 1,
	}

	h.batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.listsInFlight(dependent)
	h.batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), dependent.ID).
		Return(entity.SpeculationPathSet{}, storage.ErrNotFound)

	require.NoError(t, h.process(t, ctrl, batch.ID))
	assert.ElementsMatch(t, []entity.Batch{batch, dependent}, h.spec.saw,
		"the dependent must be re-planned against the terminal outcome, which it can only be weighed against if the terminal batch comes too")
}

// A Merging batch is the merge stage's to finish; the run still happens for the
// rest of the queue, but this batch is not an action target.
func TestProcess_MergingRunsButDoesNotAct(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newProcHarness(t, ctrl, nil)
	batch := testBatch(entity.BatchStateMerging)

	h.batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.listsInFlight()

	require.NoError(t, h.process(t, ctrl, batch.ID))
	assert.Empty(t, h.published)
}

func TestProcess_Errors(t *testing.T) {
	t.Run("malformed payload", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newProcHarness(t, ctrl, nil)

		msg := entityqueue.NewMessage("anything", []byte("not-json"), "test-queue", nil)
		d := consumermock.NewMockDelivery(ctrl)
		d.EXPECT().Message().Return(msg).AnyTimes()
		d.EXPECT().Attempt().Return(1).AnyTimes()

		require.Error(t, h.controller.Process(context.Background(), d))
	})

	t.Run("batch read failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newProcHarness(t, ctrl, nil)
		h.batches.EXPECT().Get(gomock.Any(), "test-queue/batch/1").
			Return(entity.Batch{}, storage.ErrNotFound)

		require.Error(t, h.process(t, ctrl, "test-queue/batch/1"))
	})

	t.Run("conclude publish failure on a terminal batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		h := newProcHarness(t, ctrl, assert.AnError)
		batch := testBatch(entity.BatchStateSucceeded)
		h.batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

		require.Error(t, h.process(t, ctrl, batch.ID))
	})
}
