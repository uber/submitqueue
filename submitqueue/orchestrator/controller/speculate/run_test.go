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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// scriptedSpeculator returns a fixed answer and records what it was asked.
type scriptedSpeculator struct {
	proposals []entity.Speculation
	err       error

	gotBatches []entity.Batch
	gotSets    []entity.SpeculationPathSet
	calls      int
}

func (s *scriptedSpeculator) Speculate(_ context.Context, batches []entity.Batch, sets []entity.SpeculationPathSet) ([]entity.Speculation, error) {
	s.calls++
	s.gotBatches = batches
	s.gotSets = sets
	return s.proposals, s.err
}

// updateTo matches a BatchStore.Update argument by its ID and target state —
// the two things an outcome write is asserted on.
type updateTo struct {
	id    string
	state entity.BatchState
}

func (m updateTo) Matches(x any) bool {
	b, ok := x.(entity.Batch)
	return ok && b.ID == m.id && b.State == m.state
}

func (m updateTo) String() string {
	return fmt.Sprintf("batch %s updated to state %s", m.id, m.state)
}

type runHarness struct {
	controller *Controller
	store      *storagemock.MockStorage
	batches    *storagemock.MockBatchStore
	pathSets   *storagemock.MockSpeculationPathSetStore
	pathBuilds *storagemock.MockPathBuildStore
	builds     *storagemock.MockBuildStore
	spec       *scriptedSpeculator
	published  []string
	// filed and unfiled are the queue's membership-record writes: a state
	// change has to move the record, or the batch stays listed under the
	// bucket it left and every later run re-reads it.
	filed   []entity.QueueBatchState
	unfiled []entity.QueueBatchState
	// messages are the published messages themselves, so a test can assert
	// what a publish carried and how it was partitioned.
	messages []entityqueue.Message
	// failTopic, when set, makes every publish to that topic fail.
	failTopic string
}

// failPublishTo makes publishes to one topic fail, leaving the others working,
// so a test can isolate the recovery path for a single lost publish.
func (h *runHarness) failPublishTo(topic string) {
	h.failTopic = topic
}

// speculatedOver returns the IDs of the heads the Speculator was offered.
func (h *runHarness) speculatedOver() []string {
	ids := make([]string, 0, len(h.spec.gotBatches))
	for _, b := range h.spec.gotBatches {
		ids = append(ids, b.ID)
	}
	return ids
}

// run drives a run whose triggering message names triggerID. Which batch that
// is only matters for recovery: a retry of that message revisits that batch and
// no other, so it is the one batch that needs no recovery signal of its own.
func (h *runHarness) run(triggerID string) error {
	return h.controller.run(context.Background(), h.store, entity.Batch{ID: triggerID, Queue: "q"})
}

// newRunHarness wires a controller whose queue read returns inFlight.
func newRunHarness(t *testing.T, ctrl *gomock.Controller, spec *scriptedSpeculator, inFlight []entity.Batch) *runHarness {
	t.Helper()

	h := &runHarness{spec: spec}

	h.batches = storagemock.NewMockBatchStore(ctrl)
	for _, b := range inFlight {
		h.batches.EXPECT().Get(gomock.Any(), b.ID).Return(b, nil).AnyTimes()
	}

	// The queue read goes through the membership records: List answers per
	// state bucket, and each candidate is hydrated back through Get above.
	queueStates := storagemock.NewMockQueueBatchStateStore(ctrl)
	queueStates.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, state entity.BatchState) ([]entity.QueueBatchState, error) {
			var records []entity.QueueBatchState
			for _, b := range inFlight {
				if b.State == state {
					records = append(records, entity.QueueBatchState{Queue: b.Queue, State: state, BatchID: b.ID})
				}
			}
			return records, nil
		},
	).AnyTimes()
	queueStates.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, record entity.QueueBatchState) error {
			h.filed = append(h.filed, record)
			return nil
		},
	).AnyTimes()
	queueStates.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, state entity.BatchState, batchID string) error {
			h.unfiled = append(h.unfiled, entity.QueueBatchState{State: state, BatchID: batchID})
			return nil
		},
	).AnyTimes()

	h.pathSets = storagemock.NewMockSpeculationPathSetStore(ctrl)
	h.pathBuilds = storagemock.NewMockPathBuildStore(ctrl)
	h.builds = storagemock.NewMockBuildStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	h.store = store
	store.EXPECT().GetQueueBatchStateStore().Return(queueStates).AnyTimes()
	store.EXPECT().GetBatchStore().Return(h.batches).AnyTimes()
	store.EXPECT().GetSpeculationPathSetStore().Return(h.pathSets).AnyTimes()
	store.EXPECT().GetPathBuildStore().Return(h.pathBuilds).AnyTimes()
	store.EXPECT().GetBuildStore().Return(h.builds).AnyTimes()

	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			if topic == h.failTopic {
				return assert.AnError
			}
			h.published = append(h.published, topic)
			h.messages = append(h.messages, msg)
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
		staticSpeculatorFactory{s: spec}, registry, topickey.TopicKeySpeculate, "orchestrator-speculate",
	)
	return h
}

// noBuildsDispatched makes the build lookup find nothing, i.e. no path has a build yet.
// It is opt-in rather than a harness default because a catch-all registered up
// front would shadow the specific expectations build-lookup tests set.
func (h *runHarness) noBuildsDispatched() {
	h.pathBuilds.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.PathBuild{}, storage.ErrNotFound).AnyTimes()
}

func speculatingHead() entity.Batch {
	return entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateSpeculating,
		Dependencies: []string{dep1, dep2}, Version: 1,
	}
}

// A funded path is persisted as pending and the head is dispatched to build.
func TestRun_FundsProposedPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{proposals: []entity.Speculation{{Path: path, Action: entity.PathActionBuild}}}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)

	// First funding of a head creates its set at version 1.
	h.pathSets.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, set entity.SpeculationPathSet) error {
			require.Len(t, set.Paths, 1)
			// The set must name the queue it belongs to: the store is bound to
			// one queue and refuses a write that disagrees, so an unstamped set
			// means no head is ever funded.
			assert.Equal(t, "q", set.Queue)
			assert.Equal(t, path.ID(), set.Paths[0].ID)
			assert.Equal(t, entity.SpeculationPathStatusPending, set.Paths[0].Status)
			assert.Equal(t, 1, set.Paths[0].Attempt)
			assert.Equal(t, int32(1), set.Version)
			return nil
		})

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"build"}, h.published)
}

// A build dispatch carries the head's real queue and partitions by the head.
// The two are different values here on purpose: the consumer resolves its
// storage from the payload's queue, so stamping anything else — the batch ID,
// say — dead-letters every dispatch, while partitioning by the batch is what
// lets separate heads dispatch in parallel.
func TestRun_DispatchStampsQueueAndPartitionsByHead(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{proposals: []entity.Speculation{{Path: path, Action: entity.PathActionBuild}}}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)
	h.pathSets.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	require.NoError(t, h.run(head))

	require.Len(t, h.messages, 1)
	got, err := entity.BatchIDFromBytes(h.messages[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, head, got.ID)
	assert.Equal(t, "q", got.Queue, "the payload must name the real queue, not the partition key")
	assert.Equal(t, head, h.messages[0].PartitionKey, "heads dispatch in parallel, so the batch is the partition key")
}

// The Speculator sees the speculating batches and their path sets.
func TestRun_PassesSnapshotToSpeculator(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	merging := entity.Batch{ID: "q/batch/merging", Queue: "q", State: entity.BatchStateMerging, Version: 1}
	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead(), merging})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSucceeded}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)

	existing := entity.SpeculationPathSet{Head: head, Version: 3}
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(existing, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), merging.ID).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)

	require.NoError(t, h.run(head))

	require.Equal(t, 1, spec.calls)
	require.Len(t, spec.gotBatches, 1)
	assert.Equal(t, head, spec.gotBatches[0].ID, "only speculating heads are action targets")
	require.Len(t, spec.gotSets, 1)
	assert.Equal(t, int32(3), spec.gotSets[0].Version)
}

// A resolved dependency that breaks a running path's assumption cancels it,
// without the Speculator being consulted.
func TestRun_CancelsBrokenPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	// dep1 failed, so a path betting on its success can never pass.
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateFailed}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)

	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(broken, entity.SpeculationPathStatusBuilding)},
		Version: 2,
	}, nil)

	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).
		DoAndReturn(func(_ context.Context, set entity.SpeculationPathSet, _, _ int32) error {
			assert.Equal(t, entity.SpeculationPathStatusCancelling, set.Paths[0].Status)
			return nil
		})

	require.NoError(t, h.run(head))
	assert.Empty(t, h.published,
		"a cancelling path needs no dispatch; the poll loop reads the stop off the set")
}

// A proposal the check rejects is dropped without touching storage, and the run
// still succeeds — a misbehaving Speculator must not stall the queue.
func TestRun_DropsRejectedProposal(t *testing.T) {
	ctrl := gomock.NewController(t)
	malformed := pathOver(entity.DependencyAssumptionSucceeds) // missing dep2
	spec := &scriptedSpeculator{proposals: []entity.Speculation{
		{Path: malformed, Action: entity.PathActionBuild},
	}}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)

	// No Create and no Update: nothing was accepted.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published)
}

// Re-proposing a path that is already funded must not start a second build.
func TestRun_AlreadyFundedPathIsNotRefunded(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{proposals: []entity.Speculation{{Path: path, Action: entity.PathActionBuild}}}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(path, entity.SpeculationPathStatusBuilding)},
		Version: 1,
	}, nil)

	// No Update: the path keeps the slot and the attempt it already has.
	require.NoError(t, h.run(head))

	// The head still has no actionable path, so nothing is dispatched.
	assert.Empty(t, h.published)
}

// A pending path is re-dispatched every run until the build stage moves it on,
// so a dispatch lost in flight is eventually re-sent.
func TestRun_RedispatchesPendingPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(path, entity.SpeculationPathStatusPending)},
		Version: 1,
	}, nil)

	// Nothing changed, so no write — but the dispatch goes out again.
	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"build"}, h.published)
}

// Losing the path-set race is not an error: the head is simply re-planned next
// run, and the rest of the queue is unaffected.
func TestRun_LostCASIsNotAnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateFailed}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(broken, entity.SpeculationPathStatusBuilding)},
		Version: 2,
	}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(storage.ErrVersionMismatch)

	require.NoError(t, h.run(head))
	assert.Empty(t, h.published, "a head whose write was lost is not dispatched on stale state")
}

// A Speculator failure abandons the run; the next dirty signal retries with
// fresh state.
func TestRun_SpeculatorFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{err: fmt.Errorf("scorer unavailable")}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)

	require.Error(t, h.run(head))
}

// A queue with nothing speculating never reaches the Speculator.
func TestRun_NoSpeculatingBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, nil)
	require.NoError(t, h.run(head))
	assert.Zero(t, spec.calls)
}

// Verify the harness's speculator satisfies the extension contract.
var _ speculator.Speculator = (*scriptedSpeculator)(nil)

// Reading build records is how the run learns what CI did without the build stages writing
// the path set. A finished build becomes the path's status in memory, and the
// run's own write is what persists it.
func TestRun_RecordsFinishedBuildsOnPaths(t *testing.T) {
	tests := []struct {
		name  string
		build entity.BuildStatus
		want  entity.SpeculationPathStatus
	}{
		{"succeeded becomes passed", entity.BuildStatusSucceeded, entity.SpeculationPathStatusPassed},
		{"failed becomes failed", entity.BuildStatusFailed, entity.SpeculationPathStatusFailed},
		{"cancelled becomes cancelled", entity.BuildStatusCancelled, entity.SpeculationPathStatusCancelled},
		{"running becomes building", entity.BuildStatusRunning, entity.SpeculationPathStatusBuilding},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
			spec := &scriptedSpeculator{}

			h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
			h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
			h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
			h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
				Head:    head,
				Paths:   []entity.SpeculationPathEntry{entryFor(path, entity.SpeculationPathStatusPending)},
				Version: 1,
			}, nil)

			h.pathBuilds.EXPECT().Get(gomock.Any(), path.ID(), 1).
				Return(entity.PathBuild{PathID: path.ID(), Attempt: 1, BuildID: "build-1"}, nil)
			h.builds.EXPECT().Get(gomock.Any(), "build-1").
				Return(entity.Build{ID: "build-1", BatchID: head, Status: tt.build}, nil)

			h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).
				DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
					assert.Equal(t, tt.want, s.Paths[0].Status)
					return nil
				})

			require.NoError(t, h.run(head))
		})
	}
}

// A path the run wants stopped stays cancelling while its build is still
// running: intent outlives a build still seen running, and only CI actually
// stopping ends it.
func TestRun_BuildUpdateDoesNotOverrideCancellingIntent(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(path, entity.SpeculationPathStatusCancelling)},
		Version: 1,
	}, nil)

	h.pathBuilds.EXPECT().Get(gomock.Any(), path.ID(), 1).
		Return(entity.PathBuild{PathID: path.ID(), Attempt: 1, BuildID: "build-1"}, nil)
	h.builds.EXPECT().Get(gomock.Any(), "build-1").
		Return(entity.Build{ID: "build-1", BatchID: head, Status: entity.BuildStatusRunning}, nil)

	// Nothing changed, so no write — and no dispatch either: the poll loop is
	// what keeps asking the runner to stop, not the build stage.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published)
}

// A finished path is never looked up again: its outcome is already in the set.
func TestRun_DoesNotReReadFinishedPaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	// dep1 is unresolved, so the passed path cannot merge — this test is about
	// observation being skipped, not about outcomes.
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSpeculating}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateFailed}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(path, entity.SpeculationPathStatusPassed)},
		Version: 1,
	}, nil)

	// The default pathBuilds expectation is AnyTimes, so assert on the build
	// store: a finished entry must not reach it.
	require.NoError(t, h.run(head))
}

// Broken paths are cancelled before the Speculator is asked, so it reasons over the
// queue as the facts have already left it. Asking first would have it propose
// work on top of a path this run is about to rule out, which check would only
// throw away.
func TestRun_BrokenPathsAreVisibleToTheSpeculator(t *testing.T) {
	ctrl := gomock.NewController(t)
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	// dep1 failed, so a path assuming it succeeds can never pass.
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateFailed}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSpeculating}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(broken, entity.SpeculationPathStatusBuilding)},
		Version: 2,
	}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).Return(nil)

	require.NoError(t, h.run(head))

	require.Equal(t, 1, spec.calls)
	require.Len(t, spec.gotSets, 1)
	require.Len(t, spec.gotSets[0].Paths, 1)
	assert.Equal(t, entity.SpeculationPathStatusCancelling, spec.gotSets[0].Paths[0].Status,
		"the Speculator must see the broken path as already cancelling")
}

// cancelBrokenPathsInSet marks broken paths cancelling, not cancelled: their builds
// may still be occupying CI, and only the signal that sees them stop can call
// it done.
func TestCancelBrokenPathsInSet(t *testing.T) {
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	intact := pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails)

	set := entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			{ID: broken.ID(), Path: broken, Status: entity.SpeculationPathStatusBuilding},
			{ID: intact.ID(), Path: intact, Status: entity.SpeculationPathStatusPending},
		},
	}

	// dep1 failed, so the path assuming it succeeds is broken.
	snap := snapWith(entity.BatchStateFailed, entity.BatchStateSpeculating)

	require.True(t, cancelBrokenPathsInSet(&set, snap, 42))
	assert.Equal(t, entity.SpeculationPathStatusCancelling, set.Paths[0].Status)
	assert.Equal(t, int64(42), set.Paths[0].UpdatedAtMs)
	assert.Equal(t, entity.SpeculationPathStatusPending, set.Paths[1].Status)
}

// A path whose build already finished is left alone: a recorded outcome is not
// something a later run gets to revise.
func TestCancelBrokenPathsInSet_LeavesFinishedPaths(t *testing.T) {
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	snap := snapWith(entity.BatchStateFailed, entity.BatchStateSpeculating)

	for _, status := range []entity.SpeculationPathStatus{
		entity.SpeculationPathStatusPassed,
		entity.SpeculationPathStatusFailed,
		entity.SpeculationPathStatusCancelled,
		entity.SpeculationPathStatusCancelling,
	} {
		t.Run(string(status), func(t *testing.T) {
			set := entity.SpeculationPathSet{Head: head, Paths: []entity.SpeculationPathEntry{
				{ID: broken.ID(), Path: broken, Status: status},
			}}
			assert.False(t, cancelBrokenPathsInSet(&set, snap, 42))
			assert.Equal(t, status, set.Paths[0].Status)
		})
	}
}

// A head whose outcome is already decided is not offered to the Speculator.
// Asking would invite proposals for a head that is on its way out of the queue,
// and this run would fund those paths and supersede them in the same set.
func TestRun_DecidedHeadIsNotSpeculatedOn(t *testing.T) {
	ctrl := gomock.NewController(t)
	passed := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{
		// Would be applied if the head were still offered, creating a pending
		// path that supersede then cancels in the same write.
		proposals: []entity.Speculation{{
			Path:   pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails),
			Action: entity.PathActionBuild,
		}},
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	// Both dependencies resolved the way the passed path assumed, so it merges.
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSucceeded}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateFailed}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(passed, entity.SpeculationPathStatusPassed)},
		Version: 1,
	}, nil)

	// The head moves to merging. Its set is untouched: the only path is the
	// winner, and no proposal was applied.
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateMerging}, int32(1), int32(2)).
		Return(nil)

	require.NoError(t, h.run(head))

	assert.Zero(t, spec.calls, "a decided head leaves nothing to speculate about")
	assert.Equal(t, []string{"submitqueue-merge"}, h.published)
	assert.Contains(t, h.filed, entity.QueueBatchState{Queue: "q", State: entity.BatchStateMerging, BatchID: head},
		"the outcome must file the head under its new state")
	assert.Contains(t, h.unfiled, entity.QueueBatchState{State: entity.BatchStateSpeculating, BatchID: head},
		"and drop the record under the state it left")
}

// The churn this ordering removes: a mergeable head must not gain a funded path
// that the same run immediately cancels — the dispatch would have started CI
// for work nothing waits for.
func TestRun_MergeableHeadGainsNoNewPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	passed := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	other := pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{
		proposals: []entity.Speculation{{Path: other, Action: entity.PathActionBuild}},
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSucceeded}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateFailed}, nil)

	// A sibling is still building, so supersede has something to cancel and the
	// set is written — which is exactly where a stray funded path would show up.
	sibling := entryFor(other, entity.SpeculationPathStatusBuilding)
	sibling.ID = "sibling-path"
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			entryFor(passed, entity.SpeculationPathStatusPassed),
			sibling,
		},
		Version: 1,
	}, nil)

	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).
		DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
			require.Len(t, s.Paths, 2, "no path was funded for a head that is merging")
			assert.Equal(t, entity.SpeculationPathStatusPassed, s.Paths[0].Status)
			assert.Equal(t, entity.SpeculationPathStatusCancelling, s.Paths[1].Status)
			return nil
		})
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateMerging}, int32(1), int32(2)).
		Return(nil)

	require.NoError(t, h.run(head))
	assert.Zero(t, spec.calls)
}

// A head with no future left fails, and the write order is what keeps conclude
// usable: conclude reconciles requests from the batch's state and rejects a
// non-terminal one, so it is published only once the terminal write has landed.
func TestRun_FailedHeadConcludesAfterTheStateWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}
	failed := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionSucceeds)

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.noBuildsDispatched()
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSucceeded}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateSucceeded}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(failed, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)

	var publishedBeforeWrite []string
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateFailed}, int32(1), int32(2)).
		DoAndReturn(func(context.Context, entity.Batch, int32, int32) error {
			publishedBeforeWrite = append([]string(nil), h.published...)
			return nil
		})

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"conclude"}, h.published)
	assert.Empty(t, publishedBeforeWrite,
		"conclude rejects a non-terminal batch, so it must not be published before the write")
}

// A failure resolves a dependency, which can fail everything stacked on it. The
// whole cascade finalizes inside one run: a single pass would only catch the
// dependents that happen to come later in queue order, and the rest would wait
// for an unrelated signal to wake the queue again.
func TestRun_FailureCascadesWithinOneRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	// dependent is listed first, so a single forward pass would evaluate it
	// before the head it is stacked on has failed.
	dependent := entity.Batch{
		ID: "q/batch/dependent", Queue: "q", State: entity.BatchStateSpeculating,
		Dependencies: []string{head}, Version: 1,
	}
	failing := entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateSpeculating, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{dependent, failing})
	h.noBuildsDispatched()

	failingPath := entity.SpeculationPath{Head: head}
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(failingPath, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)

	// The dependent bet that its dependency would fail and built on that, and
	// that build failed. The bet is vindicated rather than broken the moment
	// the dependency fails, which leaves the dependent with a live path that
	// cannot pass — so it can only be decided by the cascade, and only once the
	// dependency is terminal.
	dependentPath := entity.SpeculationPath{
		Head:         dependent.ID,
		Dependencies: []entity.PathDependency{{Batch: head, Assumption: entity.DependencyAssumptionFails}},
	}
	h.pathSets.EXPECT().Get(gomock.Any(), dependent.ID).Return(entity.SpeculationPathSet{
		Head:    dependent.ID,
		Paths:   []entity.SpeculationPathEntry{entryFor(dependentPath, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)

	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateFailed}, int32(1), int32(2)).Return(nil)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: dependent.ID, state: entity.BatchStateFailed}, int32(1), int32(2)).Return(nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil).AnyTimes()

	require.NoError(t, h.run(head))
	assert.Zero(t, spec.calls, "no head is left open, so there is nothing to speculate about")
}

// A cancelling batch is driven to terminal by the run like any other batch,
// off the same read: its paths are marked stopped, and once they all are the
// batch is cancelled and its requests reconciled.
func TestRun_CancellingBatchFinalizesInTheRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	cancelling := entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{cancelling})
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 2,
	}, nil)
	// The build stages record what CI did on the Build row alone; folding that
	// in is what lets the run see the path has actually stopped.
	h.pathBuilds.EXPECT().Get(gomock.Any(), "p1", 1).
		Return(entity.PathBuild{PathID: "p1", Attempt: 1, BuildID: "b1"}, nil)
	h.builds.EXPECT().Get(gomock.Any(), "b1").
		Return(entity.Build{ID: "b1", Status: entity.BuildStatusCancelled}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).
		DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
			assert.Equal(t, entity.SpeculationPathStatusCancelled, s.Paths[0].Status)
			return nil
		})
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateCancelled}, int32(1), int32(2)).Return(nil)

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"conclude"}, h.published)
}

// A cancelling batch whose builds are still running is not terminal yet. Its
// paths are marked cancelling — which is all the poll loop needs to stop them
// — and the batch waits: marking it cancelled here would declare it stopped
// while CI still held its slots.
func TestRun_CancellingBatchWaitsForBuildsToStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	cancelling := entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{cancelling})
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusBuilding, Attempt: 1},
		},
		Version: 2,
	}, nil)
	h.pathBuilds.EXPECT().Get(gomock.Any(), "p1", 1).
		Return(entity.PathBuild{PathID: "p1", Attempt: 1, BuildID: "b1"}, nil)
	h.builds.EXPECT().Get(gomock.Any(), "b1").
		Return(entity.Build{ID: "b1", Status: entity.BuildStatusRunning}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).
		DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
			assert.Equal(t, entity.SpeculationPathStatusCancelling, s.Paths[0].Status)
			return nil
		})

	// No batch Update: the batch is not terminal yet. And no publish:
	// stopping the running build is the poll loop's job, not a dispatch.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published)
}

// A path cancelled before its build was ever dispatched has nothing to stop:
// no link means no build the poll loop could ever be watching. Left alone the
// path would hold a budget slot and keep its batch out of a terminal state
// forever, so the run marks it cancelled from that absence.
func TestRun_CancellingPathWithNoBuildIsCancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	cancelling := entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{cancelling})
	h.noBuildsDispatched()
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 2,
	}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).
		DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
			assert.Equal(t, entity.SpeculationPathStatusCancelled, s.Paths[0].Status)
			return nil
		})
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateCancelled}, int32(1), int32(2)).Return(nil)

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"conclude"}, h.published)
}

// A batch cancelled before anything was funded has no builds to wait on.
func TestRun_CancellingBatchWithNoPaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	cancelling := entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{cancelling})
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateCancelled}, int32(1), int32(2)).Return(nil)

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"conclude"}, h.published)
}

// Losing the path-set race must not let the terminal write proceed off a view
// another writer has already replaced — the batch could still be holding a
// build the winning write knows about.
func TestRun_CancellingLostPathSetRaceSkipsTheTerminalWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	cancelling := entity.Batch{
		ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{cancelling})
	h.noBuildsDispatched()
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 2,
	}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).
		Return(storage.ErrVersionMismatch)

	// No batch Update and no conclude: the next run re-reads and re-decides.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published)
}

// The allocator rations a budget measured in occupied CI slots, and a path
// holds its slot until its build actually stops. A merging head's superseded
// siblings are still running, so hiding their set would let the allocator count
// those slots as free and oversubscribe CI.
func TestRun_SpeculatorSeesPathSetsOfNonOpenHeads(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	merging := entity.Batch{
		ID: "q/batch/merging", Queue: "q", State: entity.BatchStateMerging, Version: 1,
	}
	open := entity.Batch{ID: head, Queue: "q", State: entity.BatchStateSpeculating, Version: 1}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{open, merging})
	h.noBuildsDispatched()
	h.pathSets.EXPECT().Get(gomock.Any(), head).
		Return(entity.SpeculationPathSet{Head: head, Version: 1}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), merging.ID).Return(entity.SpeculationPathSet{
		Head: merging.ID,
		Paths: []entity.SpeculationPathEntry{
			{ID: "still-running", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 1,
	}, nil)
	// The undispatched cancelling path is marked cancelled, so the merging head's set is
	// rewritten even though it is closed to new work.
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).
		DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
			assert.Equal(t, merging.ID, s.Head)
			assert.Equal(t, entity.SpeculationPathStatusCancelled, s.Paths[0].Status)
			return nil
		})

	require.NoError(t, h.run(head))

	assert.Equal(t, []entity.Batch{open}, spec.gotBatches,
		"only an open head may be an action target")
	require.Len(t, spec.gotSets, 2, "every in-flight path set counts against the budget")
	assert.Equal(t, merging.ID, spec.gotSets[1].Head)
}

// A queue whose only in-flight head is closed to new work still has to be
// written: the build stages record what CI did on the Build row alone, and this
// is the only writer that can finish the path it belongs to. Left unwritten,
// the path would keep charging the budget.
func TestRun_PersistsObservationsWithNoOpenHead(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	merging := entity.Batch{
		ID: "q/batch/merging", Queue: "q", State: entity.BatchStateMerging, Version: 1,
	}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{merging})
	h.pathSets.EXPECT().Get(gomock.Any(), merging.ID).Return(entity.SpeculationPathSet{
		Head: merging.ID,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 3,
	}, nil)
	h.pathBuilds.EXPECT().Get(gomock.Any(), "p1", 1).
		Return(entity.PathBuild{PathID: "p1", Attempt: 1, BuildID: "b1"}, nil)
	h.builds.EXPECT().Get(gomock.Any(), "b1").
		Return(entity.Build{ID: "b1", Status: entity.BuildStatusCancelled}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(3), int32(4)).
		DoAndReturn(func(_ context.Context, s entity.SpeculationPathSet, _, _ int32) error {
			assert.Equal(t, entity.SpeculationPathStatusCancelled, s.Paths[0].Status)
			return nil
		})

	require.NoError(t, h.run(head))
	assert.Zero(t, spec.calls, "no head is open, so there is nothing to speculate about")
	h.noBuildsDispatched()
}

// Repeat publishes for one batch must reach the queue. It deduplicates on
// (topic, partition key, message ID) against rows it has not collected yet,
// consumed ones included, so a bare batch ID would silently drop the re-sends
// this controller relies on.
func TestPublish_MintsADistinctMessageIDPerPublish(t *testing.T) {
	ctrl := gomock.NewController(t)

	var ids []string
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			ids = append(ids, msg.ID)
			return nil
		},
	).Times(2)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: q},
	})
	require.NoError(t, err)

	c := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, staticStorageFactory{store: storagemock.NewMockStorage(ctrl)},
		staticSpeculatorFactory{}, registry, topickey.TopicKeySpeculate, "orchestrator-speculate",
	)

	require.NoError(t, c.publishBatchID(context.Background(), topickey.TopicKeyConclude, head, "q", "q"))
	require.NoError(t, c.publishBatchID(context.Background(), topickey.TopicKeyConclude, head, "q", "q"))

	require.Len(t, ids, 2)
	assert.NotEqual(t, ids[0], ids[1])
}

// cascadePair wires a queue where `prerequisite` reaches a terminal outcome
// and `derived` fails only because of it: derived bet that prerequisite would
// not succeed, built on that, and its build failed. Until prerequisite is terminal the
// bet is unresolved and derived waits; once it is, derived has a live path that
// cannot pass.
func cascadePair(t *testing.T, ctrl *gomock.Controller, prerequisiteState entity.BatchState) (*runHarness, entity.Batch, entity.Batch) {
	t.Helper()

	prerequisite := entity.Batch{ID: head, Queue: "q", State: prerequisiteState, Version: 1}
	derived := entity.Batch{
		ID: "q/batch/derived", Queue: "q", State: entity.BatchStateSpeculating,
		Dependencies: []string{head}, Version: 1,
	}

	h := newRunHarness(t, ctrl, &scriptedSpeculator{}, []entity.Batch{prerequisite, derived})

	derivedPath := entity.SpeculationPath{
		Head:         derived.ID,
		Dependencies: []entity.PathDependency{{Batch: head, Assumption: entity.DependencyAssumptionFails}},
	}
	h.pathSets.EXPECT().Get(gomock.Any(), derived.ID).Return(entity.SpeculationPathSet{
		Head:    derived.ID,
		Paths:   []entity.SpeculationPathEntry{entryFor(derivedPath, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)

	return h, prerequisite, derived
}

// An outcome is only a fact once it commits. When the prerequisite's state write
// loses its race, nothing derived from that outcome may be enacted — the winner
// may have written something else entirely. A cancellation loses precisely to a
// merge that got there first, which leaves the batch succeeded, and a dependent
// broken by that success must not already have been failed on the assumption
// it was cancelled.
func TestRun_CascadeStopsWhenThePrerequisiteStateCASLoses(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, prerequisite, derived := cascadePair(t, ctrl, entity.BatchStateCancelling)
	h.noBuildsDispatched()

	h.pathSets.EXPECT().Get(gomock.Any(), prerequisite.ID).
		Return(entity.SpeculationPathSet{}, storage.ErrNotFound)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: prerequisite.ID, state: entity.BatchStateCancelled}, int32(1), int32(2)).
		Return(storage.ErrVersionMismatch)

	// No Update for the dependent: gomock fails the test if one happens.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published, "nothing may be concluded off an outcome that did not commit")
	assert.Contains(t, h.speculatedOver(), derived.ID,
		"the dependency is unresolved in committed state, so the dependent is still open")
}

// The path set and the outcome are one decision. Losing the set's race means
// another writer has moved the head on, so the outcome read off our copy is not
// enacted — and neither is anything derived from it.
func TestRun_CascadeStopsWhenThePrerequisitePathSetCASLoses(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, prerequisite, _ := cascadePair(t, ctrl, entity.BatchStateCancelling)
	h.noBuildsDispatched()

	h.pathSets.EXPECT().Get(gomock.Any(), prerequisite.ID).Return(entity.SpeculationPathSet{
		Head: prerequisite.ID,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 2,
	}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).
		Return(storage.ErrVersionMismatch)

	// No batch Update at all: not for the prerequisite, not for the dependent.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published)
}

// The abort is scoped to what actually depended on the lost outcome. A batch
// this run decided on its own evidence still commits.
func TestRun_UnrelatedOutcomeStillCommitsAfterALostCAS(t *testing.T) {
	ctrl := gomock.NewController(t)

	losing := entity.Batch{ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1}
	independent := entity.Batch{
		ID: "q/batch/independent", Queue: "q", State: entity.BatchStateSpeculating, Version: 1,
	}

	h := newRunHarness(t, ctrl, &scriptedSpeculator{}, []entity.Batch{losing, independent})
	h.noBuildsDispatched()

	h.pathSets.EXPECT().Get(gomock.Any(), losing.ID).
		Return(entity.SpeculationPathSet{}, storage.ErrNotFound)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: losing.ID, state: entity.BatchStateCancelled}, int32(1), int32(2)).
		Return(storage.ErrVersionMismatch)

	// The independent head has no dependencies and one failed path, so its
	// outcome rests on nothing this run recorded.
	ownPath := entity.SpeculationPath{Head: independent.ID}
	h.pathSets.EXPECT().Get(gomock.Any(), independent.ID).Return(entity.SpeculationPathSet{
		Head:    independent.ID,
		Paths:   []entity.SpeculationPathEntry{entryFor(ownPath, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: independent.ID, state: entity.BatchStateFailed}, int32(1), int32(2)).
		Return(nil)

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"speculate", "conclude"}, h.published,
		"the independent batch is not the one on the message, so it is given a "+
			"signal naming it before it is made terminal")
}

// A batch decided inside the run has its set written once, by the same step
// that enacts its outcome — the dispatch step must not come along afterwards and
// try again against the version it has just superseded.
func TestRun_FinalizedBatchPathSetIsWrittenOnce(t *testing.T) {
	ctrl := gomock.NewController(t)

	cancelling := entity.Batch{ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1}
	h := newRunHarness(t, ctrl, &scriptedSpeculator{}, []entity.Batch{cancelling})
	h.noBuildsDispatched()

	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{
			{ID: "p1", Status: entity.SpeculationPathStatusCancelling, Attempt: 1},
		},
		Version: 2,
	}, nil)
	h.pathSets.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).Return(nil).Times(1)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateCancelled}, int32(1), int32(2)).Return(nil)

	require.NoError(t, h.run(head))
}

// A batch this run drove terminal cannot always be repaired by replaying the
// message. The retry names whichever batch woke the run, and once terminal this
// one is gone from the queue listing — so if it was decided by a cascade,
// neither the retry nor the dead-letter fan-out will ever name it.
//
// It is therefore given a message of its own, and given it *before* the state
// write. A signal sent afterwards is one more thing that can fail at the moment
// everything else is failing, which is exactly when it is needed; sent first, a
// failure means nothing was written and the retry re-derives the lot.
func TestRun_CascadeDerivedBatchIsGivenARecoverySignalBeforeItIsTerminal(t *testing.T) {
	ctrl := gomock.NewController(t)

	failing := entity.Batch{ID: "q/batch/derived", Queue: "q", State: entity.BatchStateSpeculating, Version: 1}
	h := newRunHarness(t, ctrl, &scriptedSpeculator{}, []entity.Batch{failing})
	h.noBuildsDispatched()

	failedPath := entity.SpeculationPath{Head: failing.ID}
	h.pathSets.EXPECT().Get(gomock.Any(), failing.ID).Return(entity.SpeculationPathSet{
		Head:    failing.ID,
		Paths:   []entity.SpeculationPathEntry{entryFor(failedPath, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)

	var publishedBeforeWrite []string
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: failing.ID, state: entity.BatchStateFailed}, int32(1), int32(2)).
		DoAndReturn(func(context.Context, entity.Batch, int32, int32) error {
			publishedBeforeWrite = append([]string(nil), h.published...)
			return nil
		})

	// The message names some other batch, so a retry would never come back here.
	require.NoError(t, h.run(head))

	assert.Equal(t, []string{"speculate"}, publishedBeforeWrite,
		"the signal has to exist before the terminal state it recovers")
	assert.Equal(t, []string{"speculate", "conclude"}, h.published)
}

// The batch on the message needs no signal of its own: a retry re-reads it,
// finds it terminal, and re-publishes from the self-heal branch, and a message
// that dead-letters already names the batch the fan-out there repairs.
func TestRun_TriggerBatchNeedsNoRecoverySignal(t *testing.T) {
	ctrl := gomock.NewController(t)

	failing := entity.Batch{ID: head, Queue: "q", State: entity.BatchStateSpeculating, Version: 1}
	h := newRunHarness(t, ctrl, &scriptedSpeculator{}, []entity.Batch{failing})
	h.noBuildsDispatched()

	failedPath := entity.SpeculationPath{Head: head}
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(failedPath, entity.SpeculationPathStatusFailed)},
		Version: 1,
	}, nil)
	h.batches.EXPECT().
		Update(gomock.Any(), updateTo{id: head, state: entity.BatchStateFailed}, int32(1), int32(2)).Return(nil)

	require.NoError(t, h.run(head))
	assert.Equal(t, []string{"conclude"}, h.published)
}

// A cancelling path whose build is still running finishes only when CI actually
// stops. The run writes nothing (the intent is already recorded), publishes
// nothing (the poll loop is what keeps asking the runner to stop), and the
// batch stays out of terminal until the stop is observed.
func TestRun_CancellingPathWithALiveBuildStaysCancelling(t *testing.T) {
	ctrl := gomock.NewController(t)

	cancelling := entity.Batch{ID: head, Queue: "q", State: entity.BatchStateCancelling, Version: 1}
	h := newRunHarness(t, ctrl, &scriptedSpeculator{}, []entity.Batch{cancelling})

	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head: head,
		Paths: []entity.SpeculationPathEntry{{
			ID:      "p1",
			Status:  entity.SpeculationPathStatusCancelling,
			Attempt: 1,
		}},
		Version: 2,
	}, nil)
	h.pathBuilds.EXPECT().Get(gomock.Any(), "p1", 1).
		Return(entity.PathBuild{PathID: "p1", Attempt: 1, BuildID: "b1"}, nil)
	h.builds.EXPECT().Get(gomock.Any(), "b1").
		Return(entity.Build{ID: "b1", Status: entity.BuildStatusRunning}, nil)

	// No path-set write, no batch Update, and no publish: gomock and the
	// published assertion fail the test if any happens.
	require.NoError(t, h.run(head))
	assert.Empty(t, h.published)
}
