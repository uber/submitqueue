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

type runHarness struct {
	controller *Controller
	store      *storagemock.MockStorage
	batches    *storagemock.MockBatchStore
	pathSets   *storagemock.MockSpeculationPathSetStore
	pathBuilds *storagemock.MockPathBuildStore
	builds     *storagemock.MockBuildStore
	spec       *scriptedSpeculator
	published  []string
	// messages are the published messages themselves, so a test can assert
	// what a publish carried and how it was partitioned.
	messages []entityqueue.Message
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

	h.pathSets = storagemock.NewMockSpeculationPathSetStore(ctrl)
	h.pathBuilds = storagemock.NewMockPathBuildStore(ctrl)
	h.builds = storagemock.NewMockBuildStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	h.store = store
	store.EXPECT().GetBatchStore().Return(h.batches).AnyTimes()
	store.EXPECT().GetQueueBatchStateStore().Return(queueStates).AnyTimes()
	store.EXPECT().GetSpeculationPathSetStore().Return(h.pathSets).AnyTimes()
	store.EXPECT().GetPathBuildStore().Return(h.pathBuilds).AnyTimes()
	store.EXPECT().GetBuildStore().Return(h.builds).AnyTimes()

	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
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
			assert.Equal(t, path.ID(), set.Paths[0].ID)
			assert.Equal(t, entity.SpeculationPathStatusPending, set.Paths[0].Status)
			assert.Equal(t, 1, set.Paths[0].Attempt)
			assert.Equal(t, int32(1), set.Version)
			return nil
		})

	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
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

	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))

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

	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))

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
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionIgnored)
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

	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
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
	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
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
	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))

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
	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
	assert.Equal(t, []string{"build"}, h.published)
}

// Losing the path-set race is not an error: the head is simply re-planned next
// run, and the rest of the queue is unaffected.
func TestRun_LostCASIsNotAnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionIgnored)
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

	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
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

	require.Error(t, h.controller.run(context.Background(), h.store, "q"))
}

// A queue with nothing speculating never reaches the Speculator.
func TestRun_NoSpeculatingBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, nil)
	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
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

			require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
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
	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
	assert.Empty(t, h.published)
}

// A finished path is never looked up again: its outcome is already in the set.
func TestRun_DoesNotReReadFinishedPaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	path := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionFails)
	spec := &scriptedSpeculator{}

	h := newRunHarness(t, ctrl, spec, []entity.Batch{speculatingHead()})
	h.batches.EXPECT().Get(gomock.Any(), dep1).Return(entity.Batch{ID: dep1, State: entity.BatchStateSucceeded}, nil)
	h.batches.EXPECT().Get(gomock.Any(), dep2).Return(entity.Batch{ID: dep2, State: entity.BatchStateFailed}, nil)
	h.pathSets.EXPECT().Get(gomock.Any(), head).Return(entity.SpeculationPathSet{
		Head:    head,
		Paths:   []entity.SpeculationPathEntry{entryFor(path, entity.SpeculationPathStatusPassed)},
		Version: 1,
	}, nil)

	// The default pathBuilds expectation is AnyTimes, so assert on the build
	// store: a finished entry must not reach it.
	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))
}

// Broken paths are cancelled before the Speculator is asked, so it reasons over the
// queue as the facts have already left it. Asking first would have it propose
// work on top of a path this run is about to rule out, which check would only
// throw away.
func TestRun_BrokenPathsAreVisibleToTheSpeculator(t *testing.T) {
	ctrl := gomock.NewController(t)
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionIgnored)
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

	require.NoError(t, h.controller.run(context.Background(), h.store, "q"))

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
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionIgnored)
	intact := pathOver(entity.DependencyAssumptionFails, entity.DependencyAssumptionIgnored)

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
	broken := pathOver(entity.DependencyAssumptionSucceeds, entity.DependencyAssumptionIgnored)
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
