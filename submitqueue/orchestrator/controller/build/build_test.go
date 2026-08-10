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

package build

import (
	"context"
	"fmt"
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
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildrunnermock "github.com/uber/submitqueue/submitqueue/extension/buildrunner/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

const (
	headID = "test-queue/batch/head"
	depA   = "test-queue/batch/depA"
	depB   = "test-queue/batch/depB"
	depC   = "test-queue/batch/depC"
)

// staticBuildRunnerFactory is a test factory that returns a fixed BuildRunner.
type staticBuildRunnerFactory struct{ r buildrunner.BuildRunner }

func (f staticBuildRunnerFactory) For(buildrunner.Config) (buildrunner.BuildRunner, error) {
	return f.r, nil
}

func batchIDPayload(t *testing.T, id string) []byte {
	t.Helper()
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// headBatch returns the head batch in the given state, depending on all three deps.
func headBatch(state entity.BatchState) entity.Batch {
	return entity.Batch{
		ID:           headID,
		Queue:        "test-queue",
		State:        state,
		Dependencies: []string{depA, depB, depC},
		Version:      1,
	}
}

// pathEntry builds one path-set entry for the head, assuming depA succeeds
// while depB and depC fail — so only depA belongs in the build base.
func pathEntry(status entity.SpeculationPathStatus, attempt int) entity.SpeculationPathEntry {
	path := entity.SpeculationPath{
		Head: headID,
		Dependencies: []entity.PathDependency{
			{Batch: depA, Assumption: entity.DependencyAssumptionSucceeds},
			{Batch: depB, Assumption: entity.DependencyAssumptionFails},
			{Batch: depC, Assumption: entity.DependencyAssumptionFails},
		},
	}
	return entity.SpeculationPathEntry{
		ID:      path.ID(),
		Path:    path,
		Status:  status,
		Attempt: attempt,
		Version: 1,
	}
}

// testDeps holds the mocks a test may want to set expectations on.
type testDeps struct {
	store      *storagemock.MockStorage
	batches    *storagemock.MockBatchStore
	pathSets   *storagemock.MockSpeculationPathSetStore
	builds     *storagemock.MockBuildStore
	pathBuilds *storagemock.MockPathBuildStore
	runner     *buildrunnermock.MockBuildRunner
	publisher  *queuemock.MockPublisher
}

// newTestController wires a controller over fresh mocks. The batch store answers
// Get for the head and every dependency; everything else is left to the test.
func newTestController(t *testing.T, ctrl *gomock.Controller, batch entity.Batch) (*Controller, *testDeps) {
	t.Helper()

	batches := storagemock.NewMockBatchStore(ctrl)
	batches.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	for _, dep := range []string{depA, depB, depC} {
		batches.EXPECT().Get(gomock.Any(), dep).
			Return(entity.Batch{ID: dep, Queue: batch.Queue, State: entity.BatchStateSucceeded}, nil).AnyTimes()
	}

	pathSets := storagemock.NewMockSpeculationPathSetStore(ctrl)
	builds := storagemock.NewMockBuildStore(ctrl)
	pathBuilds := storagemock.NewMockPathBuildStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batches).AnyTimes()
	store.EXPECT().GetSpeculationPathSetStore().Return(pathSets).AnyTimes()
	store.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	store.EXPECT().GetPathBuildStore().Return(pathBuilds).AnyTimes()

	publisher := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: q}},
	)
	require.NoError(t, err)

	runner := buildrunnermock.NewMockBuildRunner(ctrl)

	controller := NewController(
		zaptest.NewLogger(t).Sugar(), tally.NoopScope, staticStorageFactory{store: store},
		staticBuildRunnerFactory{r: runner}, registry, topickey.TopicKeyBuild, "orchestrator-build",
	)

	return controller, &testDeps{
		store: store, batches: batches, pathSets: pathSets,
		builds: builds, pathBuilds: pathBuilds,
		runner: runner, publisher: publisher,
	}
}

// processAttempt delivers the head's batch ID with the given delivery attempt.
func processAttempt(t *testing.T, ctrl *gomock.Controller, c *Controller, attempt int) error {
	t.Helper()
	msg := entityqueue.NewMessage("msg-1", batchIDPayload(t, headID), "test-queue", nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(attempt).AnyTimes()
	return c.Process(context.Background(), d)
}

// process delivers the head's batch ID as a first delivery.
func process(t *testing.T, ctrl *gomock.Controller, c *Controller) error {
	t.Helper()
	return processAttempt(t, ctrl, c, 1)
}

// expectSignal expects one buildsignal publish for the given build, asserting
// the payload carries the build ID and the message partitions on it.
func expectSignal(t *testing.T, deps *testDeps, buildID string) {
	t.Helper()
	deps.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			got, err := entity.BuildIDFromBytes(msg.Payload)
			require.NoError(t, err)
			assert.Equal(t, buildID, got.ID)
			assert.Equal(t, buildID, msg.PartitionKey,
				"polls partition per build so one slow build cannot block a head's others")
			return nil
		})
}

// notDispatched makes the reverse lookup miss, i.e. this attempt has no build yet.
func notDispatched(deps *testDeps) {
	deps.pathBuilds.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.PathBuild{}, storage.ErrNotFound).AnyTimes()
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))

	assert.Equal(t, topickey.TopicKeyBuild, c.TopicKey())
	assert.Equal(t, "orchestrator-build", c.ConsumerGroup())
	assert.Equal(t, "build", c.Name())
}

// TestProcess_TriggersWithThePathsBase is the behavioral heart of
// speculation: the build base is the dependencies the path assumes succeed, not
// the head's full dependency list. depB is assumed to fail and depC is ignored,
// so neither may appear.
//
// It also pins the write order — Trigger, Build record, link, signal — because
// each write is what makes the previous one reachable: the link must never name
// a build that has no record, and a signal must never be sent for a build the
// link does not yet make findable.
func TestProcess_TriggersWithThePathsBase(t *testing.T) {
	ctrl := gomock.NewController(t)
	batch := headBatch(entity.BatchStateSpeculating)
	c, deps := newTestController(t, ctrl, batch)

	entry := pathEntry(entity.SpeculationPathStatusPending, 1)
	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 4,
	}, nil)
	notDispatched(deps)

	wantBase := []entity.Batch{{ID: depA, Queue: "test-queue", State: entity.BatchStateSucceeded}}

	gomock.InOrder(
		deps.runner.EXPECT().
			Trigger(gomock.Any(), wantBase, batch, nil).
			Return(entity.BuildID{ID: "build-1"}, nil),
		deps.builds.EXPECT().Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, b entity.Build) error {
				assert.Equal(t, "build-1", b.ID)
				assert.Equal(t, headID, b.BatchID)
				assert.Equal(t, entry.ID, b.PathID)
				assert.Equal(t, 1, b.Attempt)
				return nil
			}),
		deps.pathBuilds.EXPECT().Create(gomock.Any(), entity.PathBuild{
			Queue: "test-queue", PathID: entry.ID, Attempt: 1, BuildID: "build-1",
		}).Return(nil),
		deps.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
				got, err := entity.BuildIDFromBytes(msg.Payload)
				require.NoError(t, err)
				assert.Equal(t, "build-1", got.ID)
				assert.Equal(t, "build-1", msg.PartitionKey,
					"polls partition per build so one slow build cannot block a head's others")
				return nil
			}),
	)

	require.NoError(t, process(t, ctrl, c))
}

// The path set belongs to the speculate run. This stage must never write it, or
// speculate loses compare-and-swap races across its far longer window. And a
// cancelling entry is not this stage's work at all on an ordinary delivery —
// the poll loop stops unwanted builds — so it must not even be looked up.
func TestProcess_NeverWritesThePathSetAndIgnoresCancelling(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))

	pending := pathEntry(entity.SpeculationPathStatusPending, 1)
	cancelling := pathEntry(entity.SpeculationPathStatusCancelling, 1)
	cancelling.ID = "cancelling-path"

	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head: headID, Paths: []entity.SpeculationPathEntry{pending, cancelling}, Version: 2,
	}, nil)

	deps.pathBuilds.EXPECT().Get(gomock.Any(), pending.ID, 1).
		Return(entity.PathBuild{}, storage.ErrNotFound)

	deps.runner.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{ID: "build-1"}, nil)
	deps.builds.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	deps.pathBuilds.EXPECT().Create(gomock.Any(), entity.PathBuild{
		Queue: "test-queue", PathID: pending.ID, Attempt: 1, BuildID: "build-1",
	}).Return(nil)
	expectSignal(t, deps, "build-1")

	// No pathSets.Update, no pathSets.Create, no runner.Cancel, and no lookup of
	// the cancelling path: gomock fails the test on any of them.
	require.NoError(t, process(t, ctrl, c))
}

// A redelivered dispatch must re-publish the existing build's signal rather
// than start a second build for the same attempt.
func TestProcess_RedeliveryDoesNotRebuild(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))

	entry := pathEntry(entity.SpeculationPathStatusPending, 1)
	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
	}, nil)
	deps.pathBuilds.EXPECT().Get(gomock.Any(), entry.ID, 1).
		Return(entity.PathBuild{PathID: entry.ID, Attempt: 1, BuildID: "build-existing"}, nil)

	expectSignal(t, deps, "build-existing")

	// No Trigger and no Creates.
	require.NoError(t, process(t, ctrl, c))
}

// A crash between the link and the signal is the one window the short-circuit
// exists for: both records are present, nothing was ever published, so the
// re-publish is not suppressed by the queue's publish idempotency.
func TestProcess_RepublishesWhenOnlyTheSignalIsMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))

	entry := pathEntry(entity.SpeculationPathStatusPending, 1)
	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
	}, nil)
	deps.pathBuilds.EXPECT().Get(gomock.Any(), entry.ID, 1).
		Return(entity.PathBuild{PathID: entry.ID, Attempt: 1, BuildID: "build-1"}, nil)

	expectSignal(t, deps, "build-1")

	// No Trigger and no writes: the records are already consistent.
	require.NoError(t, process(t, ctrl, c))
}

// Losing the link's first-insert race means another dispatch named this attempt
// first. Both builds are handed to the poll loop — the surplus so it gets
// stopped, the winner because the dispatch that won may have died before
// signalling, and acking this message retires the redelivery that would have
// repaired that.
func TestProcess_LostDispatchRaceHandsBothBuildsToThePollLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))

	entry := pathEntry(entity.SpeculationPathStatusPending, 1)
	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
	}, nil)

	gomock.InOrder(
		deps.pathBuilds.EXPECT().Get(gomock.Any(), entry.ID, 1).
			Return(entity.PathBuild{}, storage.ErrNotFound),
		deps.pathBuilds.EXPECT().Create(gomock.Any(), entity.PathBuild{
			Queue: "test-queue", PathID: entry.ID, Attempt: 1, BuildID: "build-surplus",
		}).Return(storage.ErrAlreadyExists),
		deps.pathBuilds.EXPECT().Get(gomock.Any(), entry.ID, 1).
			Return(entity.PathBuild{PathID: entry.ID, Attempt: 1, BuildID: "build-winner"}, nil),
	)

	deps.runner.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{ID: "build-surplus"}, nil)
	deps.builds.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	expectSignal(t, deps, "build-surplus")
	expectSignal(t, deps, "build-winner")

	// The race is resolved, not retried: no runner.Cancel here (the poll loop
	// stops the surplus build), and the message acks.
	require.NoError(t, process(t, ctrl, c))
}

// A halted batch gets no new builds. Stopping its running ones is the poll
// loop's job, so on a first delivery there is nothing else to do here.
func TestProcess_HaltedBatchStartsNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateCancelling))

	pending := pathEntry(entity.SpeculationPathStatusPending, 1)
	cancelling := pathEntry(entity.SpeculationPathStatusCancelling, 1)
	cancelling.ID = "cancelling-path"

	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head: headID, Paths: []entity.SpeculationPathEntry{pending, cancelling}, Version: 1,
	}, nil)

	// No lookups, no Trigger, no Cancel, no publishes: gomock fails the test if
	// anything is started or stopped from here.
	require.NoError(t, process(t, ctrl, c))
}

// A redelivery means an earlier attempt died without acking — possibly between
// writing a link and publishing its signal, after which the entry may have
// moved to a state no start would touch. Every live linked path therefore gets
// its signal re-published, so no build is left without a poll chain. The
// re-publish dedups against the original signal whenever it was actually sent.
func TestProcess_RedeliveryRepublishesSignalsForLivePaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateCancelling))

	building := pathEntry(entity.SpeculationPathStatusBuilding, 1)
	building.ID = "building-path"
	cancelling := pathEntry(entity.SpeculationPathStatusCancelling, 1)
	cancelling.ID = "cancelling-path"
	neverDispatched := pathEntry(entity.SpeculationPathStatusCancelling, 1)
	neverDispatched.ID = "never-dispatched-path"
	done := pathEntry(entity.SpeculationPathStatusCancelled, 1)
	done.ID = "done-path"

	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
		Head:  headID,
		Paths: []entity.SpeculationPathEntry{building, cancelling, neverDispatched, done},
	}, nil)

	deps.pathBuilds.EXPECT().Get(gomock.Any(), "building-path", 1).
		Return(entity.PathBuild{PathID: "building-path", Attempt: 1, BuildID: "build-a"}, nil)
	deps.pathBuilds.EXPECT().Get(gomock.Any(), "cancelling-path", 1).
		Return(entity.PathBuild{PathID: "cancelling-path", Attempt: 1, BuildID: "build-b"}, nil)
	deps.pathBuilds.EXPECT().Get(gomock.Any(), "never-dispatched-path", 1).
		Return(entity.PathBuild{}, storage.ErrNotFound)

	expectSignal(t, deps, "build-a")
	expectSignal(t, deps, "build-b")

	// The terminal path is never looked up, the unlinked one publishes nothing,
	// and no build is started or cancelled from here.
	require.NoError(t, processAttempt(t, ctrl, c, 2))
}

// TestProcess_NoPathSet covers a head nothing has speculated on yet.
func TestProcess_NoPathSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateCreated))

	deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{}, storage.ErrNotFound)

	require.NoError(t, process(t, ctrl, c))
}

func TestProcess_Errors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(deps *testDeps, entry entity.SpeculationPathEntry)
	}{
		{
			name: "path set read failure",
			setup: func(deps *testDeps, _ entity.SpeculationPathEntry) {
				deps.pathSets.EXPECT().Get(gomock.Any(), headID).
					Return(entity.SpeculationPathSet{}, fmt.Errorf("connection reset"))
			},
		},
		{
			name: "reverse lookup failure",
			setup: func(deps *testDeps, entry entity.SpeculationPathEntry) {
				deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
					Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
				}, nil)
				deps.pathBuilds.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(entity.PathBuild{}, fmt.Errorf("connection reset"))
			},
		},
		{
			name: "trigger failure",
			setup: func(deps *testDeps, entry entity.SpeculationPathEntry) {
				deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
					Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
				}, nil)
				notDispatched(deps)
				deps.runner.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(entity.BuildID{}, fmt.Errorf("runner unavailable"))
			},
		},
		{
			// The build exists and is recorded, but the link write failed for an
			// infra reason. Nacking is what repairs it: redelivery re-triggers,
			// orphaning build-1 — the accepted cost of not being able to name a
			// build before the runner mints its ID.
			name: "link write failure",
			setup: func(deps *testDeps, entry entity.SpeculationPathEntry) {
				deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
					Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
				}, nil)
				notDispatched(deps)
				deps.runner.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(entity.BuildID{ID: "build-1"}, nil)
				deps.builds.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				deps.pathBuilds.EXPECT().Create(gomock.Any(), gomock.Any()).
					Return(fmt.Errorf("connection reset"))
			},
		},
		{
			// The build was triggered but could not be recorded. The link is
			// never written and nothing is published: a link must never name a
			// build that has no record, and a signal must never point the poll
			// loop at one.
			name: "build record failure",
			setup: func(deps *testDeps, entry entity.SpeculationPathEntry) {
				deps.pathSets.EXPECT().Get(gomock.Any(), headID).Return(entity.SpeculationPathSet{
					Head: headID, Paths: []entity.SpeculationPathEntry{entry}, Version: 1,
				}, nil)
				notDispatched(deps)
				deps.runner.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(entity.BuildID{ID: "build-1"}, nil)
				deps.builds.EXPECT().Create(gomock.Any(), gomock.Any()).
					Return(fmt.Errorf("connection reset"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, deps := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))
			tt.setup(deps, pathEntry(entity.SpeculationPathStatusPending, 1))

			require.Error(t, process(t, ctrl, c))
		})
	}
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newTestController(t, ctrl, headBatch(entity.BatchStateSpeculating))
	var _ consumer.Controller = c
}
