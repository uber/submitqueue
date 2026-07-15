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
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	buildfake "github.com/uber/submitqueue/submitqueue/extension/buildrunner/fake"
	buildrunnermock "github.com/uber/submitqueue/submitqueue/extension/buildrunner/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// testBatch returns a standard test batch for build tests.
func testBatch() entity.Batch {
	return entity.Batch{
		ID:      "test-queue/batch/1",
		Queue:   "test-queue",
		State:   entity.BatchStateCreated,
		Version: 1,
	}
}

// newMockStorage wires a MockStorage backed by mock batch/speculation-tree/
// build/path-build sub-stores. Callers set whatever EXPECT() calls each test
// needs on the returned sub-stores.
func newMockStorage(ctrl *gomock.Controller) (*storagemock.MockStorage, *storagemock.MockBatchStore, *storagemock.MockSpeculationTreeStore, *storagemock.MockBuildStore, *storagemock.MockSpeculationPathBuildStore) {
	batchStore := storagemock.NewMockBatchStore(ctrl)
	treeStore := storagemock.NewMockSpeculationTreeStore(ctrl)
	buildStore := storagemock.NewMockBuildStore(ctrl)
	pathBuildStore := storagemock.NewMockSpeculationPathBuildStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationTreeStore().Return(treeStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetSpeculationPathBuildStore().Return(pathBuildStore).AnyTimes()
	return store, batchStore, treeStore, buildStore, pathBuildStore
}

// staticBuildRunnerFactory is a test factory that returns a fixed BuildRunner
// for any queue config.
type staticBuildRunnerFactory struct{ r buildrunner.BuildRunner }

func (f staticBuildRunnerFactory) For(buildrunner.Config) (buildrunner.BuildRunner, error) {
	return f.r, nil
}

// newTestController creates a controller with test dependencies. br is the
// build runner to inject; pass buildfake.New(changesetfake.New()) for the
// pass-through default. publishErr, if non-nil, is returned by every publish
// call. published, if non-nil, accumulates the BuildID of every message
// published to buildsignal, in call order.
//
// The wired registry exposes only the buildsignal topic — that is what the
// controller publishes to.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, br buildrunner.BuildRunner, publishErr error, published *[]string) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			if publishErr != nil {
				return publishErr
			}
			if published != nil {
				bid, err := entity.BuildIDFromBytes(msg.Payload)
				require.NoError(t, err)
				*published = append(*published, bid.ID)
			}
			return nil
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBuildSignal, Name: "buildsignal", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, staticBuildRunnerFactory{r: br}, registry, topickey.TopicKeyBuild, "orchestrator-build")
}

// runProcess builds a delivery for batch and invokes Process once.
func runProcess(t *testing.T, ctrl *gomock.Controller, controller *Controller, batch entity.Batch) error {
	msg := entityqueue.NewMessage(batch.ID, batchIDPayload(t, batch.ID), batch.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return controller.Process(context.Background(), delivery)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, _, _, _, _ := newMockStorage(ctrl)
	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyBuild, controller.TopicKey())
	assert.Equal(t, "orchestrator-build", controller.ConsumerGroup())
	assert.Equal(t, "build", controller.Name())
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, _, _, _, _ := newMockStorage(ctrl)
	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil, nil)

	var _ consumer.Controller = controller
}

// TestController_Process_NoPrioritizedPaths covers an empty tree (or a tree
// with no Prioritized paths): the controller does no work and acks.
func TestController_Process_NoPrioritizedPaths(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	store, batchStore, treeStore, _, _ := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1}, nil)
	// No PathBuildStore.Get expectation: an empty tree has no Prioritized path
	// to dedup-check.

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// No Trigger expectation: a stray build trigger with no Prioritized path
	// fails the test.
	controller := newTestController(t, ctrl, store, br, nil, nil)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
}

// TestController_Process_MultiplePrioritizedPathsTriggerAll covers two
// Prioritized paths with no existing mapping: both must trigger, each with
// its own path's base (in order), and each must persist a Build row and a
// path->build mapping, then publish.
func TestController_Process_MultiplePrioritizedPathsTriggerAll(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	depA := entity.Batch{ID: "test-queue/batch/dep-a", Queue: "test-queue"}
	depB := entity.Batch{ID: "test-queue/batch/dep-b", Queue: "test-queue"}

	pathDirect := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathOnDeps := entity.SpeculationPath{Base: []string{depB.ID, depA.ID}, Head: batch.ID}
	pathDirectID := "test-queue/batch/1/path/0"
	pathOnDepsID := "test-queue/batch/1/path/1"

	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: pathDirectID, Path: pathDirect, Status: entity.SpeculationPathStatusPrioritized},
			{ID: pathOnDepsID, Path: pathOnDeps, Status: entity.SpeculationPathStatusPrioritized},
		},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	batchStore.EXPECT().Get(gomock.Any(), depA.ID).Return(depA, nil).AnyTimes()
	batchStore.EXPECT().Get(gomock.Any(), depB.ID).Return(depB, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathDirectID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathOnDepsID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)

	var createdBuilds []entity.Build
	buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, b entity.Build) error {
			createdBuilds = append(createdBuilds, b)
			return nil
		},
	).Times(2)

	var createdMappings []entity.SpeculationPathBuild
	pathBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pb entity.SpeculationPathBuild) error {
			createdMappings = append(createdMappings, pb)
			return nil
		},
	).Times(2)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// Each path's base is passed to Trigger in the path's own order.
	br.EXPECT().Trigger(gomock.Any(), []entity.Batch(nil), batch, gomock.Nil()).Return(entity.BuildID{ID: "build-direct"}, nil)
	br.EXPECT().Trigger(gomock.Any(), []entity.Batch{depB, depA}, batch, gomock.Nil()).Return(entity.BuildID{ID: "build-on-deps"}, nil)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))

	require.Len(t, createdBuilds, 2)
	gotPathIDs := map[string]string{}
	for _, b := range createdBuilds {
		gotPathIDs[b.ID] = b.SpeculationPathID
	}
	assert.Equal(t, pathDirectID, gotPathIDs["build-direct"])
	assert.Equal(t, pathOnDepsID, gotPathIDs["build-on-deps"])

	require.Len(t, createdMappings, 2)
	gotMappingBuildIDs := map[string]string{}
	for _, pb := range createdMappings {
		gotMappingBuildIDs[pb.PathID] = pb.BuildID
	}
	assert.Equal(t, "build-direct", gotMappingBuildIDs[pathDirectID])
	assert.Equal(t, "build-on-deps", gotMappingBuildIDs[pathOnDepsID])

	assert.ElementsMatch(t, []string{"build-direct", "build-on-deps"}, published)
}

// TestController_Process_ExistingMappingNonTerminalBuildRepublishes covers
// the case where a Prioritized path already has a path->build mapping whose
// build is still non-terminal (redelivery, or a prior partial pass): it must
// not trigger or persist again, but it must republish buildsignal for the
// existing build so a lost original publish is healed.
func TestController_Process_ExistingMappingNonTerminalBuildRepublishes(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized, BuildID: "runner-existing"},
		},
	}
	existingBuild := entity.Build{ID: "runner-existing", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusAccepted}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-existing"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "runner-existing").Return(existingBuild, nil)
	// No Create expectation on either store: an already-mapped, non-terminal
	// path must not trigger or persist again.

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// No Trigger expectation: triggering an already-mapped path fails the test.

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"runner-existing"}, published, "must republish buildsignal for the existing non-terminal build")
}

// TestController_Process_ExistingMappingTerminalBuildNoOp covers the case
// where a Prioritized path's existing mapping resolves to an already-terminal
// build: nothing happens at all, not even a republish.
func TestController_Process_ExistingMappingTerminalBuildNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized, BuildID: "runner-existing"},
		},
	}
	existingBuild := entity.Build{ID: "runner-existing", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusSucceeded}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-existing"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "runner-existing").Return(existingBuild, nil)
	// No Create, no Trigger, and no publish expected: the build already
	// reached a terminal state.

	br := buildrunnermock.NewMockBuildRunner(ctrl)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Empty(t, published)
}

// TestController_Process_MappingDanglingInvariantBreachTolerated covers a
// path whose mapping resolves to a build row that no longer exists — an
// invariant breach (the write order in the trigger flow guarantees a mapping
// is only created once its build row exists). It must not crash, error, or
// trigger a duplicate build for the path; the mapping stays authoritative.
func TestController_Process_MappingDanglingInvariantBreachTolerated(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized, BuildID: "missing-build"},
		},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "missing-build"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "missing-build").Return(entity.Build{}, storage.ErrNotFound)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// No Trigger, no Create, no publish expected — the dangling mapping is
	// authoritative even though its target build is missing.

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Empty(t, published)
}

// TestController_Process_OnlyPrioritizedPathsTrigger covers a tree with
// Selected, Building, and Passed paths alongside one Prioritized path:
// only the Prioritized path triggers.
func TestController_Process_OnlyPrioritizedPathsTrigger(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	selectedPath := entity.SpeculationPath{Base: []string{"test-queue/batch/other-1"}, Head: batch.ID}
	buildingPath := entity.SpeculationPath{Base: []string{"test-queue/batch/other-2"}, Head: batch.ID}
	passedPath := entity.SpeculationPath{Base: []string{"test-queue/batch/other-3"}, Head: batch.ID}
	prioritizedPath := entity.SpeculationPath{Base: nil, Head: batch.ID}
	prioritizedPathID := "test-queue/batch/1/path/3"

	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: "test-queue/batch/1/path/0", Path: selectedPath, Status: entity.SpeculationPathStatusSelected},
			{ID: "test-queue/batch/1/path/1", Path: buildingPath, Status: entity.SpeculationPathStatusBuilding, BuildID: "build-in-flight"},
			{ID: "test-queue/batch/1/path/2", Path: passedPath, Status: entity.SpeculationPathStatusPassed, BuildID: "build-done"},
			{ID: prioritizedPathID, Path: prioritizedPath, Status: entity.SpeculationPathStatusPrioritized},
		},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	// Only the Prioritized path reaches the dedup Get; Selected, Building,
	// and Passed paths continue before it.
	pathBuildStore.EXPECT().Get(gomock.Any(), prioritizedPathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	pathBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// Only the Prioritized path (empty base, since its Path.Base is nil) may trigger.
	br.EXPECT().Trigger(gomock.Any(), []entity.Batch(nil), batch, gomock.Nil()).Return(entity.BuildID{ID: "build-new"}, nil)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"build-new"}, published)
}

// TestController_Process_SpeculationTreeNotFound covers a missing speculation
// tree: the tree exists before any build message is published and is never
// deleted, so a Get miss is an invariant violation. Process must surface the
// error (dead-lettering the message so the DLQ consumer fails the batch
// loudly), not ack.
func TestController_Process_SpeculationTreeNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	store, batchStore, treeStore, _, _ := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound)

	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil, nil)

	err := runProcess(t, ctrl, controller, batch)
	require.Error(t, err)
}

// TestController_Process_SpeculationTreeStorageFailure covers a
// non-ErrNotFound failure reading the speculation tree: it is wrapped and
// returned like other storage failures in this controller.
func TestController_Process_SpeculationTreeStorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	store, batchStore, treeStore, _, _ := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, fmt.Errorf("db connection lost"))

	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil, nil)

	err := runProcess(t, ctrl, controller, batch)
	require.Error(t, err)
}

// TestController_Process_TerminalShortCircuit: a terminal batch must
// short-circuit before touching the speculation tree or build stores: the
// build controller acks without triggering a build in the build system and without
// publishing anything. The cancel flow only writes its terminal state after
// every build has quiesced, and other terminal transitions leave stragglers
// to run out, so there is nothing left for this controller to enact.
func TestController_Process_TerminalShortCircuit(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateCancelled,
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := testBatch()
			batch.State = state
			store, batchStore, _, _, _ := newMockStorage(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
			// No tree/build/path-build store expectations: a terminal batch
			// must never reach the speculation tree or build stores.

			// No Trigger expectation: a stray build trigger on a terminal batch
			// fails the test.
			br := buildrunnermock.NewMockBuildRunner(ctrl)

			// Sentinel publish error: the terminal path must not publish. If it
			// does, Process surfaces this error and require.NoError catches it.
			controller := newTestController(t, ctrl, store, br, fmt.Errorf("should not publish"), nil)

			require.NoError(t, runProcess(t, ctrl, controller, batch))
		})
	}
}

// TestController_Process_CancellingBatchCancelsInFlightNeverTriggers: a
// batch in BatchStateCancelling is being torn down batch-wide, so the loop
// cancels every path's in-flight build regardless of the path's recorded
// status — including a path still recorded as Building, which the tree
// sweep may not have marked Cancelling yet — while never triggering a build.
// Passed paths are skipped without any storage read (their builds are
// terminal by construction), and a path with no build yet is a tolerated
// no-op (the Prioritized path here gets no Trigger and no Create).
func TestController_Process_CancellingBatchCancelsInFlightNeverTriggers(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	batch.State = entity.BatchStateCancelling
	cancellingID := "test-queue/batch/1/path/0"
	buildingID := "test-queue/batch/1/path/1"
	prioritizedID := "test-queue/batch/1/path/2"
	passedID := "test-queue/batch/1/path/3"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 2,
		Paths: []entity.SpeculationPathInfo{
			{ID: cancellingID, Path: entity.SpeculationPath{Head: batch.ID}, Status: entity.SpeculationPathStatusCancelling},
			{ID: buildingID, Path: entity.SpeculationPath{Base: []string{"test-queue/batch/0"}, Head: batch.ID}, Status: entity.SpeculationPathStatusBuilding, BuildID: "runner-2"},
			{ID: prioritizedID, Path: entity.SpeculationPath{Head: batch.ID}, Status: entity.SpeculationPathStatusPrioritized},
			{ID: passedID, Path: entity.SpeculationPath{Head: batch.ID}, Status: entity.SpeculationPathStatusPassed, BuildID: "runner-4"},
		},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	// The Cancelling and Building paths resolve to live builds and are both
	// cancelled; the Prioritized path has no mapping (nothing to stop); the
	// Passed path is never even looked up.
	pathBuildStore.EXPECT().Get(gomock.Any(), cancellingID).Return(entity.SpeculationPathBuild{PathID: cancellingID, BuildID: "runner-1"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{
		ID: "runner-1", BatchID: batch.ID, SpeculationPathID: cancellingID, Status: entity.BuildStatusRunning,
	}, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), buildingID).Return(entity.SpeculationPathBuild{PathID: buildingID, BuildID: "runner-2"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "runner-2").Return(entity.Build{
		ID: "runner-2", BatchID: batch.ID, SpeculationPathID: buildingID, Status: entity.BuildStatusRunning,
	}, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), prioritizedID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: "runner-1"}).Return(nil)
	br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: "runner-2"}).Return(nil)
	// No Trigger expectation: a stray build trigger fails the test.

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"runner-1", "runner-2"}, published, "each enacted cancel must republish buildsignal so the poll loop observes the stop")
}

// TestController_Process_CreateAlreadyExistsTolerated covers the redelivery
// case for a single path: the Build row Create returns ErrAlreadyExists (a
// build row can pre-exist from a prior partial pass), but the path->build
// mapping Create still succeeds (no concurrent winner), so the controller
// proceeds to publish to buildsignal. The polling loop will pick up the
// existing row via UpdateStatus.
func TestController_Process_CreateAlreadyExistsTolerated(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized}},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	pathBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(entity.BuildID{ID: "build-dup"}, nil)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"build-dup"}, published, "publish to buildsignal must run even when the Build row Create reports ErrAlreadyExists")
}

// TestController_Process_PathBuildCreateRaceLostRepublishesWinner covers a
// concurrent delivery winning the path->build mapping race: our own Trigger
// and Build Create succeed, but the mapping Create loses to a winner. The
// controller must not error — it re-reads the mapping and republishes
// buildsignal for the winner's build, not our own.
func TestController_Process_PathBuildCreateRaceLostRepublishesWinner(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized}},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	pathBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "winner-build-id"}, nil)
	winnerBuild := entity.Build{ID: "winner-build-id", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusAccepted}
	buildStore.EXPECT().Get(gomock.Any(), "winner-build-id").Return(winnerBuild, nil)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(entity.BuildID{ID: "our-losing-build"}, nil)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"winner-build-id"}, published, "must republish buildsignal for the mapping race winner, not our own build")
}

// TestController_Process_TriggerFailure verifies a build-runner failure for
// a Prioritized path is surfaced as an error, and neither Create is
// ever called for that path.
func TestController_Process_TriggerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized}},
	}

	store, batchStore, treeStore, _, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	// No Create expectation on either store: a Trigger failure must
	// short-circuit before either Create.

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Trigger(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.BuildID{}, fmt.Errorf("provider down"))

	controller := newTestController(t, ctrl, store, br, nil, nil)

	require.Error(t, runProcess(t, ctrl, controller, batch))
}

// TestController_Process_EmptyBasePath is the trigger-flow happy path for a
// single Prioritized path with no existing mapping and a nil base (it builds
// directly on the target). The ordering Trigger -> Build Create -> mapping
// Create -> publish is load-bearing for crash-safety, so it is pinned down
// with gomock.InOrder.
func TestController_Process_EmptyBasePath(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized}},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	trigger := br.EXPECT().Trigger(gomock.Any(), []entity.Batch(nil), batch, gomock.Nil()).Return(entity.BuildID{ID: "build-no-base"}, nil)

	var capturedMapping entity.SpeculationPathBuild
	buildCreate := buildStore.EXPECT().Create(gomock.Any(), entity.Build{
		ID:                "build-no-base",
		BatchID:           batch.ID,
		SpeculationPathID: pathID,
		Status:            entity.BuildStatusAccepted,
	}).Return(nil)
	mappingCreate := pathBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, pb entity.SpeculationPathBuild) error {
			capturedMapping = pb
			return nil
		},
	)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	gomock.InOrder(trigger, buildCreate, mappingCreate)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"build-no-base"}, published)
	assert.Equal(t, pathID, capturedMapping.PathID)
	assert.Equal(t, "build-no-base", capturedMapping.BuildID)
}

// TestController_Process_BatchStorageFailure covers a failure loading the
// head batch itself: the error is wrapped, non-retryable by default, and the
// speculation tree is never consulted.
func TestController_Process_BatchStorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	store, batchStore, _, _, _ := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("db connection lost"))
	// No tree/build/path-build store expectations: a batch load failure must
	// short-circuit before the speculation tree is consulted.

	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), nil, nil)

	err := runProcess(t, ctrl, controller, entity.Batch{ID: "test-queue/batch/1", Queue: "test-queue"})
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

// TestController_Process_PublishFailure covers a publish failure for a
// triggered path: the error propagates, even though Trigger and both Creates
// succeeded.
func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPrioritized}},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	pathBuildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	controller := newTestController(t, ctrl, store, buildfake.New(changesetfake.New()), fmt.Errorf("publish failed"), nil)

	err := runProcess(t, ctrl, controller, batch)
	assert.Error(t, err)
}

// TestController_Process_CancellingNonTerminalBuildEnactsCancel covers the
// intent/enactment split (D1/D2/D4): a Cancelling path with a non-terminal
// build must have its build runner Cancel called and buildsignal republished
// so the poll loop observes the cancellation promptly. Cancel decisions are
// recorded elsewhere (prioritize's preemption, speculate's reconcile) as
// intent only; this controller is the sole owner of the runner call.
func TestController_Process_CancellingNonTerminalBuildEnactsCancel(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling}},
	}
	inFlightBuild := entity.Build{ID: "build-in-flight", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusRunning}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "build-in-flight"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "build-in-flight").Return(inFlightBuild, nil)
	// No Trigger, no Create on either store: a Cancelling path never triggers.

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: "build-in-flight"}).Return(nil)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Equal(t, []string{"build-in-flight"}, published, "must republish buildsignal after enacting the cancel")
}

// TestController_Process_CancellingTerminalBuildNoOp covers a Cancelling path
// whose build has already reached a terminal status (e.g. it finished
// naturally between the cancel decision and this pass): nothing to cancel,
// so Cancel must never be called and nothing is published.
func TestController_Process_CancellingTerminalBuildNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling}},
	}
	doneBuild := entity.Build{ID: "build-done", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusSucceeded}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "build-done"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "build-done").Return(doneBuild, nil)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// No Cancel expectation: a terminal build must not be cancelled.

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Empty(t, published)
}

// TestController_Process_CancellingNoMappingSkips covers a Cancelling path
// with no path->build mapping yet (e.g. the intent was recorded before this
// controller ever triggered a build for the path): there is nothing to
// cancel, so the pass is a silent no-op left for speculate's reconcile to
// settle.
func TestController_Process_CancellingNoMappingSkips(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling}},
	}

	store, batchStore, treeStore, _, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	// No BuildStore.Get, no Cancel: nothing to look up or cancel.

	br := buildrunnermock.NewMockBuildRunner(ctrl)

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Empty(t, published)
}

// TestController_Process_CancellingMappingDanglingSkips covers a Cancelling
// path whose mapping resolves to a build row that no longer exists (the same
// invariant-breach defense as the trigger flow): tolerated as a no-op rather
// than an error or a fresh trigger.
func TestController_Process_CancellingMappingDanglingSkips(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling}},
	}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "missing-build"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "missing-build").Return(entity.Build{}, storage.ErrNotFound)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	// No Cancel expectation — the dangling mapping is authoritative even
	// though its target build is missing.

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.NoError(t, runProcess(t, ctrl, controller, batch))
	assert.Empty(t, published)
}

// TestController_Process_CancelErrorSurfaces covers a runner Cancel failure:
// it must surface as an error (never a silent ack), and buildsignal must not
// be republished for a cancel that never completed.
func TestController_Process_CancelErrorSurfaces(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	path := entity.SpeculationPath{Base: nil, Head: batch.ID}
	pathID := "test-queue/batch/1/path/0"
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling}},
	}
	inFlightBuild := entity.Build{ID: "build-in-flight", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusRunning}

	store, batchStore, treeStore, buildStore, pathBuildStore := newMockStorage(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "build-in-flight"}, nil)
	buildStore.EXPECT().Get(gomock.Any(), "build-in-flight").Return(inFlightBuild, nil)

	br := buildrunnermock.NewMockBuildRunner(ctrl)
	br.EXPECT().Cancel(gomock.Any(), entity.BuildID{ID: "build-in-flight"}).Return(fmt.Errorf("runner unreachable"))

	var published []string
	controller := newTestController(t, ctrl, store, br, nil, &published)

	require.Error(t, runProcess(t, ctrl, controller, batch))
	assert.Empty(t, published, "must not republish buildsignal when the cancel itself failed")
}
