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
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	dependencylimitfake "github.com/uber/submitqueue/submitqueue/extension/speculation/dependencylimit/fake"
	dependencylimitmock "github.com/uber/submitqueue/submitqueue/extension/speculation/dependencylimit/mock"
	enumeratorfake "github.com/uber/submitqueue/submitqueue/extension/speculation/enumerator/fake"
	enumeratormock "github.com/uber/submitqueue/submitqueue/extension/speculation/enumerator/mock"
	scorerfake "github.com/uber/submitqueue/submitqueue/extension/speculation/pathscorer/fake"
	scorermock "github.com/uber/submitqueue/submitqueue/extension/speculation/pathscorer/mock"
	selectorfake "github.com/uber/submitqueue/submitqueue/extension/speculation/selector/fake"
	selectormock "github.com/uber/submitqueue/submitqueue/extension/speculation/selector/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// pubRec records a single publish call for order/content assertions.
type pubRec struct {
	topic string
	msgID string
}

// batchIDPayload serializes a BatchID to JSON bytes for test message payloads.
func batchIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.BatchID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// testBatch returns a standard test batch with the given state and dependencies.
func testBatch(state entity.BatchState, deps ...string) entity.Batch {
	return entity.Batch{
		ID:           "test-queue/batch/1",
		Queue:        "test-queue",
		Dependencies: deps,
		State:        state,
		Version:      1,
	}
}

// testHarness wires a Controller against mocked storage, plus programmable
// fake seam implementations (enumerator, scorer, selector, dependency limit)
// that tests script per case. Every publish is recorded so tests can assert
// both content and order. There is no build runner anywhere in the harness
// because the controller has no runner dependency: every cancel decision —
// applySelection's, reconcile's, and cancelBatch's — is captured as intent
// only, enacted by the build stage.
type testHarness struct {
	controller     *Controller
	batchStore     *storagemock.MockBatchStore
	treeStore      *storagemock.MockSpeculationTreeStore
	buildStore     *storagemock.MockBuildStore
	pathBuildStore *storagemock.MockSpeculationPathBuildStore
	depStore       *storagemock.MockBatchDependentStore
	enum           *enumeratorfake.Enumerator
	scorer         *scorerfake.Scorer
	selector       *selectorfake.Selector
	depLimit       *dependencylimitfake.DependencyLimit
	records        *[]pubRec
}

// newTestHarness builds a harness whose seams default to parity behavior:
// the fake enumerator produces an empty tree unless seeded, the fake scorer
// echoes its input, the fake selector decides nothing, and the fake
// dependency limit is effectively ungated (1000). Every publish succeeds and
// is appended to records.
func newTestHarness(t *testing.T, ctrl *gomock.Controller) *testHarness {
	return newHarness(t, ctrl, nil, "", 1000)
}

// newFailingPublishHarness is identical to newTestHarness except every
// publish returns publishErr instead of succeeding.
func newFailingPublishHarness(t *testing.T, ctrl *gomock.Controller, publishErr error) *testHarness {
	return newHarness(t, ctrl, publishErr, "", 1000)
}

// newTopicFailingPublishHarness is identical to newTestHarness except
// publishes to failTopic return publishErr; publishes to every other topic
// succeed and are recorded.
func newTopicFailingPublishHarness(t *testing.T, ctrl *gomock.Controller, publishErr error, failTopic string) *testHarness {
	return newHarness(t, ctrl, publishErr, failTopic, 1000)
}

// newDependencyLimitHarness is identical to newTestHarness except the
// dependency limit is set to limit instead of the default ungated value.
func newDependencyLimitHarness(t *testing.T, ctrl *gomock.Controller, limit int) *testHarness {
	return newHarness(t, ctrl, nil, "", limit)
}

func newHarness(t *testing.T, ctrl *gomock.Controller, publishErr error, failTopic string, depLimit int) *testHarness {
	batchStore := storagemock.NewMockBatchStore(ctrl)
	treeStore := storagemock.NewMockSpeculationTreeStore(ctrl)
	buildStore := storagemock.NewMockBuildStore(ctrl)
	pathBuildStore := storagemock.NewMockSpeculationPathBuildStore(ctrl)
	depStore := storagemock.NewMockBatchDependentStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationTreeStore().Return(treeStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetSpeculationPathBuildStore().Return(pathBuildStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(depStore).AnyTimes()

	enum := enumeratorfake.New()
	enumFactory := enumeratormock.NewMockFactory(ctrl)
	enumFactory.EXPECT().For(gomock.Any()).Return(enum, nil).AnyTimes()

	scr := scorerfake.New()
	scorerFactory := scorermock.NewMockFactory(ctrl)
	scorerFactory.EXPECT().For(gomock.Any()).Return(scr, nil).AnyTimes()

	sel := selectorfake.New()
	selectorFactory := selectormock.NewMockFactory(ctrl)
	selectorFactory.EXPECT().For(gomock.Any()).Return(sel, nil).AnyTimes()

	dl := dependencylimitfake.New(depLimit)
	depLimitFactory := dependencylimitmock.NewMockFactory(ctrl)
	depLimitFactory.EXPECT().For(gomock.Any()).Return(dl, nil).AnyTimes()

	var records []pubRec
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			if publishErr != nil && (failTopic == "" || topic == failTopic) {
				return publishErr
			}
			records = append(records, pubRec{topic: topic, msgID: msg.ID})
			return nil
		}).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: mockQ},
		{Key: topickey.TopicKeyPrioritize, Name: "prioritize", Queue: mockQ},
		{Key: topickey.TopicKeyMerge, Name: "submitqueue-merge", Queue: mockQ},
		{Key: topickey.TopicKeyConclude, Name: "conclude", Queue: mockQ},
	})
	require.NoError(t, err)

	c := NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		store,
		enumFactory,
		scorerFactory,
		selectorFactory,
		depLimitFactory,
		registry,
		topickey.TopicKeySpeculate,
		"orchestrator-speculate",
	)

	return &testHarness{
		controller:     c,
		batchStore:     batchStore,
		treeStore:      treeStore,
		buildStore:     buildStore,
		pathBuildStore: pathBuildStore,
		depStore:       depStore,
		enum:           enum,
		scorer:         scr,
		selector:       sel,
		depLimit:       dl,
		records:        &records,
	}
}

// runProcess builds a delivery for batchID and invokes Process once.
func runProcess(t *testing.T, ctrl *gomock.Controller, controller *Controller, batchID string) error {
	msg := entityqueue.NewMessage(batchID, batchIDPayload(t, batchID), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return controller.Process(context.Background(), delivery)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	require.NotNil(t, h.controller)
	assert.Equal(t, topickey.TopicKeySpeculate, h.controller.TopicKey())
	assert.Equal(t, "orchestrator-speculate", h.controller.ConsumerGroup())
	assert.Equal(t, "speculate", h.controller.Name())

	var _ consumer.Controller = h.controller
}

func TestController_Process_BadPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	msg := entityqueue.NewMessage("anything", []byte("not-json"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.Error(t, h.controller.Process(context.Background(), delivery))
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)

	h.batchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("db connection lost"))

	err := runProcess(t, ctrl, h.controller, "test-queue/batch/1")
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_UnrecognizedState(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateUnknown)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
}

// Merging is owned by the merge controller — speculate is a no-op for it.
func TestController_Process_MergingNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateMerging)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// No UpdateState, no tree access, no publish expected.

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Empty(t, *h.records)
}

// Terminal states wake dependents (re-publish speculate for each) and then
// re-publish conclude — this is both the normal dependent wake-up (every
// terminal transition is routed back through speculate under the batch's own
// ID) and the crash self-heal for a lost publish. State must not change (no
// UpdateState), and neither BuildStore nor SpeculationTreeStore is touched.
func TestController_Process_TerminalSelfHeals(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateSucceeded,
		entity.BatchStateFailed,
		entity.BatchStateCancelled,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)
			batch := testBatch(state)

			h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			// No UpdateState, no tree access expected.
			h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
				BatchID:    batch.ID,
				Dependents: []string{"test-queue/batch/2", "test-queue/batch/3"},
				Version:    1,
			}, nil)

			require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
			assert.Equal(t, []pubRec{
				{topic: "speculate", msgID: "test-queue/batch/2"},
				{topic: "speculate", msgID: "test-queue/batch/3"},
				{topic: "conclude", msgID: batch.ID},
			}, *h.records)
		})
	}
}

// An empty dependents list publishes nothing extra beyond conclude. The
// BatchDependent row itself must still exist for the batch — a missing row
// is a storage invariant violation surfaced as an error, not a normal
// "no dependents" case.
func TestController_Process_TerminalSelfHealNoDependents(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateSucceeded)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: nil,
		Version:    1,
	}, nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "conclude", msgID: batch.ID}}, *h.records)
}

// A missing BatchDependent row is a storage invariant violation (the batch
// controller creates the row before the batch itself), surfaced as an error
// so the message nacks — the fan-out must not be silently skipped.
func TestController_Process_TerminalMissingDependentRowErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateSucceeded)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{}, storage.ErrNotFound)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Empty(t, *h.records)
}

// Created batch: no tree yet -> enumerate, stamp Candidate, mint path IDs,
// Version 1, Create. Reconcile is a no-op (no build mapping yet). Score
// echoes. Select promotes the lone path to Selected -> tree changed ->
// Update(1,2,...). The forward step then CASes Created -> Speculating and
// publishes the queue to prioritize. No path is mergeable yet (Selected, not
// Passed) and the lone path is still viable, so finalize just waits.
func TestController_Process_CreatedEnumeratesScoresSelects(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCreated)
	path := entity.SpeculationPath{Head: batch.ID}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound)

	h.enum.Set(batch.ID, []entity.SpeculationPath{path})
	h.treeStore.EXPECT().Create(gomock.Any(), gomock.AssignableToTypeOf(entity.SpeculationTree{})).
		DoAndReturn(func(_ context.Context, tree entity.SpeculationTree) error {
			require.Len(t, tree.Paths, 1)
			assert.Equal(t, entity.SpeculationPathStatusCandidate, tree.Paths[0].Status)
			assert.Equal(t, fmt.Sprintf("%s/path/0", batch.ID), tree.Paths[0].ID)
			assert.Equal(t, int32(1), tree.Version)
			return nil
		})
	h.pathBuildStore.EXPECT().Get(gomock.Any(), fmt.Sprintf("%s/path/0", batch.ID)).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)

	h.selector.SetDecisions(entity.PathDecision{PathID: fmt.Sprintf("%s/path/0", batch.ID), Action: entity.SpeculationPathActionPromote})
	h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
			require.Len(t, paths, 1)
			assert.Equal(t, entity.SpeculationPathStatusSelected, paths[0].Status)
			return nil
		})
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// TestController_Process_CreateTreeMintsPathIDs pins down the exact minted
// path ID format: fmt.Sprintf("%s/path/%d", batch.ID, i), by enumeration
// index.
func TestController_Process_CreateTreeMintsPathIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCreated)
	pathA := entity.SpeculationPath{Head: batch.ID}
	pathB := entity.SpeculationPath{Base: []string{"test-queue/batch/0"}, Head: batch.ID}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound)

	h.enum.Set(batch.ID, []entity.SpeculationPath{pathA, pathB})
	h.treeStore.EXPECT().Create(gomock.Any(), gomock.AssignableToTypeOf(entity.SpeculationTree{})).
		DoAndReturn(func(_ context.Context, tree entity.SpeculationTree) error {
			require.Len(t, tree.Paths, 2)
			for i, p := range tree.Paths {
				assert.Equal(t, fmt.Sprintf("%s/path/%d", batch.ID, i), p.ID)
			}
			return nil
		})
	h.pathBuildStore.EXPECT().Get(gomock.Any(), fmt.Sprintf("%s/path/0", batch.ID)).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), fmt.Sprintf("%s/path/1", batch.ID)).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// Create racing with a concurrent creator: ErrAlreadyExists must fall back to
// re-reading the winner's tree rather than erroring. The forward step still
// runs afterward.
func TestController_Process_CreateTreeAlreadyExistsRereads(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCreated)
	path := entity.SpeculationPath{Head: batch.ID}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	existing := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusCandidate}},
	}
	gomock.InOrder(
		h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound),
		h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(existing, nil),
	)
	h.enum.Set(batch.ID, []entity.SpeculationPath{path})
	h.treeStore.EXPECT().Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tree entity.SpeculationTree) error {
			require.Len(t, tree.Paths, 1)
			assert.NotEmpty(t, tree.Paths[0].ID, "minted path ID must not be empty")
			return storage.ErrAlreadyExists
		})
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	// No changes this pass (selector decides nothing) -> no Update call.
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// Dependency gate: too many active dependencies for a batch with no tree yet
// blocks enumeration entirely (ack without creating a tree, no forward step
// either). A later dependency-terminal event re-triggers speculate.
func TestController_Process_DependencyGateBlocksTreeCreation(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newDependencyLimitHarness(t, ctrl, 1)

	depA := entity.Batch{ID: "test-queue/batch/0a", Queue: "test-queue", State: entity.BatchStateCreated, Version: 1}
	depB := entity.Batch{ID: "test-queue/batch/0b", Queue: "test-queue", State: entity.BatchStateScored, Version: 1}
	batch := testBatch(entity.BatchStateCreated, depA.ID, depB.ID)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), depA.ID).Return(depA, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), depB.ID).Return(depB, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound)
	// No Create, no Enumerate call, no UpdateState expected — the mocks fail
	// the test if called.

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Empty(t, *h.records)
}

// A pass over an unchanged tree must not call Update, but prioritize is still
// published every pass regardless of whether anything changed.
func TestController_Process_NoChangeSkipsTreeUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateSpeculating)
	path := entity.SpeculationPath{Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 4,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	// Fake scorer echoes, fake selector decides nothing -> no change.
	// No treeStore.Update, no batchStore.UpdateState expected (already Speculating).

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// A version mismatch on the speculation tree Update must surface as an error
// (nack; the round is recomputed on redelivery) and must not reach finalize.
func TestController_Process_TreeUpdateVersionMismatchErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateSpeculating)
	path := entity.SpeculationPath{Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusCandidate}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	h.selector.SetDecisions(entity.PathDecision{PathID: tree.Paths[0].ID, Action: entity.SpeculationPathActionPromote})
	h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).Return(storage.ErrVersionMismatch)
	// finalize must never run: no batchStore.UpdateState, no merge publish.

	err := runProcess(t, ctrl, h.controller, batch.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
	assert.Empty(t, *h.records)
}

// Merge gate: a Passed path with every base dependency landed (or cancelled)
// and every non-base dependency ruled out merges immediately; a pending
// non-terminal base dependency waits; a path whose own build has not yet
// passed waits even if every dependency has resolved favorably.
func TestController_Process_MergeGate(t *testing.T) {
	tests := []struct {
		name       string
		pathStatus entity.SpeculationPathStatus
		depState   entity.BatchState
		wantMerge  bool
	}{
		{name: "passed_and_base_dep_succeeded_merges", pathStatus: entity.SpeculationPathStatusPassed, depState: entity.BatchStateSucceeded, wantMerge: true},
		// A base dependency merely published for merge (Merging) must NOT
		// count as landed: the queue reorders across nack backoff, so an
		// optimistic dependent could overtake its base on the way to runway.
		{name: "passed_and_base_dep_merging_waits", pathStatus: entity.SpeculationPathStatusPassed, depState: entity.BatchStateMerging, wantMerge: false},
		{name: "passed_and_base_dep_pending_waits", pathStatus: entity.SpeculationPathStatusPassed, depState: entity.BatchStateSpeculating, wantMerge: false},
		{name: "building_and_base_dep_succeeded_waits", pathStatus: entity.SpeculationPathStatusBuilding, depState: entity.BatchStateSucceeded, wantMerge: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)
			dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: tt.depState, Version: 1}
			batch := testBatch(entity.BatchStateSpeculating, dep.ID)
			path := entity.SpeculationPath{Base: []string{dep.ID}, Head: batch.ID}
			tree := entity.SpeculationTree{
				BatchID: batch.ID,
				Version: 2,
				Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: tt.pathStatus, BuildID: "build-1"}},
			}

			h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
			h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
			if !isTerminalPathStatus(tt.pathStatus) {
				// Terminal paths are settled: reconcile skips their
				// path->build read entirely.
				h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
			}

			if tt.wantMerge {
				h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)
			}

			require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))

			wantRecords := []pubRec{{topic: "prioritize", msgID: "test-queue"}}
			if tt.wantMerge {
				wantRecords = append(wantRecords, pubRec{topic: "submitqueue-merge", msgID: batch.ID})
			}
			assert.ElementsMatch(t, wantRecords, *h.records)
		})
	}
}

// A Cancelled base dependency is tolerated by mergeableNow exactly like
// Succeeded — parity with today's chain semantics (the cancelled batch never
// lands, so it cannot conflict).
func TestController_Process_CancelledBaseDepToleratedMerges(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateCancelled, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)
	path := entity.SpeculationPath{Base: []string{dep.ID}, Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusPassed, BuildID: "build-1"}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	// No path->build read: the path is already terminal (Passed), so
	// reconcile skips it without touching storage.
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.ElementsMatch(t, []pubRec{
		{topic: "prioritize", msgID: "test-queue"},
		{topic: "submitqueue-merge", msgID: batch.ID},
	}, *h.records)
}

// A failed base dependency deads the path: a Building path is captured as a
// cancel intent (Cancelling) with no runner call (D1/D2) — nothing in this
// file talks to a build runner. Enactment is the build stage's job, reached
// via the prioritize round speculate publishes every pass. A pre-build path
// drops straight to Cancelled. Either way, with no viable path left the
// batch fails and conclude is published.
func TestController_Process_FailedBaseDepDeadsPath(t *testing.T) {
	tests := []struct {
		name       string
		pathStatus entity.SpeculationPathStatus
		wantStatus entity.SpeculationPathStatus
	}{
		{name: "building_path_marks_cancelling", pathStatus: entity.SpeculationPathStatusBuilding, wantStatus: entity.SpeculationPathStatusCancelling},
		{name: "pre_build_path_drops_to_cancelled", pathStatus: entity.SpeculationPathStatusCandidate, wantStatus: entity.SpeculationPathStatusCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)
			dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateFailed, Version: 1}
			batch := testBatch(entity.BatchStateSpeculating, dep.ID)
			path := entity.SpeculationPath{Base: []string{dep.ID}, Head: batch.ID}
			tree := entity.SpeculationTree{
				BatchID: batch.ID,
				Version: 2,
				Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: tt.pathStatus}},
			}

			h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
			h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
			h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
			// No runner interaction expected regardless of pathStatus: reconcile
			// only ever records intent (D1/D2).
			h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(2), int32(3), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
					require.Len(t, paths, 1)
					assert.Equal(t, tt.wantStatus, paths[0].Status)
					return nil
				})
			h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateFailed).Return(nil)
			h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
				BatchID: batch.ID,
				Version: 1,
			}, nil)

			require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
			assert.ElementsMatch(t, []pubRec{
				{topic: "prioritize", msgID: batch.Queue},
				{topic: "conclude", msgID: batch.ID},
			}, *h.records)
		})
	}
}

// The path's own build failing (independent of any dependency) fails the
// path and, with no other path viable, fails the batch.
func TestController_Process_OwnBuildFailedFailsBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateSpeculating)
	path := entity.SpeculationPath{Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 5,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusBuilding, BuildID: "build-1"}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{PathID: "test-queue/batch/1/path/0", BuildID: "build-1"}, nil)
	h.buildStore.EXPECT().Get(gomock.Any(), "build-1").Return(entity.Build{
		ID: "build-1", BatchID: batch.ID, SpeculationPathID: "test-queue/batch/1/path/0", Status: entity.BuildStatusFailed,
	}, nil)
	h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(5), int32(6), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
			require.Len(t, paths, 1)
			assert.Equal(t, entity.SpeculationPathStatusFailed, paths[0].Status)
			return nil
		})
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateFailed).Return(nil)
	// Failing the batch must fan out to its dependents so their dead chain
	// paths get reconciled and a surviving path can take over.
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{"test-queue/batch/9"},
		Version:    1,
	}, nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.ElementsMatch(t, []pubRec{
		{topic: "prioritize", msgID: batch.Queue},
		{topic: "speculate", msgID: "test-queue/batch/9"},
		{topic: "conclude", msgID: batch.ID},
	}, *h.records)
}

// A dependent-wake publish failure after finalize's terminal CAS nacks the
// message; redelivery converges through the terminal branch, which re-runs
// the fan-out and the conclude publish. Only the speculate topic fails here —
// the prioritize publish earlier in the pass succeeds and is recorded.
func TestController_Process_FinalizeDependentPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTopicFailingPublishHarness(t, ctrl, fmt.Errorf("publish failed"), "speculate")
	batch := testBatch(entity.BatchStateSpeculating)
	path := entity.SpeculationPath{Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 5,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusBuilding, BuildID: "build-1"}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{PathID: "test-queue/batch/1/path/0", BuildID: "build-1"}, nil)
	h.buildStore.EXPECT().Get(gomock.Any(), "build-1").Return(entity.Build{
		ID: "build-1", BatchID: batch.ID, SpeculationPathID: "test-queue/batch/1/path/0", Status: entity.BuildStatusFailed,
	}, nil)
	h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(5), int32(6), gomock.Any()).Return(nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateFailed).Return(nil)
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{"test-queue/batch/9"},
		Version:    1,
	}, nil)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// Hand-built two-path tree: a dependency of the head landing (Succeeded)
// while NOT in a path's base deads that path, even though the sibling path
// (which included the dependency in its base) survives.
func TestController_Process_NonBaseDependencyLandingDeadsSiblingPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)

	pathAlone := entity.SpeculationPath{Head: batch.ID}                            // does not assume dep lands
	pathWithBase := entity.SpeculationPath{Base: []string{dep.ID}, Head: batch.ID} // assumes dep lands
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths: []entity.SpeculationPathInfo{
			{ID: "test-queue/batch/1/path/0", Path: pathAlone, Status: entity.SpeculationPathStatusSelected},
			{ID: "test-queue/batch/1/path/1", Path: pathWithBase, Status: entity.SpeculationPathStatusSelected},
		},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/1").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
	h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
			require.Len(t, paths, 2)
			byHeadOnly := func(p entity.SpeculationPathInfo) bool { return len(p.Path.Base) == 0 }
			for _, p := range paths {
				if byHeadOnly(p) {
					assert.Equal(t, entity.SpeculationPathStatusCancelled, p.Status, "path that didn't base on the landed dep must be dead")
				} else {
					assert.Equal(t, entity.SpeculationPathStatusSelected, p.Status, "path that based on the landed dep must survive")
				}
			}
			return nil
		})

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
}

// Cancelling drives the terminal-cancellation flow as a convergent sweep
// with no runner interaction anywhere: each pass resolves every non-terminal
// path through its path->build mapping and either settles it to Cancelled
// (no build, dangling mapping, or terminal build) or captures intent
// (Cancelling) while its build is still in flight. While any path is left
// Cancelling the batch stays in BatchStateCancelling and the pass only
// publishes the queue to prioritize — the channel that routes the persisted
// intents to the build stage — then acks and waits for the next buildsignal
// wake. Only a pass that finds nothing pending CASes the batch to terminal
// Cancelled, fans out dependents (AFTER the CAS, so the woken dependents
// observe the dep as Cancelled and drop it from their chain rather than
// waiting on still-Cancelling state nobody will nudge), and publishes
// conclude.
func TestController_Process_CancellingFlow(t *testing.T) {
	tests := []struct {
		name string
		// wantPending is true when the pass must leave the batch in
		// Cancelling and only publish prioritize; false when the pass
		// completes the terminal sequence.
		wantPending bool
		setup       func(t *testing.T, h *testHarness, batch entity.Batch)
	}{
		{
			name:        "live_builds_marked_cancelling_and_waits",
			wantPending: true,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				pathA := entity.SpeculationPath{Head: batch.ID}
				pathB := entity.SpeculationPath{Base: []string{"test-queue/batch/dep"}, Head: batch.ID}
				pathAID := "test-queue/batch/1/path/0"
				pathBID := "test-queue/batch/1/path/1"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 3,
					Paths: []entity.SpeculationPathInfo{
						{ID: pathAID, Path: pathA, Status: entity.SpeculationPathStatusBuilding, BuildID: "runner-1"},
						{ID: pathBID, Path: pathB, Status: entity.SpeculationPathStatusBuilding, BuildID: "runner-2"},
					},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathAID).Return(entity.SpeculationPathBuild{PathID: pathAID, BuildID: "runner-1"}, nil)
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{
					ID: "runner-1", BatchID: batch.ID, SpeculationPathID: pathAID, Status: entity.BuildStatusRunning,
				}, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathBID).Return(entity.SpeculationPathBuild{PathID: pathBID, BuildID: "runner-2"}, nil)
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-2").Return(entity.Build{
					ID: "runner-2", BatchID: batch.ID, SpeculationPathID: pathBID, Status: entity.BuildStatusAccepted,
				}, nil)
				// Intent only: both live paths flip to Cancelling; no runner
				// call, no build row write.
				h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(3), int32(4), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
						require.Len(t, paths, 2)
						for _, p := range paths {
							assert.Equal(t, entity.SpeculationPathStatusCancelling, p.Status)
						}
						return nil
					})
			},
		},
		{
			name:        "already_cancelling_live_build_waits_without_write",
			wantPending: true,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 4,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling, BuildID: "runner-1"}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-1"}, nil)
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{
					ID: "runner-1", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusRunning,
				}, nil)
				// Nothing changed: no tree Update expected on a no-op sweep.
			},
		},
		{
			name:        "builds_now_terminal_settle_and_complete",
			wantPending: false,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 5,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelling, BuildID: "runner-1"}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-1"}, nil)
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{
					ID: "runner-1", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusCancelled,
				}, nil)
				h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(5), int32(6), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
						require.Len(t, paths, 1)
						assert.Equal(t, entity.SpeculationPathStatusCancelled, paths[0].Status)
						return nil
					})
			},
		},
		{
			name:        "already_terminal_build_settles_path",
			wantPending: false,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 1,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusBuilding, BuildID: "runner-1"}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-1"}, nil)
				// The build finished before the cancel could matter: nothing
				// is in flight, so the path settles straight to Cancelled.
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{
					ID: "runner-1", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusSucceeded,
				}, nil)
				h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
						require.Len(t, paths, 1)
						assert.Equal(t, entity.SpeculationPathStatusCancelled, paths[0].Status)
						return nil
					})
			},
		},
		{
			name:        "terminal_path_skipped_without_io",
			wantPending: false,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 1,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusPassed, BuildID: "runner-1"}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				// Passed is a settled outcome: no path->build read, no build
				// read, no tree Update.
			},
		},
		{
			// Terminal-transition guard: a path already at Cancelled can hide
			// a live build (pre-build Cancel racing the build stage's
			// trigger). A pass about to settle must pull it back to
			// Cancelling so the build stage enacts the stop, instead of
			// CASing the batch terminal over a running CI.
			name:        "cancelled_path_with_live_build_reopened",
			wantPending: true,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 7,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusCancelled}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-1"}, nil)
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{
					ID: "runner-1", BatchID: batch.ID, SpeculationPathID: pathID, Status: entity.BuildStatusRunning,
				}, nil)
				h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(7), int32(8), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
						require.Len(t, paths, 1)
						assert.Equal(t, entity.SpeculationPathStatusCancelling, paths[0].Status)
						return nil
					})
			},
		},
		{
			name:        "missing_tree_tolerated",
			wantPending: false,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound)
				// No PathBuildStore/BuildStore calls: nothing was ever
				// speculated, so nothing can be pending.
			},
		},
		{
			name:        "path_with_no_mapping_drops_to_cancelled",
			wantPending: false,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 1,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusSelected}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)
				h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
						require.Len(t, paths, 1)
						assert.Equal(t, entity.SpeculationPathStatusCancelled, paths[0].Status)
						return nil
					})
			},
		},
		{
			name:        "mapping_dangling_invariant_breach_tolerated",
			wantPending: false,
			setup: func(t *testing.T, h *testHarness, batch entity.Batch) {
				path := entity.SpeculationPath{Head: batch.ID}
				pathID := "test-queue/batch/1/path/0"
				tree := entity.SpeculationTree{
					BatchID: batch.ID,
					Version: 1,
					Paths:   []entity.SpeculationPathInfo{{ID: pathID, Path: path, Status: entity.SpeculationPathStatusBuilding, BuildID: "runner-1"}},
				}
				h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
				// Mapping exists but its target build row is missing: the
				// invariant-breach case. It must not crash or error — the
				// path is treated as unbuilt and settles to Cancelled.
				h.pathBuildStore.EXPECT().Get(gomock.Any(), pathID).Return(entity.SpeculationPathBuild{PathID: pathID, BuildID: "runner-1"}, nil)
				h.buildStore.EXPECT().Get(gomock.Any(), "runner-1").Return(entity.Build{}, storage.ErrNotFound)
				h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).
					DoAndReturn(func(_ context.Context, _ string, _, _ int32, paths []entity.SpeculationPathInfo) error {
						require.Len(t, paths, 1)
						assert.Equal(t, entity.SpeculationPathStatusCancelled, paths[0].Status)
						return nil
					})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			h := newTestHarness(t, ctrl)
			batch := testBatch(entity.BatchStateCancelling)

			h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			if !tt.wantPending {
				h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)
				h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
					BatchID:    batch.ID,
					Dependents: []string{"test-queue/batch/2", "test-queue/batch/3"},
					Version:    1,
				}, nil)
			}

			tt.setup(t, h, batch)

			require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))

			if tt.wantPending {
				assert.Equal(t, []pubRec{
					{topic: "prioritize", msgID: "test-queue"},
				}, *h.records)
				return
			}
			assert.Equal(t, []pubRec{
				{topic: "speculate", msgID: "test-queue/batch/2"},
				{topic: "speculate", msgID: "test-queue/batch/3"},
				{topic: "conclude", msgID: batch.ID},
			}, *h.records)
		})
	}
}

// storage.ErrVersionMismatch on the terminal cancel CAS must surface as an
// error with the underlying sentinel in the chain, and must not run the
// dependent fan-out or conclude publish.
func TestController_Process_CancellingTerminalCASVersionMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCancelling)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).
		Return(storage.ErrVersionMismatch)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{}, storage.ErrNotFound)
	// No BuildStore calls: the sweep returns immediately when the tree
	// itself is missing. BatchDependentStore must NOT be touched — terminal
	// CAS failed before fan-out.

	err := runProcess(t, ctrl, h.controller, batch.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
	assert.Empty(t, *h.records)
}

// Publish failure must not advance the batch state further: the CAS to
// Speculating (which precedes the prioritize publish) still lands, but
// finalize must never run since the publish error aborts speculateBatch.
func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newFailingPublishHarness(t, ctrl, fmt.Errorf("publish failed"))
	batch := testBatch(entity.BatchStateScored)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
}

// A publish failure on an already-Speculating batch (no CAS in play) must
// still abort before finalize runs.
func TestController_Process_PublishFailureAlreadySpeculating(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newFailingPublishHarness(t, ctrl, fmt.Errorf("publish failed"))
	batch := testBatch(entity.BatchStateSpeculating)
	path := entity.SpeculationPath{Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 1,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	h.pathBuildStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1/path/0").Return(entity.SpeculationPathBuild{}, storage.ErrNotFound)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
}

// reconcileStatus is a pure function: table-test every transition, with
// special attention to "never downgrade a terminal status".
func TestReconcileStatus(t *testing.T) {
	tests := []struct {
		name    string
		current entity.SpeculationPathStatus
		build   entity.BuildStatus
		want    entity.SpeculationPathStatus
	}{
		{"accepted_maps_to_building", entity.SpeculationPathStatusPrioritized, entity.BuildStatusAccepted, entity.SpeculationPathStatusBuilding},
		{"running_maps_to_building", entity.SpeculationPathStatusBuilding, entity.BuildStatusRunning, entity.SpeculationPathStatusBuilding},
		{"succeeded_maps_to_passed", entity.SpeculationPathStatusBuilding, entity.BuildStatusSucceeded, entity.SpeculationPathStatusPassed},
		{"failed_maps_to_failed", entity.SpeculationPathStatusBuilding, entity.BuildStatusFailed, entity.SpeculationPathStatusFailed},
		{"cancelled_maps_to_cancelled", entity.SpeculationPathStatusBuilding, entity.BuildStatusCancelled, entity.SpeculationPathStatusCancelled},
		{"cancelling_stays_until_build_terminal", entity.SpeculationPathStatusCancelling, entity.BuildStatusRunning, entity.SpeculationPathStatusCancelling},
		{"cancelling_resolves_on_terminal_build", entity.SpeculationPathStatusCancelling, entity.BuildStatusCancelled, entity.SpeculationPathStatusCancelled},
		{"passed_never_downgraded", entity.SpeculationPathStatusPassed, entity.BuildStatusFailed, entity.SpeculationPathStatusPassed},
		{"failed_never_downgraded", entity.SpeculationPathStatusFailed, entity.BuildStatusSucceeded, entity.SpeculationPathStatusFailed},
		{"cancelled_never_downgraded", entity.SpeculationPathStatusCancelled, entity.BuildStatusSucceeded, entity.SpeculationPathStatusCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, reconcileStatus(tt.current, tt.build))
		})
	}
}

// pathDead is a pure function: table-test the base-failed and
// landed-non-base-dependency cases, plus the deliberate Cancelled-base
// leniency.
func TestPathDead(t *testing.T) {
	head := "q/batch/2"
	tests := []struct {
		name string
		path entity.SpeculationPath
		deps map[string]entity.Batch
		want bool
	}{
		{
			name: "base_dep_failed_is_dead",
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: head},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", State: entity.BatchStateFailed}},
			want: true,
		},
		{
			name: "base_dep_cancelled_is_tolerated",
			path: entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: head},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", State: entity.BatchStateCancelled}},
			want: false,
		},
		{
			name: "non_base_dep_landed_is_dead",
			path: entity.SpeculationPath{Head: head},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", State: entity.BatchStateSucceeded}},
			want: true,
		},
		{
			name: "non_base_dep_pending_is_not_dead",
			path: entity.SpeculationPath{Head: head},
			deps: map[string]entity.Batch{"q/batch/1": {ID: "q/batch/1", State: entity.BatchStateSpeculating}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, pathDead(tt.path, tt.deps))
		})
	}
}

// mergeableNow and viable are pure functions over a path and its
// dependencies; table-test the combinations not already covered end-to-end.
func TestMergeableNowAndViable(t *testing.T) {
	base := "q/batch/1"
	head := "q/batch/2"

	tests := []struct {
		name           string
		status         entity.SpeculationPathStatus
		deps           map[string]entity.Batch
		wantMergeable  bool
		wantViable     bool
		pathBaseIsBase bool
	}{
		{
			name:           "passed_base_succeeded_no_extra_deps",
			status:         entity.SpeculationPathStatusPassed,
			deps:           map[string]entity.Batch{base: {ID: base, State: entity.BatchStateSucceeded}},
			wantMergeable:  true,
			wantViable:     true,
			pathBaseIsBase: true,
		},
		{
			name:           "passed_base_pending",
			status:         entity.SpeculationPathStatusPassed,
			deps:           map[string]entity.Batch{base: {ID: base, State: entity.BatchStateSpeculating}},
			wantMergeable:  false,
			wantViable:     true,
			pathBaseIsBase: true,
		},
		{
			// A base dependency published for merge is NOT landed yet: the
			// queue reorders across nack backoff, so merging now could
			// overtake the base on the way to runway.
			name:           "passed_base_merging_waits",
			status:         entity.SpeculationPathStatusPassed,
			deps:           map[string]entity.Batch{base: {ID: base, State: entity.BatchStateMerging}},
			wantMergeable:  false,
			wantViable:     true,
			pathBaseIsBase: true,
		},
		{
			// A non-base dependency in Merging might yet land and dead the
			// path, so merging waits for its confirmed outcome — and the
			// path is not (yet) dead either.
			name:           "passed_non_base_merging_waits",
			status:         entity.SpeculationPathStatusPassed,
			deps:           map[string]entity.Batch{base: {ID: base, State: entity.BatchStateMerging}},
			wantMergeable:  false,
			wantViable:     true,
			pathBaseIsBase: false,
		},
		{
			name:           "failed_path_not_viable",
			status:         entity.SpeculationPathStatusFailed,
			deps:           map[string]entity.Batch{},
			wantMergeable:  false,
			wantViable:     false,
			pathBaseIsBase: false,
		},
		{
			name:           "cancelling_path_not_viable",
			status:         entity.SpeculationPathStatusCancelling,
			deps:           map[string]entity.Batch{},
			wantMergeable:  false,
			wantViable:     false,
			pathBaseIsBase: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var path entity.SpeculationPath
			if tt.pathBaseIsBase {
				path = entity.SpeculationPath{Base: []string{base}, Head: head}
			} else {
				path = entity.SpeculationPath{Head: head}
			}
			info := entity.SpeculationPathInfo{Path: path, Status: tt.status}
			assert.Equal(t, tt.wantMergeable, mergeableNow(info, tt.deps))
			assert.Equal(t, tt.wantViable, viable(info, tt.deps))
		})
	}
}
