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

// testHarness wires a Controller against mocked storage plus programmable
// fake seam implementations (enumerator, scorer, selector, dependency limit)
// that tests script per case. Every publish is recorded so tests can assert
// both content and order. There is no build-runner seam yet: nothing in this
// package's speculate.go calls one — cancel decisions on the speculation tree
// are captured as intent only.
type testHarness struct {
	controller *Controller
	batchStore *storagemock.MockBatchStore
	treeStore  *storagemock.MockSpeculationTreeStore
	buildStore *storagemock.MockBuildStore
	depStore   *storagemock.MockBatchDependentStore
	enum       *enumeratorfake.Enumerator
	scorer     *scorerfake.Scorer
	selector   *selectorfake.Selector
	depLimit   *dependencylimitfake.DependencyLimit
	records    *[]pubRec
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
	depStore := storagemock.NewMockBatchDependentStore(ctrl)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetSpeculationTreeStore().Return(treeStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
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
		controller: c,
		batchStore: batchStore,
		treeStore:  treeStore,
		buildStore: buildStore,
		depStore:   depStore,
		enum:       enum,
		scorer:     scr,
		selector:   sel,
		depLimit:   dl,
		records:    &records,
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
// re-publish conclude — this is both the normal dependent wake-up (mergesignal
// routes every terminal transition of a batch back through speculate under
// the batch's own ID) and the crash self-heal for a lost publish. State must
// not change (no UpdateState), and neither BuildStore nor
// SpeculationTreeStore is touched.
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
// so the message nacks — unlike a stale dependent entry, which is tolerated.
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
// Version 1, Create. Score echoes. Select promotes the lone path to Selected
// -> tree changed -> Update(1,2,...). The forward step then CASes Created ->
// Speculating and publishes the queue to prioritize; tryFinalize does not run
// on this first pass (it only runs once the batch has reached Speculating).
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
	// No changes this pass (selector decides nothing) -> no Update call.
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// Dependency gate: too many active dependencies for a batch with no tree yet
// blocks enumeration entirely (ack without creating a tree, no forward
// step either). A later dependency-terminal event re-triggers speculate.
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

// A pass over an unchanged tree must not call Update. The batch still
// publishes the queue to prioritize every pass regardless of tree changes;
// tryFinalize waits on the still-pending dependency, so no merge/conclude
// publish happens.
func TestController_Process_NoChangeSkipsTreeUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)
	path := entity.SpeculationPath{Head: batch.ID}
	tree := entity.SpeculationTree{
		BatchID: batch.ID,
		Version: 4,
		Paths:   []entity.SpeculationPathInfo{{ID: "test-queue/batch/1/path/0", Path: path, Status: entity.SpeculationPathStatusSelected}},
	}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil).AnyTimes()
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(tree, nil)
	// Fake scorer echoes, fake selector decides nothing -> no change.
	// No treeStore.Update expected. tryFinalize waits on the pending dep, so
	// no UpdateState/merge publish either.

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// A version mismatch on the speculation tree Update must surface as an error
// (nack; the round is recomputed on redelivery) and must not reach the
// forward step.
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
	h.selector.SetDecisions(entity.PathDecision{PathID: tree.Paths[0].ID, Action: entity.SpeculationPathActionPromote})
	h.treeStore.EXPECT().Update(gomock.Any(), batch.ID, int32(1), int32(2), gomock.Any()).Return(storage.ErrVersionMismatch)
	// tryFinalize must never run: no batchStore.UpdateState, no merge publish.

	err := runProcess(t, ctrl, h.controller, batch.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
	assert.Empty(t, *h.records)
}

// tryFinalize: Speculating with no deps should publish to merge and CAS to
// Merging. The batch already has an (empty) speculation tree from its
// Created/Scored pass. Every pass also publishes the queue to prioritize,
// before tryFinalize's own merge publish.
func TestController_Process_FinalizeNoDeps(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateSpeculating)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{
		{topic: "prioritize", msgID: batch.Queue},
		{topic: "submitqueue-merge", msgID: batch.ID},
	}, *h.records)
}

// tryFinalize: Speculating with all deps Succeeded should publish to merge
// and CAS to Merging.
func TestController_Process_FinalizeAllDepsSucceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	depA := entity.Batch{ID: "test-queue/batch/0a", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 5}
	depB := entity.Batch{ID: "test-queue/batch/0b", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 3}
	batch := testBatch(entity.BatchStateSpeculating, depA.ID, depB.ID)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// speculateBatch fetches dependencies once for the pre-tree gate check,
	// and tryFinalize fetches them again itself.
	h.batchStore.EXPECT().Get(gomock.Any(), depA.ID).Return(depA, nil).Times(2)
	h.batchStore.EXPECT().Get(gomock.Any(), depB.ID).Return(depB, nil).Times(2)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{
		{topic: "prioritize", msgID: batch.Queue},
		{topic: "submitqueue-merge", msgID: batch.ID},
	}, *h.records)
}

// tryFinalize: Speculating with a dep still in flight is a no-op (no merge
// publish, no state change). The queue is still published to prioritize
// every pass.
func TestController_Process_WaitingOnDep(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// speculateBatch fetches dependencies once for the pre-tree gate check,
	// and tryFinalize fetches them again itself.
	h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil).Times(2)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	// No UpdateState expected — gomock will fail if it is called.

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// tryFinalize: a failed dep must fail the batch (Speculating → Failed), wake
// its dependents (so the failure cascades), and publish to conclude.
// Otherwise the batch livelocks.
func TestController_Process_FailedDepFailsBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateFailed, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)
	batch.Contains = []string{"test-queue/req/1", "test-queue/req/2"}

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// speculateBatch fetches dependencies once for the pre-tree gate check,
	// and tryFinalize fetches them again itself.
	h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil).Times(2)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateFailed).Return(nil)
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{"test-queue/batch/2"},
		Version:    1,
	}, nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{
		{topic: "prioritize", msgID: batch.Queue},
		{topic: "speculate", msgID: "test-queue/batch/2"},
		{topic: "conclude", msgID: batch.ID},
	}, *h.records)
}

// A dependent-wake publish failure after the terminal CAS nacks the message;
// redelivery converges through the terminal branch, which re-runs the
// fan-out and the conclude publish. Only the speculate topic fails here —
// the prioritize publish earlier in the pass succeeds and is recorded.
func TestController_Process_FailedDepDependentPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTopicFailingPublishHarness(t, ctrl, fmt.Errorf("publish failed"), "speculate")
	dep := entity.Batch{ID: "test-queue/batch/0", Queue: "test-queue", State: entity.BatchStateFailed, Version: 1}
	batch := testBatch(entity.BatchStateSpeculating, dep.ID)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// speculateBatch fetches dependencies once for the pre-tree gate check,
	// and tryFinalize fetches them again itself.
	h.batchStore.EXPECT().Get(gomock.Any(), dep.ID).Return(dep, nil).Times(2)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateFailed).Return(nil)
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{"test-queue/batch/2"},
		Version:    1,
	}, nil)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "prioritize", msgID: batch.Queue}}, *h.records)
}

// tryFinalize: a cancelled dep is treated as out-of-the-way — it will never
// land and can no longer conflict. The dep is dropped from the chain and the
// batch advances to Merging as if the cancelled dep had succeeded.
func TestController_Process_CancelledDepSkipped(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	depCancelled := entity.Batch{ID: "test-queue/batch/0a", Queue: "test-queue", State: entity.BatchStateCancelled, Version: 2}
	depSucceeded := entity.Batch{ID: "test-queue/batch/0b", Queue: "test-queue", State: entity.BatchStateSucceeded, Version: 5}
	batch := testBatch(entity.BatchStateSpeculating, depCancelled.ID, depSucceeded.ID)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// speculateBatch fetches dependencies once for the pre-tree gate check,
	// and tryFinalize fetches them again itself.
	h.batchStore.EXPECT().Get(gomock.Any(), depCancelled.ID).Return(depCancelled, nil).Times(2)
	h.batchStore.EXPECT().Get(gomock.Any(), depSucceeded.ID).Return(depSucceeded, nil).Times(2)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateMerging).Return(nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{
		{topic: "prioritize", msgID: batch.Queue},
		{topic: "submitqueue-merge", msgID: batch.ID},
	}, *h.records)
}

// Cancelling drives the terminal-cancellation flow: cancel any in-flight
// build, CAS the batch to Cancelled, fan out dependents, publish to
// conclude. Validates the full order with a running build and
// a couple of dependents. Order matters: dependents must publish AFTER the
// terminal CAS so the woken dependents observe the dep as Cancelled (and
// drop it from their chain) rather than as still-Cancelling (which would
// leave them waiting on a state nobody is going to nudge).
func TestController_Process_CancellingTerminalFlow(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCancelling)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	h.buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{
		ID: batch.ID, BatchID: batch.ID, Status: entity.BuildStatusRunning,
	}, nil)
	h.buildStore.EXPECT().UpdateStatus(gomock.Any(), batch.ID, entity.BuildStatusCancelled).Return(nil)

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
}

// If the build for the batch has already reached a terminal status (e.g. CI
// finished naturally between the cancel intent and the speculate pickup), the
// cancellation must not re-flip it — UpdateStatus must never fire. The rest
// of the flow (terminal batch CAS, dependent fan-out, conclude) still runs.
func TestController_Process_CancellingBuildAlreadyTerminal(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCancelling)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	h.buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{
		ID: batch.ID, BatchID: batch.ID, Status: entity.BuildStatusSucceeded,
	}, nil)
	// No UpdateStatus expected — the build is already terminal.

	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID: batch.ID, Version: 1,
	}, nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
}

// If no Build entity exists for the batch (e.g. cancel arrived before
// speculation started building), the BuildStore.Get NotFound must be
// tolerated and the rest of the cancellation flow must continue.
func TestController_Process_CancellingNoBuildYet(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCancelling)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	h.buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{}, storage.ErrNotFound)
	// No UpdateStatus expected.

	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{
		BatchID: batch.ID, Version: 1,
	}, nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
}

// A batch whose BatchDependent row exists with an empty Dependents list must
// still drive itself to terminal and publish to conclude. This is the normal
// "no dependents" path: the batch controller creates the row with an empty
// list at batch creation time and it stays empty if no later batch conflicts.
func TestController_Process_CancellingNoDependents(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCancelling)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).Return(nil)

	h.buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{}, storage.ErrNotFound)
	h.depStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.BatchDependent{BatchID: batch.ID, Dependents: []string{}, Version: 1}, nil)

	require.NoError(t, runProcess(t, ctrl, h.controller, batch.ID))
	assert.Equal(t, []pubRec{{topic: "conclude", msgID: batch.ID}}, *h.records)
}

// storage.ErrVersionMismatch on the terminal CAS must surface as an error
// with the underlying sentinel in the chain so the base controller can
// classify it as retryable. The dependent fan-out and conclude publish must
// NOT run if the terminal CAS failed — on redelivery the self-heal branch
// will pick up the (now-terminal) state and complete the fan-out.
func TestController_Process_CancellingTerminalCASVersionMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newTestHarness(t, ctrl)
	batch := testBatch(entity.BatchStateCancelling)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateCancelled).
		Return(storage.ErrVersionMismatch)

	h.buildStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.Build{}, storage.ErrNotFound)
	// BatchDependentStore must NOT be touched — terminal CAS failed before fan-out.

	err := runProcess(t, ctrl, h.controller, batch.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
	assert.Empty(t, *h.records)
}

// Publish failure must not advance the batch state further: the CAS to
// Speculating (which precedes the prioritize publish) still lands, but
// tryFinalize must never run since the publish error aborts speculateBatch.
func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := newFailingPublishHarness(t, ctrl, fmt.Errorf("publish failed"))
	batch := testBatch(entity.BatchStateScored)

	h.batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	h.treeStore.EXPECT().Get(gomock.Any(), batch.ID).Return(entity.SpeculationTree{BatchID: batch.ID, Version: 1, Paths: []entity.SpeculationPathInfo{}}, nil)
	h.batchStore.EXPECT().UpdateState(gomock.Any(), batch.ID, int32(1), int32(2), entity.BatchStateSpeculating).Return(nil)

	require.Error(t, runProcess(t, ctrl, h.controller, batch.ID))
}
