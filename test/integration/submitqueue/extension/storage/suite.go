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

package storage

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// StorageContractSuite defines the contract tests for the storage extension:
// the queue-scoped aggregate resolved through storage.Factory plus the global
// read-model stores. All storage implementations must pass these tests.
// Implementation-specific tests should embed this suite and call SetFactory()
// and SetGlobalStores().
type StorageContractSuite struct {
	suite.Suite
	ctx       context.Context
	factory   storage.Factory
	summaries storage.RequestSummaryStore
	uris      storage.RequestURIStore
	log       *testutil.TestLogger
}

// SetContext sets the context for tests
func (s *StorageContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetFactory is called by implementation tests to provide the queue-scoped
// storage factory under test.
func (s *StorageContractSuite) SetFactory(factory storage.Factory) {
	s.factory = factory
}

// SetGlobalStores is called by implementation tests to provide the global
// read-model stores under test.
func (s *StorageContractSuite) SetGlobalStores(summaries storage.RequestSummaryStore, uris storage.RequestURIStore) {
	s.summaries = summaries
	s.uris = uris
}

// forQueue resolves the queue-scoped store aggregate for a queue, failing the
// test on resolution errors.
func (s *StorageContractSuite) forQueue(queue string) storage.Storage {
	store, err := s.factory.For(storage.Config{QueueName: queue})
	s.Require().NoError(err)
	return store
}

// SetLogger sets the logger for tests
func (s *StorageContractSuite) SetLogger(log *testutil.TestLogger) {
	s.log = log
}

// TestStorage_CreateAndGet tests creating and retrieving a request
func (s *StorageContractSuite) TestStorage_CreateAndGet() {
	t := s.T()
	ctx := s.ctx

	request := entity.Request{
		ID:    "test/create-get",
		Queue: "test-queue",
		State: entity.RequestStateStarted,
		Change: change.Change{
			URIs: []string{"github://github.example.com/uber/storage-test/pull/123/abcdef0123456789abcdef0123456789abcdef01"},
		},
		LandStrategy: mergestrategy.MergeStrategyMerge,
		Version:      1,
	}

	// Create request
	err := s.forQueue("test-queue").GetRequestStore().Create(ctx, request)
	require.NoError(t, err, "failed to create request")

	// Get request back
	retrieved, err := s.forQueue("test-queue").GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err, "failed to get request")

	// Verify fields
	assert.Equal(t, request.ID, retrieved.ID)
	assert.Equal(t, request.Queue, retrieved.Queue)
	assert.Equal(t, request.State, retrieved.State)
	assert.Equal(t, request.Change.URIs, retrieved.Change.URIs)
	assert.Equal(t, request.LandStrategy, retrieved.LandStrategy)
	assert.Equal(t, request.Version, retrieved.Version)
}

// TestStorage_CreateAndGet_StackedPRs tests creating and retrieving a request with stacked PRs
func (s *StorageContractSuite) TestStorage_CreateAndGet_StackedPRs() {
	t := s.T()
	ctx := s.ctx

	// Stacked PRs as separate URIs
	stackedURIs := []string{
		"github://github.example.com/uber/monorepo/pull/101/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"github://github.example.com/uber/monorepo/pull/102/cccccccccccccccccccccccccccccccccccccccc",
		"github://github.example.com/uber/monorepo/pull/103/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"github://github.example.com/uber/monorepo/pull/104/0000000000000000000000000000000000000004",
	}

	request := entity.Request{
		ID:    "test/stacked-prs",
		Queue: "test-queue",
		State: entity.RequestStateStarted,
		Change: change.Change{
			URIs: stackedURIs,
		},
		LandStrategy: mergestrategy.MergeStrategySquashRebase,
		Version:      1,
	}

	// Create request
	err := s.forQueue("test-queue").GetRequestStore().Create(ctx, request)
	require.NoError(t, err, "failed to create request with stacked PRs")

	// Get request back
	retrieved, err := s.forQueue("test-queue").GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err, "failed to get request with stacked PRs")

	// Verify the stacked URIs are preserved
	assert.Equal(t, stackedURIs, retrieved.Change.URIs, "stacked PR URIs should be preserved exactly")
	assert.Equal(t, request.ID, retrieved.ID)
	assert.Equal(t, request.LandStrategy, retrieved.LandStrategy)
}

// TestStorage_Update tests replacing all non-key request fields.
func (s *StorageContractSuite) TestStorage_Update() {
	t := s.T()
	ctx := s.ctx

	request := entity.Request{
		ID:           "test/update",
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/monorepo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		State:        entity.RequestStateStarted,
		LandStrategy: mergestrategy.MergeStrategyMerge,
		Version:      1,
	}

	// Create initial request
	err := s.forQueue("test-queue").GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	updated := request
	updated.Change.URIs = []string{"github://github.example.com/uber/monorepo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	updated.LandStrategy = mergestrategy.MergeStrategySquashRebase
	updated.State = entity.RequestStateProcessing
	err = s.forQueue("test-queue").GetRequestStore().Update(ctx, updated, request.Version, request.Version+1)
	require.NoError(t, err, "failed to update request")

	// Verify update
	retrieved, err := s.forQueue("test-queue").GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err)
	updated.Version = request.Version + 1
	assert.Equal(t, updated, retrieved)
}

// TestStorage_OptimisticLocking tests version-based optimistic locking
func (s *StorageContractSuite) TestStorage_OptimisticLocking() {
	t := s.T()
	ctx := s.ctx

	request := entity.Request{
		ID:           "test/optimistic-lock",
		Queue:        "test-queue",
		State:        entity.RequestStateStarted,
		LandStrategy: mergestrategy.MergeStrategyMerge,
		Version:      1,
	}

	// Create request
	err := s.forQueue("test-queue").GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Update with correct version.
	updated := request
	updated.Change.URIs = []string{"github://github.example.com/uber/monorepo/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	updated.LandStrategy = mergestrategy.MergeStrategySquashRebase
	updated.State = entity.RequestStateProcessing
	err = s.forQueue("test-queue").GetRequestStore().Update(ctx, updated, 1, 2)
	require.NoError(t, err, "update with correct version should succeed")

	// Try to replace every field with a stale version.
	stale := request
	stale.Change.URIs = []string{"github://github.example.com/uber/monorepo/pull/3/cccccccccccccccccccccccccccccccccccccccc"}
	stale.LandStrategy = mergestrategy.MergeStrategyRebase
	stale.State = entity.RequestStateLanded
	err = s.forQueue("test-queue").GetRequestStore().Update(ctx, stale, 1, 3)
	assert.Error(t, err, "update with stale version should fail")
	assert.ErrorIs(t, err, storage.ErrVersionMismatch, "should return ErrVersionMismatch")

	// Verify no field was changed by the stale update.
	retrieved, err := s.forQueue("test-queue").GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err)
	updated.Version = 2
	assert.Equal(t, updated, retrieved)
}

// TestStorage_UpdateChangeURIs tests nil and empty URI replacement semantics.
func (s *StorageContractSuite) TestStorage_UpdateChangeURIs() {
	t := s.T()
	ctx := s.ctx

	tests := []struct {
		name string
		uris []string
	}{
		{name: "nil", uris: nil},
		{name: "empty", uris: []string{}},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := entity.Request{
				ID:           fmt.Sprintf("test/update-change-uris-%d", i),
				Queue:        "test-queue",
				Change:       change.Change{URIs: []string{"github://github.example.com/uber/monorepo/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
				State:        entity.RequestStateStarted,
				LandStrategy: mergestrategy.MergeStrategyMerge,
				Version:      1,
			}
			require.NoError(t, s.forQueue("test-queue").GetRequestStore().Create(ctx, request))

			updated := request
			updated.Change.URIs = tt.uris
			require.NoError(t, s.forQueue("test-queue").GetRequestStore().Update(ctx, updated, request.Version, request.Version+1))

			retrieved, err := s.forQueue("test-queue").GetRequestStore().Get(ctx, request.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.uris, retrieved.Change.URIs)
		})
	}
}

// TestStorage_BatchDependentUpdate verifies full updates, collection encoding, and optimistic locking.
func (s *StorageContractSuite) TestStorage_BatchDependentUpdate() {
	t := s.T()
	ctx := s.ctx

	batchDependent := entity.BatchDependent{
		BatchID:    "test/batch-dependent-update",
		Dependents: []string{"test/dependent/1"},
		Version:    1,
	}
	require.NoError(t, s.forQueue("test-queue").GetBatchDependentStore().Create(ctx, batchDependent))

	nilDependents := batchDependent
	nilDependents.Dependents = nil
	require.NoError(t, s.forQueue("test-queue").GetBatchDependentStore().Update(ctx, nilDependents, 1, 2))

	retrieved, err := s.forQueue("test-queue").GetBatchDependentStore().Get(ctx, batchDependent.BatchID)
	require.NoError(t, err)
	assert.Nil(t, retrieved.Dependents)
	assert.Equal(t, int32(2), retrieved.Version)

	emptyDependents := retrieved
	emptyDependents.Dependents = []string{}
	require.NoError(t, s.forQueue("test-queue").GetBatchDependentStore().Update(ctx, emptyDependents, 2, 3))

	retrieved, err = s.forQueue("test-queue").GetBatchDependentStore().Get(ctx, batchDependent.BatchID)
	require.NoError(t, err)
	assert.Empty(t, retrieved.Dependents)
	assert.NotNil(t, retrieved.Dependents)
	assert.Equal(t, int32(3), retrieved.Version)

	staleUpdate := retrieved
	staleUpdate.Dependents = []string{"test/dependent/stale"}
	err = s.forQueue("test-queue").GetBatchDependentStore().Update(ctx, staleUpdate, 2, 4)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)

	retrieved, err = s.forQueue("test-queue").GetBatchDependentStore().Get(ctx, batchDependent.BatchID)
	require.NoError(t, err)
	assert.Empty(t, retrieved.Dependents)
	assert.NotNil(t, retrieved.Dependents)
	assert.Equal(t, int32(3), retrieved.Version)
}

func (s *StorageContractSuite) TestStorage_BatchUpdateReplacesAllNonKeyFields() {
	t := s.T()
	ctx := s.ctx
	store := s.forQueue("batch-update").GetBatchStore()
	batch := entity.Batch{
		ID:           "batch-update/batch/1",
		Queue:        "batch-update",
		Contains:     []string{"batch-update/1"},
		Dependencies: []string{"batch-update/batch/0"},
		State:        entity.BatchStateCreated,
		Version:      1,
	}
	require.NoError(t, store.Create(ctx, batch))

	nilCollections := batch
	nilCollections.Contains = nil
	nilCollections.Dependencies = nil
	nilCollections.State = entity.BatchStateSpeculating
	require.NoError(t, store.Update(ctx, nilCollections, 1, 2))

	got, err := store.Get(ctx, batch.ID)
	require.NoError(t, err)
	assert.Equal(t, "batch-update", got.Queue)
	assert.Nil(t, got.Contains)
	assert.Nil(t, got.Dependencies)
	assert.Equal(t, entity.BatchStateSpeculating, got.State)
	assert.Equal(t, int32(2), got.Version)

	emptyCollections := got
	emptyCollections.Contains = []string{}
	emptyCollections.Dependencies = []string{}
	emptyCollections.State = entity.BatchStateMerging
	require.NoError(t, store.Update(ctx, emptyCollections, 2, 3))

	got, err = store.Get(ctx, batch.ID)
	require.NoError(t, err)
	assert.Equal(t, "batch-update", got.Queue)
	assert.NotNil(t, got.Contains)
	assert.Empty(t, got.Contains)
	assert.NotNil(t, got.Dependencies)
	assert.Empty(t, got.Dependencies)
	assert.Equal(t, entity.BatchStateMerging, got.State)
	assert.Equal(t, int32(3), got.Version)

	stale := got
	stale.Contains = []string{"stale/request"}
	stale.Dependencies = []string{"stale/batch"}
	stale.State = entity.BatchStateFailed
	require.ErrorIs(t, store.Update(ctx, stale, 2, 4), storage.ErrVersionMismatch)

	unchanged, err := store.Get(ctx, batch.ID)
	require.NoError(t, err)
	assert.Equal(t, got, unchanged)
}

// TestStorage_QueueBatchStateRecordLifecycle exercises the QueueBatchStateStore
// contract: Put idempotency, List isolation between (queue, state) buckets, the
// Put-then-Delete record move, and Delete idempotency.
func (s *StorageContractSuite) TestStorage_QueueBatchStateRecordLifecycle() {
	t := s.T()
	ctx := s.ctx
	storeA := s.forQueue("qbs-queue-a").GetQueueBatchStateStore()
	storeB := s.forQueue("qbs-queue-b").GetQueueBatchStateStore()

	created1 := entity.QueueBatchState{Queue: "qbs-queue-a", State: entity.BatchStateCreated, BatchID: "qbs-queue-a/batch/1"}
	created2 := entity.QueueBatchState{Queue: "qbs-queue-a", State: entity.BatchStateCreated, BatchID: "qbs-queue-a/batch/2"}
	speculating := entity.QueueBatchState{Queue: "qbs-queue-a", State: entity.BatchStateSpeculating, BatchID: "qbs-queue-a/batch/3"}
	otherQueue := entity.QueueBatchState{Queue: "qbs-queue-b", State: entity.BatchStateCreated, BatchID: "qbs-queue-b/batch/1"}

	require.NoError(t, storeA.Put(ctx, created1))
	require.NoError(t, storeA.Put(ctx, created2))
	require.NoError(t, storeA.Put(ctx, speculating))
	require.NoError(t, storeB.Put(ctx, otherQueue))

	// Re-putting an existing record is a no-op success.
	require.NoError(t, storeA.Put(ctx, created1))

	// List returns exactly one (queue, state) bucket: no other states, no other queues, no duplicates.
	got, err := storeA.List(ctx, entity.BatchStateCreated)
	require.NoError(t, err)
	assert.ElementsMatch(t, []entity.QueueBatchState{created1, created2}, got)

	got, err = storeA.List(ctx, entity.BatchStateSpeculating)
	require.NoError(t, err)
	assert.ElementsMatch(t, []entity.QueueBatchState{speculating}, got)

	// An empty bucket lists empty, not an error.
	got, err = storeA.List(ctx, entity.BatchStateMerging)
	require.NoError(t, err)
	assert.Empty(t, got)

	// A record move: file under the new state, then remove the old bucket's record.
	moved := entity.QueueBatchState{Queue: created1.Queue, State: entity.BatchStateSpeculating, BatchID: created1.BatchID}
	require.NoError(t, storeA.Put(ctx, moved))
	require.NoError(t, storeA.Delete(ctx, created1.State, created1.BatchID))

	got, err = storeA.List(ctx, entity.BatchStateCreated)
	require.NoError(t, err)
	assert.ElementsMatch(t, []entity.QueueBatchState{created2}, got)

	got, err = storeA.List(ctx, entity.BatchStateSpeculating)
	require.NoError(t, err)
	assert.ElementsMatch(t, []entity.QueueBatchState{speculating, moved}, got)

	// Deleting an absent record is a no-op success.
	require.NoError(t, storeA.Delete(ctx, created1.State, created1.BatchID))

	// The other queue is untouched by all of the above.
	got, err = storeB.List(ctx, entity.BatchStateCreated)
	require.NoError(t, err)
	assert.ElementsMatch(t, []entity.QueueBatchState{otherQueue}, got)
}

// TestStorage_QueueIsolation verifies a store aggregate bound to one queue can
// neither read nor write another queue's records: cross-queue reads miss, and
// writes whose entity queue disagrees with the binding are rejected.
func (s *StorageContractSuite) TestStorage_QueueIsolation() {
	t := s.T()
	ctx := s.ctx
	storeA := s.forQueue("iso-queue-a")
	storeB := s.forQueue("iso-queue-b")

	request := entity.Request{ID: "iso-a/1", Queue: "iso-queue-a", State: entity.RequestStateStarted, LandStrategy: mergestrategy.MergeStrategyMerge, Version: 1}
	require.NoError(t, storeA.GetRequestStore().Create(ctx, request))
	_, err := storeB.GetRequestStore().Get(ctx, request.ID)
	require.ErrorIs(t, err, storage.ErrNotFound, "a request must be invisible through another queue's binding")
	require.Error(t, storeB.GetRequestStore().Create(ctx, request), "a mismatched-queue write must be rejected")

	batch := entity.Batch{ID: "iso-a/batch/1", Queue: "iso-queue-a", State: entity.BatchStateCreated, Version: 1}
	require.NoError(t, storeA.GetBatchStore().Create(ctx, batch))
	_, err = storeB.GetBatchStore().Get(ctx, batch.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// Builds are keyed by a runner-minted ID; the queue-leading key removes the
	// cross-queue uniqueness assumption, so the same runner ID coexists per queue.
	build := entity.Build{ID: "runner/iso/1", BatchID: batch.ID, Status: entity.BuildStatusRunning}
	require.NoError(t, storeA.GetBuildStore().Create(ctx, build))
	_, err = storeB.GetBuildStore().Get(ctx, build.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, storeB.GetBuildStore().Create(ctx, build), "the same runner-minted build ID must coexist across queues")

	dependent := entity.BatchDependent{BatchID: batch.ID, Dependents: []string{}, Version: 1}
	require.NoError(t, storeA.GetBatchDependentStore().Create(ctx, dependent))
	_, err = storeB.GetBatchDependentStore().Get(ctx, batch.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)

	association := entity.RequestBatch{RequestID: request.ID, BatchID: batch.ID, Version: 1}
	require.NoError(t, storeA.GetRequestBatchStore().Create(ctx, association))
	crossQueue, err := storeB.GetRequestBatchStore().GetByRequestID(ctx, request.ID)
	require.NoError(t, err)
	assert.Empty(t, crossQueue, "associations must be invisible through another queue's binding")

	// The owning queue still sees everything it wrote.
	fromA, err := storeA.GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err)
	assert.Equal(t, request, fromA)
}

// TestStorage_NotFound tests getting a non-existent request
func (s *StorageContractSuite) TestStorage_NotFound() {
	t := s.T()
	ctx := s.ctx

	// Try to get non-existent request
	_, err := s.forQueue("test-queue").GetRequestStore().Get(ctx, "test/nonexistent")
	assert.Error(t, err, "getting non-existent request should return error")
	assert.ErrorIs(t, err, storage.ErrNotFound, "should return ErrNotFound")
}

// TestStorage_CreateDuplicate tests creating a request with duplicate ID
func (s *StorageContractSuite) TestStorage_CreateDuplicate() {
	t := s.T()
	ctx := s.ctx

	request := entity.Request{
		ID:           "test/duplicate",
		Queue:        "test-queue",
		State:        entity.RequestStateStarted,
		LandStrategy: mergestrategy.MergeStrategyMerge,
		Version:      1,
	}

	// Create request
	err := s.forQueue("test-queue").GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Try to create duplicate
	err = s.forQueue("test-queue").GetRequestStore().Create(ctx, request)
	assert.Error(t, err, "creating duplicate request should return error")
	assert.ErrorIs(t, err, storage.ErrAlreadyExists, "should return ErrAlreadyExists")
}

// changeURI is a representative change URI reused across change-store contract tests.
const changeURI = "github://github.example.com/uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// Change-store contract tests scope each case to a distinct queue so they stay
// isolated without truncation (GetByURI is scoped by (queue, uri)).

// TestStorage_ChangeCreateAndGet_NoMatch verifies GetByURI returns empty for an unclaimed URI.
func (s *StorageContractSuite) TestStorage_ChangeCreateAndGet_NoMatch() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-nomatch"

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, "github://github.example.com/uber/x/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestStorage_ChangeCreateAndGet_Match verifies a created record is returned by GetByURI.
func (s *StorageContractSuite) TestStorage_ChangeCreateAndGet_Match() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-match"

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, queue+"/1", got[0].RequestID)
	assert.Equal(t, changeURI, got[0].URI)
	assert.Equal(t, queue, got[0].Queue)
	assert.Equal(t, int32(1), got[0].Version)
}

// TestStorage_ChangeGetByURI_DoesNotExcludeSelf verifies the store does not filter by request_id.
func (s *StorageContractSuite) TestStorage_ChangeGetByURI_DoesNotExcludeSelf() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-self"

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1, "store returns the row even when caller might consider it self")
	assert.Equal(t, queue+"/1", got[0].RequestID)
}

// TestStorage_ChangeGetByURI_QueueScoped verifies GetByURI never returns rows from another queue.
func (s *StorageContractSuite) TestStorage_ChangeGetByURI_QueueScoped() {
	t := s.T()
	ctx := s.ctx

	require.NoError(t, s.forQueue("cq-scoped-A").GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: "cq-scoped-A/1", Queue: "cq-scoped-A", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.forQueue("cq-scoped-B").GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	assert.Empty(t, got, "GetByURI must not return rows from a different queue")
}

// TestStorage_ChangeCreate_Idempotent verifies a repeated Create of the same PK is a no-op.
func (s *StorageContractSuite) TestStorage_ChangeCreate_Idempotent() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-idem"
	rec := entity.ChangeRecord{URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1}

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, rec))
	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, rec), "second insert with same PK must succeed (INSERT IGNORE)")

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	assert.Len(t, got, 1, "idempotent create must not duplicate rows")
}

// TestStorage_ChangeCreate_DifferentRequestSameURI verifies distinct requests on one URI coexist.
func (s *StorageContractSuite) TestStorage_ChangeCreate_DifferentRequestSameURI() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-multi"

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))
	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/2", Queue: queue, CreatedAt: 2, UpdatedAt: 2, Version: 1,
	}))

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 2)

	ids := []string{got[0].RequestID, got[1].RequestID}
	sort.Strings(ids)
	assert.Equal(t, []string{queue + "/1", queue + "/2"}, ids)
}

// sampleDetails is a representative ChangeDetails reused across change-store contract tests.
func sampleDetails() entity.ChangeDetails {
	return entity.ChangeDetails{
		Author: entity.Author{Name: "Ada Lovelace", Email: "ada@example.com"},
		ChangedFiles: []entity.ChangedFile{
			{Path: "main.go", LinesAdded: 10, LinesDeleted: 3, LinesModified: 2},
			{Path: "main_test.go", LinesAdded: 20, LinesDeleted: 0},
		},
	}
}

// TestStorage_ChangeCreate_PreservesDetails verifies typed Details round-trip through the store.
func (s *StorageContractSuite) TestStorage_ChangeCreate_PreservesDetails() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-details"
	details := sampleDetails()

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, Details: details, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, details, got[0].Details)
}

// TestStorage_ChangeCreate_EmptyDetails verifies a zero-value Details round-trips (stored as a JSON object).
func (s *StorageContractSuite) TestStorage_ChangeCreate_EmptyDetails() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-emptydetails"

	require.NoError(t, s.forQueue(queue).GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.forQueue(queue).GetChangeStore().GetByURI(ctx, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, entity.ChangeDetails{}, got[0].Details)
}

// TestStorage_BuildCreateAndGet verifies a build round-trips through the
// store: ID is the runner-minted build identifier, and Get by ID finds the
// build with every field intact.
func (s *StorageContractSuite) TestStorage_BuildCreateAndGet() {
	t := s.T()
	ctx := s.ctx
	const batchID = "q/batch/create-and-get"
	const buildID = "runner/create-and-get/42"

	build := entity.Build{
		ID:      buildID,
		BatchID: batchID,
		Status:  entity.BuildStatusRunning,
	}

	require.NoError(t, s.forQueue("test-queue").GetBuildStore().Create(ctx, build))

	got, err := s.forQueue("test-queue").GetBuildStore().Get(ctx, buildID)
	require.NoError(t, err, "Get by ID should find the build")
	assert.Equal(t, build.ID, got.ID)
	assert.Equal(t, build.BatchID, got.BatchID)
	assert.Equal(t, build.Status, got.Status)
}

func (s *StorageContractSuite) TestStorage_RequestSummaryCreateGetAndCAS() {
	t := s.T()
	ctx := s.ctx
	summary := entity.RequestSummary{
		RequestID: "summary/1", Queue: "summary-q", ChangeURIs: nil, ReceivedAtMs: 100,
		Status: entity.RequestStatusAccepted, RequestVersion: 1, StatusTimestampMs: 100, Version: 1,
		LastError: "", Metadata: nil,
	}
	store := s.summaries

	require.NoError(t, store.Create(ctx, summary))
	require.ErrorIs(t, store.Create(ctx, summary), storage.ErrAlreadyExists)

	got, err := store.Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.Equal(t, []string{}, got.ChangeURIs)
	assert.Equal(t, map[string]string{}, got.Metadata)
	_, err = store.Get(ctx, "summary/missing")
	require.ErrorIs(t, err, storage.ErrNotFound)

	got.Queue = "summary-q-updated"
	got.ChangeURIs = []string{"change/updated"}
	got.ReceivedAtMs = 200
	got.Status = entity.RequestStatusLanded
	got.RequestVersion = 2
	got.StatusTimestampMs = 300
	got.LastError = "terminal detail"
	got.Metadata = map[string]string{"source": "test"}
	require.NoError(t, store.Update(ctx, got, 1, 2))

	updated, err := store.Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.Equal(t, entity.RequestSummary{
		RequestID:         summary.RequestID,
		Queue:             "summary-q-updated",
		ChangeURIs:        []string{"change/updated"},
		ReceivedAtMs:      200,
		Status:            entity.RequestStatusLanded,
		RequestVersion:    2,
		StatusTimestampMs: 300,
		Version:           2,
		LastError:         "terminal detail",
		Metadata:          map[string]string{"source": "test"},
	}, updated)

	stale := updated
	stale.Queue = "stale-q"
	stale.ChangeURIs = []string{"change/stale"}
	stale.ReceivedAtMs = 400
	stale.Status = entity.RequestStatusError
	stale.RequestVersion = 3
	stale.StatusTimestampMs = 500
	stale.LastError = "stale detail"
	stale.Metadata = map[string]string{"source": "stale"}
	require.ErrorIs(t, store.Update(ctx, stale, 1, 3), storage.ErrVersionMismatch)

	afterStale, err := store.Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.Equal(t, updated, afterStale)

	updated.ChangeURIs = nil
	updated.Metadata = nil
	require.NoError(t, store.Update(ctx, updated, 2, 3))

	normalizedNil, err := store.Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.Equal(t, []string{}, normalizedNil.ChangeURIs)
	assert.Equal(t, map[string]string{}, normalizedNil.Metadata)
	assert.Equal(t, int32(3), normalizedNil.Version)

	normalizedNil.ChangeURIs = []string{}
	normalizedNil.Metadata = map[string]string{}
	require.NoError(t, store.Update(ctx, normalizedNil, 3, 4))

	normalizedEmpty, err := store.Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.Equal(t, []string{}, normalizedEmpty.ChangeURIs)
	assert.Equal(t, map[string]string{}, normalizedEmpty.Metadata)
	assert.Equal(t, int32(4), normalizedEmpty.Version)
}

func (s *StorageContractSuite) TestStorage_RequestQueueSummaryListAndCursor() {
	t := s.T()
	ctx := s.ctx
	store := s.forQueue("queue-summary").GetRequestQueueSummaryStore()
	rows := []entity.RequestQueueSummary{
		{RequestID: "queue-summary/1", Queue: "queue-summary", ChangeURIs: nil, ReceivedAtMs: 100, Status: entity.RequestStatusAccepted, Version: 1, Metadata: nil},
		{RequestID: "queue-summary/2", Queue: "queue-summary", ChangeURIs: []string{"uri/2"}, ReceivedAtMs: 200, Status: entity.RequestStatusLanded, Version: 1, Metadata: map[string]string{}},
		{RequestID: "queue-summary/3", Queue: "queue-summary", ChangeURIs: []string{"uri/3"}, ReceivedAtMs: 200, Status: entity.RequestStatusError, Version: 1, Metadata: map[string]string{}},
	}
	for _, row := range rows {
		require.NoError(t, store.Create(ctx, row))
	}
	require.ErrorIs(t, store.Create(ctx, rows[0]), storage.ErrAlreadyExists)

	got, err := store.Get(ctx, rows[0].ReceivedAtMs, rows[0].RequestID)
	require.NoError(t, err)
	assert.NotNil(t, got.ChangeURIs)
	assert.NotNil(t, got.Metadata)
	_, err = store.Get(ctx, 999, "queue-summary/missing")
	require.ErrorIs(t, err, storage.ErrNotFound)

	got.Status = entity.RequestStatusLanded
	got.ChangeURIs = []string{"uri/replacement/1", "uri/replacement/2"}
	got.LastError = "done"
	got.Metadata = map[string]string{"result": "landed"}
	require.NoError(t, store.Update(ctx, got, 1, 2))
	updated, err := store.Get(ctx, got.ReceivedAtMs, got.RequestID)
	require.NoError(t, err)
	assert.Equal(t, int32(2), updated.Version)
	assert.Equal(t, []string{"uri/replacement/1", "uri/replacement/2"}, updated.ChangeURIs)
	assert.Equal(t, entity.RequestStatusLanded, updated.Status)
	assert.Equal(t, "done", updated.LastError)
	assert.Equal(t, map[string]string{"result": "landed"}, updated.Metadata)

	stale := updated
	stale.ChangeURIs = []string{}
	stale.Status = entity.RequestStatusError
	stale.LastError = "stale"
	stale.Metadata = map[string]string{}
	require.ErrorIs(t, store.Update(ctx, stale, 1, 3), storage.ErrVersionMismatch)
	unchanged, err := store.Get(ctx, got.ReceivedAtMs, got.RequestID)
	require.NoError(t, err)
	assert.Equal(t, updated, unchanged)

	updated.ChangeURIs = nil
	updated.Metadata = nil
	require.NoError(t, store.Update(ctx, updated, 2, 3))
	normalized, err := store.Get(ctx, got.ReceivedAtMs, got.RequestID)
	require.NoError(t, err)
	assert.Equal(t, int32(3), normalized.Version)
	assert.NotNil(t, normalized.ChangeURIs)
	assert.Empty(t, normalized.ChangeURIs)
	assert.NotNil(t, normalized.Metadata)
	assert.Empty(t, normalized.Metadata)

	normalized.ChangeURIs = []string{}
	normalized.Metadata = map[string]string{}
	require.NoError(t, store.Update(ctx, normalized, 3, 4))
	emptyCollections, err := store.Get(ctx, got.ReceivedAtMs, got.RequestID)
	require.NoError(t, err)
	assert.Equal(t, int32(4), emptyCollections.Version)
	assert.NotNil(t, emptyCollections.ChangeURIs)
	assert.Empty(t, emptyCollections.ChangeURIs)
	assert.NotNil(t, emptyCollections.Metadata)
	assert.Empty(t, emptyCollections.Metadata)

	firstPage, err := store.List(ctx, storage.RequestQueueSummaryQuery{
		ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, Limit: 2,
	})
	require.NoError(t, err)
	require.Len(t, firstPage, 2)
	assert.Equal(t, []string{"queue-summary/3", "queue-summary/2"}, []string{firstPage[0].RequestID, firstPage[1].RequestID})

	secondPage, err := store.List(ctx, storage.RequestQueueSummaryQuery{
		ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, Limit: 2,
		HasCursor: true, Cursor: storage.RequestQueueSummaryCursor{ReceivedAtMs: 200, RequestID: "queue-summary/2"},
	})
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	assert.Equal(t, "queue-summary/1", secondPage[0].RequestID)
	assert.NotNil(t, secondPage[0].ChangeURIs)
	assert.NotNil(t, secondPage[0].Metadata)

	bounded, err := store.List(ctx, storage.RequestQueueSummaryQuery{
		ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, bounded, 1)
	assert.Equal(t, "queue-summary/1", bounded[0].RequestID)

	empty, err := store.List(ctx, storage.RequestQueueSummaryQuery{
		ReceivedAtOrAfterMs: 300, ReceivedBeforeMs: 400, Limit: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func (s *StorageContractSuite) TestStorage_RequestURIListIsBoundedAndOrdered() {
	t := s.T()
	ctx := s.ctx
	store := s.uris
	rows := []entity.RequestURI{
		{ChangeURI: "uri/shared", ReceivedAtMs: 100, RequestID: "uri/1"},
		{ChangeURI: "uri/shared", ReceivedAtMs: 200, RequestID: "uri/2"},
		{ChangeURI: "uri/shared", ReceivedAtMs: 200, RequestID: "uri/3"},
	}
	for _, row := range rows {
		require.NoError(t, store.Create(ctx, row))
	}
	require.ErrorIs(t, store.Create(ctx, rows[0]), storage.ErrAlreadyExists)

	got, err := store.ListByURI(ctx, "uri/shared", 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"uri/3", "uri/2"}, []string{got[0].RequestID, got[1].RequestID})

	empty, err := store.ListByURI(ctx, "uri/missing", 2)
	require.NoError(t, err)
	assert.Empty(t, empty)
}
