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
	"sort"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// StorageContractSuite defines the contract tests for the storage.Storage interface.
// All storage implementations must pass these tests.
// Implementation-specific tests should embed this suite and call SetStorage().
type StorageContractSuite struct {
	suite.Suite
	ctx     context.Context
	storage storage.Storage
	log     *testutil.TestLogger
}

// SetContext sets the context for tests
func (s *StorageContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetStorage is called by implementation tests to provide the concrete storage instance
func (s *StorageContractSuite) SetStorage(store storage.Storage) {
	s.storage = store
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
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err, "failed to create request")

	// Get request back
	retrieved, err := s.storage.GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err, "failed to get request")

	// Verify fields
	assert.Equal(t, request.ID, retrieved.ID)
	assert.Equal(t, request.Queue, retrieved.Queue)
	assert.Equal(t, request.State, retrieved.State)
	assert.Equal(t, request.Change.URIs, retrieved.Change.URIs)
	assert.Equal(t, request.LandStrategy, retrieved.LandStrategy)
	assert.Equal(t, request.Version, retrieved.Version)

	s.log.Logf("CreateAndGet test passed: created and retrieved request %s", request.ID)
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
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err, "failed to create request with stacked PRs")

	// Get request back
	retrieved, err := s.storage.GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err, "failed to get request with stacked PRs")

	// Verify the stacked URIs are preserved
	assert.Equal(t, stackedURIs, retrieved.Change.URIs, "stacked PR URIs should be preserved exactly")
	assert.Equal(t, request.ID, retrieved.ID)
	assert.Equal(t, request.LandStrategy, retrieved.LandStrategy)

	s.log.Logf("CreateAndGet_StackedPRs test passed: %d stacked URIs", len(stackedURIs))
}

// TestStorage_UpdateState tests updating request state
func (s *StorageContractSuite) TestStorage_UpdateState() {
	t := s.T()
	ctx := s.ctx

	request := entity.Request{
		ID:           "test/update",
		Queue:        "test-queue",
		State:        entity.RequestStateStarted,
		LandStrategy: mergestrategy.MergeStrategyMerge,
		Version:      1,
	}

	// Create initial request
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Update state
	err = s.storage.GetRequestStore().UpdateState(ctx, request.ID, request.Version, request.Version+1, entity.RequestStateProcessing)
	require.NoError(t, err, "failed to update request state")

	// Verify update
	retrieved, err := s.storage.GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err)
	assert.Equal(t, entity.RequestStateProcessing, retrieved.State)
	assert.Equal(t, int32(2), retrieved.Version, "version should increment after update")

	s.log.Logf("UpdateState test passed: updated request %s to state %s", request.ID, retrieved.State)
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
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Update with correct version
	err = s.storage.GetRequestStore().UpdateState(ctx, request.ID, 1, 2, entity.RequestStateProcessing)
	require.NoError(t, err, "update with correct version should succeed")

	// Try to update with stale version (should fail)
	err = s.storage.GetRequestStore().UpdateState(ctx, request.ID, 1, 2, entity.RequestStateLanded)
	assert.Error(t, err, "update with stale version should fail")
	assert.ErrorIs(t, err, storage.ErrVersionMismatch, "should return ErrVersionMismatch")

	// Verify state wasn't changed by stale update
	retrieved, err := s.storage.GetRequestStore().Get(ctx, request.ID)
	require.NoError(t, err)
	assert.Equal(t, entity.RequestStateProcessing, retrieved.State, "stale update should not modify state")
	assert.Equal(t, int32(2), retrieved.Version)

	s.log.Logf("Optimistic locking test passed: prevented stale update for request %s", request.ID)
}

// TestStorage_NotFound tests getting a non-existent request
func (s *StorageContractSuite) TestStorage_NotFound() {
	t := s.T()
	ctx := s.ctx

	// Try to get non-existent request
	_, err := s.storage.GetRequestStore().Get(ctx, "test/nonexistent")
	assert.Error(t, err, "getting non-existent request should return error")
	assert.ErrorIs(t, err, storage.ErrNotFound, "should return ErrNotFound")

	s.log.Logf("NotFound test passed: correctly returned ErrNotFound")
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
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Try to create duplicate
	err = s.storage.GetRequestStore().Create(ctx, request)
	assert.Error(t, err, "creating duplicate request should return error")
	assert.ErrorIs(t, err, storage.ErrAlreadyExists, "should return ErrAlreadyExists")

	s.log.Logf("CreateDuplicate test passed: prevented duplicate creation")
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

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, "github://github.example.com/uber/x/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestStorage_ChangeCreateAndGet_Match verifies a created record is returned by GetByURI.
func (s *StorageContractSuite) TestStorage_ChangeCreateAndGet_Match() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-match"

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, changeURI)
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

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1, "store returns the row even when caller might consider it self")
	assert.Equal(t, queue+"/1", got[0].RequestID)
}

// TestStorage_ChangeGetByURI_QueueScoped verifies GetByURI never returns rows from another queue.
func (s *StorageContractSuite) TestStorage_ChangeGetByURI_QueueScoped() {
	t := s.T()
	ctx := s.ctx

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: "cq-scoped-A/1", Queue: "cq-scoped-A", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, "cq-scoped-B", changeURI)
	require.NoError(t, err)
	assert.Empty(t, got, "GetByURI must not return rows from a different queue")
}

// TestStorage_ChangeCreate_Idempotent verifies a repeated Create of the same PK is a no-op.
func (s *StorageContractSuite) TestStorage_ChangeCreate_Idempotent() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-idem"
	rec := entity.ChangeRecord{URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1}

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, rec))
	require.NoError(t, s.storage.GetChangeStore().Create(ctx, rec), "second insert with same PK must succeed (INSERT IGNORE)")

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, changeURI)
	require.NoError(t, err)
	assert.Len(t, got, 1, "idempotent create must not duplicate rows")
}

// TestStorage_ChangeCreate_DifferentRequestSameURI verifies distinct requests on one URI coexist.
func (s *StorageContractSuite) TestStorage_ChangeCreate_DifferentRequestSameURI() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-multi"

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))
	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/2", Queue: queue, CreatedAt: 2, UpdatedAt: 2, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, changeURI)
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

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, Details: details, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, details, got[0].Details)
}

// TestStorage_ChangeCreate_EmptyDetails verifies a zero-value Details round-trips (stored as a JSON object).
func (s *StorageContractSuite) TestStorage_ChangeCreate_EmptyDetails() {
	t := s.T()
	ctx := s.ctx
	const queue = "cq-emptydetails"

	require.NoError(t, s.storage.GetChangeStore().Create(ctx, entity.ChangeRecord{
		URI: changeURI, RequestID: queue + "/1", Queue: queue, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, changeURI)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, entity.ChangeDetails{}, got[0].Details)
}

// sampleSpeculationTree returns a representative tree for the given batch: a
// fallback path (build alone), a speculative path in flight on an assumed-good
// base, a budget-cleared path, and a path being cancelled — exercising every
// SpeculationPathInfo field and the intent statuses (Prioritized, Cancelling).
func sampleSpeculationTree(batchID string) entity.SpeculationTree {
	return entity.SpeculationTree{
		BatchID: batchID,
		Paths: []entity.SpeculationPathInfo{
			{
				Path:   entity.SpeculationPath{Base: nil, Head: batchID},
				Score:  0.5,
				Status: entity.SpeculationPathStatusCandidate,
			},
			{
				Path:    entity.SpeculationPath{Base: []string{"q/batch/1", "q/batch/2"}, Head: batchID},
				Score:   0.25,
				Status:  entity.SpeculationPathStatusBuilding,
				BuildID: "build-42",
			},
			{
				Path:   entity.SpeculationPath{Base: []string{"q/batch/1"}, Head: batchID},
				Score:  0.4,
				Status: entity.SpeculationPathStatusPrioritized,
			},
			{
				Path:    entity.SpeculationPath{Base: []string{"q/batch/2"}, Head: batchID},
				Score:   0.1,
				Status:  entity.SpeculationPathStatusCancelling,
				BuildID: "build-43",
			},
		},
		Version: 1,
	}
}

// TestStorage_SpeculationCreateAndGet verifies a tree round-trips through the
// store preserving every path field (Base/Head, Score, Status, BuildID).
func (s *StorageContractSuite) TestStorage_SpeculationCreateAndGet() {
	t := s.T()
	ctx := s.ctx

	tree := sampleSpeculationTree("spec/create-get")

	require.NoError(t, s.storage.GetSpeculationTreeStore().Create(ctx, tree))

	retrieved, err := s.storage.GetSpeculationTreeStore().Get(ctx, tree.BatchID)
	require.NoError(t, err)
	assert.Equal(t, tree, retrieved, "speculation tree should round-trip unchanged")
}

// TestStorage_SpeculationCreateDuplicate verifies a repeated Create for the same
// batch returns ErrAlreadyExists.
func (s *StorageContractSuite) TestStorage_SpeculationCreateDuplicate() {
	t := s.T()
	ctx := s.ctx

	tree := sampleSpeculationTree("spec/duplicate")

	require.NoError(t, s.storage.GetSpeculationTreeStore().Create(ctx, tree))

	err := s.storage.GetSpeculationTreeStore().Create(ctx, tree)
	assert.ErrorIs(t, err, storage.ErrAlreadyExists, "duplicate create should return ErrAlreadyExists")
}

// TestStorage_SpeculationUpdate verifies Update overwrites the entire set of
// paths for a batch (the controller persists the whole tree each respeculate)
// under the version guard, and that a stale version is rejected with
// ErrVersionMismatch without modifying the stored tree.
func (s *StorageContractSuite) TestStorage_SpeculationUpdate() {
	t := s.T()
	ctx := s.ctx

	tree := sampleSpeculationTree("spec/update")
	require.NoError(t, s.storage.GetSpeculationTreeStore().Create(ctx, tree))

	// Respeculate: the speculative base broke, so its path is cancelled and the
	// fallback advanced to passed — a wholesale replacement of the paths.
	updatedPaths := []entity.SpeculationPathInfo{
		{
			Path:    entity.SpeculationPath{Base: nil, Head: tree.BatchID},
			Score:   0.75,
			Status:  entity.SpeculationPathStatusPassed,
			BuildID: "build-99",
		},
	}
	require.NoError(t, s.storage.GetSpeculationTreeStore().Update(ctx, tree.BatchID, tree.Version, tree.Version+1, updatedPaths))

	retrieved, err := s.storage.GetSpeculationTreeStore().Get(ctx, tree.BatchID)
	require.NoError(t, err)
	want := entity.SpeculationTree{BatchID: tree.BatchID, Paths: updatedPaths, Version: tree.Version + 1}
	assert.Equal(t, want, retrieved, "Update should overwrite the whole tree and bump the version")

	// A write against the already-consumed version must fail and leave the
	// stored tree untouched.
	err = s.storage.GetSpeculationTreeStore().Update(ctx, tree.BatchID, tree.Version, tree.Version+1, sampleSpeculationTree(tree.BatchID).Paths)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch, "stale update should return ErrVersionMismatch")

	retrieved, err = s.storage.GetSpeculationTreeStore().Get(ctx, tree.BatchID)
	require.NoError(t, err)
	assert.Equal(t, want, retrieved, "stale update should not modify the tree")
}

// TestStorage_SpeculationGetNotFound verifies Get for an unknown batch returns ErrNotFound.
func (s *StorageContractSuite) TestStorage_SpeculationGetNotFound() {
	t := s.T()
	ctx := s.ctx

	_, err := s.storage.GetSpeculationTreeStore().Get(ctx, "spec/nonexistent")
	assert.ErrorIs(t, err, storage.ErrNotFound, "Get for unknown batch should return ErrNotFound")
}

// TestStorage_SpeculationUpdateNotFound verifies Update for an unknown batch
// returns ErrVersionMismatch — the conditional write matches no row, and the
// store cannot distinguish a missing tree from a stale version.
func (s *StorageContractSuite) TestStorage_SpeculationUpdateNotFound() {
	t := s.T()
	ctx := s.ctx

	tree := sampleSpeculationTree("spec/update-nonexistent")
	err := s.storage.GetSpeculationTreeStore().Update(ctx, tree.BatchID, tree.Version, tree.Version+1, tree.Paths)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch, "Update for unknown batch should return ErrVersionMismatch")
}
func (s *StorageContractSuite) TestStorage_RequestSummaryCreateGetAndCAS() {
	t := s.T()
	ctx := s.ctx
	summary := entity.RequestSummary{
		RequestID: "summary/1", Queue: "summary-q", ChangeURIs: nil, ReceivedAtMs: 100,
		Status: entity.RequestStatusAccepted, StatusTimestampMs: 100, Version: 1, Metadata: nil,
	}

	require.NoError(t, s.storage.GetRequestSummaryStore().Create(ctx, summary))
	require.ErrorIs(t, s.storage.GetRequestSummaryStore().Create(ctx, summary), storage.ErrAlreadyExists)

	got, err := s.storage.GetRequestSummaryStore().Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.NotNil(t, got.ChangeURIs)
	assert.NotNil(t, got.Metadata)

	got.Status = entity.RequestStatusLanded
	got.RequestVersion = 2
	got.StatusTimestampMs = 200
	require.NoError(t, s.storage.GetRequestSummaryStore().Update(ctx, got, 1, 2))
	require.ErrorIs(t, s.storage.GetRequestSummaryStore().Update(ctx, got, 1, 3), storage.ErrVersionMismatch)

	updated, err := s.storage.GetRequestSummaryStore().Get(ctx, summary.RequestID)
	require.NoError(t, err)
	assert.Equal(t, int32(2), updated.Version)
	assert.Equal(t, entity.RequestStatusLanded, updated.Status)
}

func (s *StorageContractSuite) TestStorage_RequestQueueSummaryListAndCursor() {
	t := s.T()
	ctx := s.ctx
	store := s.storage.GetRequestQueueSummaryStore()
	rows := []entity.RequestQueueSummary{
		{RequestID: "queue-summary/1", Queue: "queue-summary", ChangeURIs: nil, ReceivedAtMs: 100, Status: entity.RequestStatusAccepted, Version: 1, Metadata: nil},
		{RequestID: "queue-summary/2", Queue: "queue-summary", ChangeURIs: []string{"uri/2"}, ReceivedAtMs: 200, Status: entity.RequestStatusLanded, Version: 1, Metadata: map[string]string{}},
		{RequestID: "queue-summary/3", Queue: "queue-summary", ChangeURIs: []string{"uri/3"}, ReceivedAtMs: 200, Status: entity.RequestStatusError, Version: 1, Metadata: map[string]string{}},
	}
	for _, row := range rows {
		require.NoError(t, store.Create(ctx, row))
	}
	require.ErrorIs(t, store.Create(ctx, rows[0]), storage.ErrAlreadyExists)

	firstPage, err := store.List(ctx, storage.RequestQueueSummaryQuery{
		Queue: "queue-summary", ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, Limit: 2,
	})
	require.NoError(t, err)
	require.Len(t, firstPage, 2)
	assert.Equal(t, []string{"queue-summary/3", "queue-summary/2"}, []string{firstPage[0].RequestID, firstPage[1].RequestID})

	secondPage, err := store.List(ctx, storage.RequestQueueSummaryQuery{
		Queue: "queue-summary", ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, Limit: 2,
		HasCursor: true, Cursor: storage.RequestQueueSummaryCursor{ReceivedAtMs: 200, RequestID: "queue-summary/2"},
	})
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	assert.Equal(t, "queue-summary/1", secondPage[0].RequestID)
	assert.NotNil(t, secondPage[0].ChangeURIs)
	assert.NotNil(t, secondPage[0].Metadata)
}

func (s *StorageContractSuite) TestStorage_RequestURIListIsBoundedAndOrdered() {
	t := s.T()
	ctx := s.ctx
	store := s.storage.GetRequestURIStore()
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
}
