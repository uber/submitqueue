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
	"github.com/uber/submitqueue/entity/change"
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
			URIs: []string{"github://uber/storage-test/pull/123/abcdef0123456789abcdef0123456789abcdef01"},
		},
		LandStrategy: entity.RequestLandStrategyMerge,
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
		"github://uber/monorepo/pull/101/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"github://uber/monorepo/pull/102/cccccccccccccccccccccccccccccccccccccccc",
		"github://uber/monorepo/pull/103/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		"github://uber/monorepo/pull/104/0000000000000000000000000000000000000004",
	}

	request := entity.Request{
		ID:    "test/stacked-prs",
		Queue: "test-queue",
		State: entity.RequestStateStarted,
		Change: change.Change{
			URIs: stackedURIs,
		},
		LandStrategy: entity.RequestLandStrategySquashRebase,
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
		LandStrategy: entity.RequestLandStrategyMerge,
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
		LandStrategy: entity.RequestLandStrategyMerge,
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
		LandStrategy: entity.RequestLandStrategyMerge,
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
const changeURI = "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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

	got, err := s.storage.GetChangeStore().GetByURI(ctx, queue, "github://uber/x/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
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
