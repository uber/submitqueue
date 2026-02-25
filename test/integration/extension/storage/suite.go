package storage

import (
	"context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
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
		State: entity.RequestStateNew,
		Change: entity.Change{
			URI: "github://uber/storage-test/pull/123/abc123def",
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
	assert.Equal(t, request.Change.URI, retrieved.Change.URI)
	assert.Equal(t, request.LandStrategy, retrieved.LandStrategy)
	assert.Equal(t, request.Version, retrieved.Version)

	s.log.Logf("CreateAndGet test passed: created and retrieved request %s", request.ID)
}

// TestStorage_CreateAndGet_StackedPRs tests creating and retrieving a request with stacked PRs
func (s *StorageContractSuite) TestStorage_CreateAndGet_StackedPRs() {
	t := s.T()
	ctx := s.ctx

	// Stacked PR URI with multiple PRs encoded in the path
	stackedURI := "github://uber/monorepo/pull/101/aaa111bbb/102/ccc222ddd/103/eee333fff/104/ggg444hhh"

	request := entity.Request{
		ID:    "test/stacked-prs",
		Queue: "test-queue",
		State: entity.RequestStateNew,
		Change: entity.Change{
			URI: stackedURI,
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

	// Verify the full stacked URI is preserved
	assert.Equal(t, stackedURI, retrieved.Change.URI, "stacked PR URI should be preserved exactly")
	assert.Equal(t, request.ID, retrieved.ID)
	assert.Equal(t, request.LandStrategy, retrieved.LandStrategy)

	s.log.Logf("CreateAndGet_StackedPRs test passed: stacked URI length=%d", len(stackedURI))
}

// TestStorage_UpdateState tests updating request state
func (s *StorageContractSuite) TestStorage_UpdateState() {
	t := s.T()
	ctx := s.ctx

	request := entity.Request{
		ID:           "test/update",
		Queue:        "test-queue",
		State:        entity.RequestStateNew,
		LandStrategy: entity.RequestLandStrategyMerge,
		Version:      1,
	}

	// Create initial request
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Update state
	err = s.storage.GetRequestStore().UpdateState(ctx, request.ID, request.Version, entity.RequestStateProcessing)
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
		State:        entity.RequestStateNew,
		LandStrategy: entity.RequestLandStrategyMerge,
		Version:      1,
	}

	// Create request
	err := s.storage.GetRequestStore().Create(ctx, request)
	require.NoError(t, err)

	// Update with correct version
	err = s.storage.GetRequestStore().UpdateState(ctx, request.ID, 1, entity.RequestStateProcessing)
	require.NoError(t, err, "update with correct version should succeed")

	// Try to update with stale version (should fail)
	err = s.storage.GetRequestStore().UpdateState(ctx, request.ID, 1, entity.RequestStateLanded)
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
		State:        entity.RequestStateNew,
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
