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
		State: entity.RequestStateStarted,
		Change: entity.Change{
			URIs: []string{"github://uber/storage-test/pull/123/abc123def"},
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
		"github://uber/monorepo/pull/101/aaa111bbb",
		"github://uber/monorepo/pull/102/ccc222ddd",
		"github://uber/monorepo/pull/103/eee333fff",
		"github://uber/monorepo/pull/104/ggg444hhh",
	}

	request := entity.Request{
		ID:    "test/stacked-prs",
		Queue: "test-queue",
		State: entity.RequestStateStarted,
		Change: entity.Change{
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
		State:        entity.RequestStateStarted,
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
