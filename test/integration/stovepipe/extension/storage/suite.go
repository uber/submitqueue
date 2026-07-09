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
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// QueueStoreContractSuite defines contract tests for storage.QueueStore.
// All QueueStore implementations must pass these tests.
type QueueStoreContractSuite struct {
	suite.Suite
	ctx        context.Context
	queueStore storage.QueueStore
	log        *testutil.TestLogger
}

// SetContext sets the context for tests.
func (s *QueueStoreContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetQueueStore provides the concrete QueueStore under test.
func (s *QueueStoreContractSuite) SetQueueStore(store storage.QueueStore) {
	s.queueStore = store
}

// SetLogger sets the logger for tests.
func (s *QueueStoreContractSuite) SetLogger(log *testutil.TestLogger) {
	s.log = log
}

func (s *QueueStoreContractSuite) queueDefaults() entity.Queue {
	return entity.Queue{Version: 1}
}

// TestQueueStore_Create verifies Create inserts a new row with caller-supplied fields.
func (s *QueueStoreContractSuite) TestQueueStore_Create() {
	t := s.T()
	const name = "contract/create"

	require.NoError(t, s.queueStore.Create(s.ctx, entity.Queue{
		Name:    name,
		Version: 1,
	}))

	got, err := s.queueStore.Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, entity.Queue{
		Name:             name,
		LatestRequestSeq: 0,
		Version:          1,
	}, got)

	s.log.Logf("Create passed: created queue %s", name)
}

// TestQueueStore_CreateWithFields verifies caller-supplied initial field values are persisted.
func (s *QueueStoreContractSuite) TestQueueStore_CreateWithFields() {
	t := s.T()
	const name = "contract/defaults"

	toCreate := entity.Queue{
		Name:             name,
		LastGreenURI:     "git://remote/monorepo/main/green-bbbb",
		LatestRequestSeq: 99,
		Version:          1,
	}
	require.NoError(t, s.queueStore.Create(s.ctx, toCreate))

	got, err := s.queueStore.Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, toCreate, got)

	s.log.Logf("CreateWithFields passed: persisted fields for queue %s", name)
}

// TestQueueStore_CreateAlreadyExists verifies a duplicate Create returns ErrAlreadyExists.
func (s *QueueStoreContractSuite) TestQueueStore_CreateAlreadyExists() {
	t := s.T()
	const name = "contract/already-exists"

	first := entity.Queue{Name: name, LatestRequestSeq: 3, Version: 1}
	require.NoError(t, s.queueStore.Create(s.ctx, first))

	err := s.queueStore.Create(s.ctx, entity.Queue{
		Name:             name,
		LastGreenURI:     "git://remote/monorepo/main/ignored-on-race",
		LatestRequestSeq: 500,
		Version:          1,
	})
	assert.ErrorIs(t, err, storage.ErrAlreadyExists)

	got, err := s.queueStore.Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, first, got)

	s.log.Logf("CreateAlreadyExists passed: queue %s", name)
}

// TestQueueStore_GetNotFound verifies Get returns ErrNotFound for a missing queue.
func (s *QueueStoreContractSuite) TestQueueStore_GetNotFound() {
	t := s.T()

	_, err := s.queueStore.Get(s.ctx, "contract/does-not-exist")
	assert.True(t, storage.IsNotFound(err))

	s.log.Logf("GetNotFound passed")
}

// TestQueueStore_UpdateCAS verifies a conditional update persists all mutable fields and rejects stale versions.
func (s *QueueStoreContractSuite) TestQueueStore_UpdateCAS() {
	t := s.T()
	const name = "contract/update-cas"

	created := entity.Queue{Name: name, Version: 1}
	require.NoError(t, s.queueStore.Create(s.ctx, created))

	updated := created
	updated.LastGreenURI = "git://remote/monorepo/main/green-cccc"
	updated.LatestRequestSeq = 42
	updated.InFlightCount = 1
	require.NoError(t, s.queueStore.Update(s.ctx, updated, 1, 2))

	got, err := s.queueStore.Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, updated.LastGreenURI, got.LastGreenURI)
	assert.Equal(t, int64(42), got.LatestRequestSeq)
	assert.Equal(t, int32(1), got.InFlightCount)
	assert.Equal(t, int32(2), got.Version)

	err = s.queueStore.Update(s.ctx, updated, 1, 2)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)

	s.log.Logf("UpdateCAS passed: queue %s", name)
}

// TestQueueStore_UpdateNotFoundIsVersionMismatch verifies Update on a missing row returns ErrVersionMismatch.
func (s *QueueStoreContractSuite) TestQueueStore_UpdateNotFoundIsVersionMismatch() {
	t := s.T()

	err := s.queueStore.Update(s.ctx, entity.Queue{Name: "contract/missing"}, 1, 2)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)

	s.log.Logf("UpdateNotFoundIsVersionMismatch passed")
}

// TestQueueStore_UpdateSequentialCAS verifies successive conditional updates advance version monotonically.
func (s *QueueStoreContractSuite) TestQueueStore_UpdateSequentialCAS() {
	t := s.T()
	const name = "contract/sequential-cas"

	require.NoError(t, s.queueStore.Create(s.ctx, entity.Queue{Name: name, Version: 1}))

	v2 := entity.Queue{Name: name, LatestRequestSeq: 10, Version: 1}
	require.NoError(t, s.queueStore.Update(s.ctx, v2, 1, 2))

	v3 := entity.Queue{Name: name, LatestRequestSeq: 10, InFlightCount: 1, Version: 2}
	require.NoError(t, s.queueStore.Update(s.ctx, v3, 2, 3))

	got, err := s.queueStore.Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, int64(10), got.LatestRequestSeq)
	assert.Equal(t, int32(1), got.InFlightCount)
	assert.Equal(t, int32(3), got.Version)

	s.log.Logf("UpdateSequentialCAS passed: queue %s", name)
}

// TestQueueStore_QueueIsolation verifies updates to one queue do not affect another.
func (s *QueueStoreContractSuite) TestQueueStore_QueueIsolation() {
	t := s.T()
	const (
		nameA = "contract/isolation-a"
		nameB = "contract/isolation-b"
	)

	require.NoError(t, s.queueStore.Create(s.ctx, entity.Queue{Name: nameA, Version: 1}))
	require.NoError(t, s.queueStore.Create(s.ctx, entity.Queue{Name: nameB, Version: 1}))

	baseline, err := s.queueStore.Get(s.ctx, nameB)
	require.NoError(t, err)

	updatedA := entity.Queue{
		Name:             nameA,
		LastGreenURI:     "git://remote/monorepo/a/green",
		LatestRequestSeq: 7,
		InFlightCount:    2,
		Version:          1,
	}
	require.NoError(t, s.queueStore.Update(s.ctx, updatedA, 1, 2))

	gotB, err := s.queueStore.Get(s.ctx, nameB)
	require.NoError(t, err)
	assert.Equal(t, baseline, gotB)

	s.log.Logf("QueueIsolation passed: queues %s and %s", nameA, nameB)
}
