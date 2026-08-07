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
// All QueueStore implementations must pass these tests. Queue rows are keyed
// by the queue name itself, so each test resolves the store bound to its own
// test queue through the factory.
type QueueStoreContractSuite struct {
	suite.Suite
	ctx     context.Context
	factory storage.Factory
	log     *testutil.TestLogger
}

// SetContext sets the context for tests.
func (s *QueueStoreContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetFactory provides the storage factory under test.
func (s *QueueStoreContractSuite) SetFactory(factory storage.Factory) {
	s.factory = factory
}

// storeFor resolves the QueueStore bound to the named queue.
func (s *QueueStoreContractSuite) storeFor(name string) storage.QueueStore {
	store, err := s.factory.For(storage.Config{QueueName: name})
	s.Require().NoError(err)
	return store.GetQueueStore()
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

	require.NoError(t, s.storeFor(name).Create(s.ctx, entity.Queue{
		Name:    name,
		Version: 1,
	}))

	got, err := s.storeFor(name).Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, entity.Queue{
		Name:            name,
		LatestRequestID: "",
		Version:         1,
	}, got)
}

// TestQueueStore_CreateWithFields verifies caller-supplied initial field values are persisted.
func (s *QueueStoreContractSuite) TestQueueStore_CreateWithFields() {
	t := s.T()
	const name = "contract/defaults"

	toCreate := entity.Queue{
		Name:               name,
		LastGreenURI:       "git://remote/monorepo/main/green-bbbb",
		LastGreenRequestID: "request/contract/defaults/98",
		LatestRequestID:    "request/contract/defaults/99",
		Version:            1,
	}
	require.NoError(t, s.storeFor(name).Create(s.ctx, toCreate))

	got, err := s.storeFor(name).Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, toCreate, got)
}

// TestQueueStore_CreateAlreadyExists verifies a duplicate Create returns ErrAlreadyExists.
func (s *QueueStoreContractSuite) TestQueueStore_CreateAlreadyExists() {
	t := s.T()
	const name = "contract/already-exists"

	first := entity.Queue{Name: name, LatestRequestID: "request/contract/already-exists/3", Version: 1}
	require.NoError(t, s.storeFor(name).Create(s.ctx, first))

	err := s.storeFor(name).Create(s.ctx, entity.Queue{
		Name:            name,
		LastGreenURI:    "git://remote/monorepo/main/ignored-on-race",
		LatestRequestID: "request/contract/already-exists/500",
		Version:         1,
	})
	assert.ErrorIs(t, err, storage.ErrAlreadyExists)

	got, err := s.storeFor(name).Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, first, got)
}

// TestQueueStore_GetNotFound verifies Get returns ErrNotFound for a missing queue.
func (s *QueueStoreContractSuite) TestQueueStore_GetNotFound() {
	t := s.T()

	_, err := s.storeFor("contract/does-not-exist").Get(s.ctx, "contract/does-not-exist")
	assert.True(t, storage.IsNotFound(err))
}

// TestQueueStore_UpdateCAS verifies a conditional update persists all mutable fields and rejects stale versions.
func (s *QueueStoreContractSuite) TestQueueStore_UpdateCAS() {
	t := s.T()
	const name = "contract/update-cas"

	created := entity.Queue{Name: name, Version: 1}
	require.NoError(t, s.storeFor(name).Create(s.ctx, created))

	updated := created
	updated.LastGreenURI = "git://remote/monorepo/main/green-cccc"
	updated.LastGreenRequestID = "request/contract/update-cas/41"
	updated.LatestRequestID = "request/contract/update-cas/42"
	updated.InFlightCount = 1
	require.NoError(t, s.storeFor(name).Update(s.ctx, updated, 1, 2))

	got, err := s.storeFor(name).Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, updated.LastGreenURI, got.LastGreenURI)
	assert.Equal(t, "request/contract/update-cas/41", got.LastGreenRequestID)
	assert.Equal(t, "request/contract/update-cas/42", got.LatestRequestID)
	assert.Equal(t, int32(1), got.InFlightCount)
	assert.Equal(t, int32(2), got.Version)

	err = s.storeFor(name).Update(s.ctx, updated, 1, 2)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
}

// TestQueueStore_UpdateNotFoundIsVersionMismatch verifies Update on a missing row returns ErrVersionMismatch.
func (s *QueueStoreContractSuite) TestQueueStore_UpdateNotFoundIsVersionMismatch() {
	t := s.T()

	err := s.storeFor("contract/missing").Update(s.ctx, entity.Queue{Name: "contract/missing"}, 1, 2)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)
}

// TestQueueStore_UpdateSequentialCAS verifies successive conditional updates advance version monotonically.
func (s *QueueStoreContractSuite) TestQueueStore_UpdateSequentialCAS() {
	t := s.T()
	const name = "contract/sequential-cas"

	require.NoError(t, s.storeFor(name).Create(s.ctx, entity.Queue{Name: name, Version: 1}))

	v2 := entity.Queue{Name: name, LatestRequestID: "request/contract/sequential-cas/10", Version: 1}
	require.NoError(t, s.storeFor(name).Update(s.ctx, v2, 1, 2))

	v3 := entity.Queue{Name: name, LatestRequestID: "request/contract/sequential-cas/10", InFlightCount: 1, Version: 2}
	require.NoError(t, s.storeFor(name).Update(s.ctx, v3, 2, 3))

	got, err := s.storeFor(name).Get(s.ctx, name)
	require.NoError(t, err)
	assert.Equal(t, "request/contract/sequential-cas/10", got.LatestRequestID)
	assert.Equal(t, int32(1), got.InFlightCount)
	assert.Equal(t, int32(3), got.Version)
}

// BuildStoreContractSuite defines contract tests for storage.BuildStore.
// All BuildStore implementations must pass these tests.
type BuildStoreContractSuite struct {
	suite.Suite
	ctx        context.Context
	buildStore storage.BuildStore
	log        *testutil.TestLogger
}

// SetContext sets the context for tests.
func (s *BuildStoreContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetBuildStore provides the concrete BuildStore under test.
func (s *BuildStoreContractSuite) SetBuildStore(store storage.BuildStore) {
	s.buildStore = store
}

// SetLogger sets the logger for tests.
func (s *BuildStoreContractSuite) SetLogger(log *testutil.TestLogger) {
	s.log = log
}

// TestBuildStore_CreateAndGet verifies Create persists caller-supplied fields, readable via Get.
func (s *BuildStoreContractSuite) TestBuildStore_CreateAndGet() {
	t := s.T()
	const id = "contract/create"

	build := entity.Build{
		ID:        id,
		RequestID: "request/contract/create/1",
		Status:    entity.BuildStatusAccepted,
		Version:   1,
	}
	require.NoError(t, s.buildStore.Create(s.ctx, build))

	got, err := s.buildStore.Get(s.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, build, got)

	s.log.Logf("CreateAndGet passed: created build %s", id)
}

// TestBuildStore_CreateAlreadyExists verifies a duplicate Create returns ErrAlreadyExists.
func (s *BuildStoreContractSuite) TestBuildStore_CreateAlreadyExists() {
	t := s.T()
	const id = "contract/already-exists"

	first := entity.Build{
		ID:        id,
		RequestID: "request/contract/already-exists/1",
		Status:    entity.BuildStatusAccepted,
		Version:   1,
	}
	require.NoError(t, s.buildStore.Create(s.ctx, first))

	err := s.buildStore.Create(s.ctx, entity.Build{
		ID:        id,
		RequestID: "request/contract/already-exists/ignored-on-race",
		Status:    entity.BuildStatusRunning,
		Version:   1,
	})
	assert.ErrorIs(t, err, storage.ErrAlreadyExists)

	got, err := s.buildStore.Get(s.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, first, got)

	s.log.Logf("CreateAlreadyExists passed: build %s", id)
}

// TestBuildStore_GetNotFound verifies Get returns ErrNotFound for a missing build.
func (s *BuildStoreContractSuite) TestBuildStore_GetNotFound() {
	t := s.T()

	_, err := s.buildStore.Get(s.ctx, "contract/does-not-exist")
	assert.True(t, storage.IsNotFound(err))

	s.log.Logf("GetNotFound passed")
}

// TestBuildStore_UpdateCAS verifies a conditional update persists the new status and rejects stale versions.
func (s *BuildStoreContractSuite) TestBuildStore_UpdateCAS() {
	t := s.T()
	const id = "contract/update-cas"

	created := entity.Build{
		ID:        id,
		RequestID: "request/contract/update-cas/1",
		Status:    entity.BuildStatusAccepted,
		Version:   1,
	}
	require.NoError(t, s.buildStore.Create(s.ctx, created))

	updated := created
	updated.Status = entity.BuildStatusRunning
	require.NoError(t, s.buildStore.Update(s.ctx, updated, 1, 2))

	got, err := s.buildStore.Get(s.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusRunning, got.Status)
	assert.Equal(t, int32(2), got.Version)

	err = s.buildStore.Update(s.ctx, updated, 1, 2)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)

	s.log.Logf("UpdateCAS passed: build %s", id)
}

// TestBuildStore_UpdateNotFoundIsVersionMismatch verifies Update on a missing row returns ErrVersionMismatch.
func (s *BuildStoreContractSuite) TestBuildStore_UpdateNotFoundIsVersionMismatch() {
	t := s.T()

	err := s.buildStore.Update(s.ctx, entity.Build{ID: "contract/missing"}, 1, 2)
	assert.ErrorIs(t, err, storage.ErrVersionMismatch)

	s.log.Logf("UpdateNotFoundIsVersionMismatch passed")
}

// TestBuildStore_UpdateSequentialCAS verifies successive conditional updates advance version monotonically
// and are write-once on terminal status, per build.md's algorithm step 6.
func (s *BuildStoreContractSuite) TestBuildStore_UpdateSequentialCAS() {
	t := s.T()
	const id = "contract/sequential-cas"

	require.NoError(t, s.buildStore.Create(s.ctx, entity.Build{
		ID:        id,
		RequestID: "request/contract/sequential-cas/1",
		Status:    entity.BuildStatusAccepted,
		Version:   1,
	}))

	v2 := entity.Build{ID: id, Status: entity.BuildStatusRunning, Version: 1}
	require.NoError(t, s.buildStore.Update(s.ctx, v2, 1, 2))

	v3 := entity.Build{ID: id, Status: entity.BuildStatusSucceeded, Version: 2}
	require.NoError(t, s.buildStore.Update(s.ctx, v3, 2, 3))

	got, err := s.buildStore.Get(s.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, got.Status)
	assert.Equal(t, int32(3), got.Version)

	s.log.Logf("UpdateSequentialCAS passed: build %s", id)
}

// TestBuildStore_QueueIsolation verifies updates to one build do not affect another.
func (s *BuildStoreContractSuite) TestBuildStore_QueueIsolation() {
	t := s.T()
	const (
		idA = "contract/isolation-a"
		idB = "contract/isolation-b"
	)

	require.NoError(t, s.buildStore.Create(s.ctx, entity.Build{
		ID: idA, RequestID: "request/contract/isolation-a/1",
		Status: entity.BuildStatusAccepted, Version: 1,
	}))
	require.NoError(t, s.buildStore.Create(s.ctx, entity.Build{
		ID: idB, RequestID: "request/contract/isolation-b/1",
		Status: entity.BuildStatusAccepted, Version: 1,
	}))

	baseline, err := s.buildStore.Get(s.ctx, idB)
	require.NoError(t, err)

	updatedA := entity.Build{
		ID: idA, RequestID: "request/contract/isolation-a/1",
		Status: entity.BuildStatusRunning, Version: 1,
	}
	require.NoError(t, s.buildStore.Update(s.ctx, updatedA, 1, 2))

	gotB, err := s.buildStore.Get(s.ctx, idB)
	require.NoError(t, err)
	assert.Equal(t, baseline, gotB)

	s.log.Logf("QueueIsolation passed: builds %s and %s", idA, idB)
}

// TestQueueStore_QueueIsolation verifies updates to one queue do not affect another.
func (s *QueueStoreContractSuite) TestQueueStore_QueueIsolation() {
	t := s.T()
	const (
		nameA = "contract/isolation-a"
		nameB = "contract/isolation-b"
	)

	storeA := s.storeFor(nameA)
	storeB := s.storeFor(nameB)
	require.NoError(t, storeA.Create(s.ctx, entity.Queue{Name: nameA, Version: 1}))
	require.NoError(t, storeB.Create(s.ctx, entity.Queue{Name: nameB, Version: 1}))

	baseline, err := storeB.Get(s.ctx, nameB)
	require.NoError(t, err)

	updatedA := entity.Queue{
		Name:            nameA,
		LastGreenURI:    "git://remote/monorepo/a/green",
		LatestRequestID: "request/contract/isolation-a/7",
		InFlightCount:   2,
		Version:         1,
	}
	require.NoError(t, storeA.Update(s.ctx, updatedA, 1, 2))

	gotB, err := storeB.Get(s.ctx, nameB)
	require.NoError(t, err)
	assert.Equal(t, baseline, gotB)

	// A store bound to one queue must reject another queue's row outright.
	require.Error(t, storeA.Create(s.ctx, entity.Queue{Name: nameB, Version: 1}))
	_, err = storeA.Get(s.ctx, nameB)
	assert.True(t, storage.IsNotFound(err))
}

// ValidationFactStoreContractSuite defines contract tests for storage.ValidationFactStore.
// All ValidationFactStore implementations must pass these tests.
type ValidationFactStoreContractSuite struct {
	suite.Suite
	ctx     context.Context
	factory storage.Factory
	log     *testutil.TestLogger
}

// SetContext sets the context for tests.
func (s *ValidationFactStoreContractSuite) SetContext(ctx context.Context) {
	s.ctx = ctx
}

// SetFactory provides the Factory that resolves the store under test per queue.
func (s *ValidationFactStoreContractSuite) SetFactory(factory storage.Factory) {
	s.factory = factory
}

// SetLogger sets the logger for tests.
func (s *ValidationFactStoreContractSuite) SetLogger(log *testutil.TestLogger) {
	s.log = log
}

func (s *ValidationFactStoreContractSuite) storeFor(queue string) storage.ValidationFactStore {
	store, err := s.factory.For(storage.Config{QueueName: queue})
	s.Require().NoError(err)
	return store.GetValidationFactStore()
}

// TestValidationFactStore_CreateAndGet verifies Create persists every field, readable via Get.
func (s *ValidationFactStoreContractSuite) TestValidationFactStore_CreateAndGet() {
	t := s.T()
	const queue = "contract/fact-create"

	fact := entity.ValidationFact{
		URI:       "git://remote/contract/fact-create/aaaa",
		Degree:    entity.DegreeGreen,
		RequestID: "request/contract/fact-create/1",
		CreatedAt: 1735689600000,
	}
	require.NoError(t, s.storeFor(queue).Create(s.ctx, fact))

	got, err := s.storeFor(queue).Get(s.ctx, fact.URI, fact.Project)
	require.NoError(t, err)
	assert.Equal(t, fact, got)
}

// TestValidationFactStore_CreateAlreadyExists verifies facts are create-only: a second
// Create for the same identity is rejected and leaves the stored fact untouched.
func (s *ValidationFactStoreContractSuite) TestValidationFactStore_CreateAlreadyExists() {
	t := s.T()
	const queue = "contract/fact-exists"

	first := entity.ValidationFact{
		URI:       "git://remote/contract/fact-exists/aaaa",
		Degree:    entity.DegreeGreen,
		RequestID: "request/contract/fact-exists/1",
		CreatedAt: 1735689600000,
	}
	require.NoError(t, s.storeFor(queue).Create(s.ctx, first))

	err := s.storeFor(queue).Create(s.ctx, entity.ValidationFact{
		URI:       first.URI,
		Degree:    entity.DegreeBroken,
		RequestID: "request/contract/fact-exists/2",
		CreatedAt: 1735689700000,
	})
	assert.ErrorIs(t, err, storage.ErrAlreadyExists)

	// First writer wins: the losing degree must not have overwritten the stored fact.
	got, err := s.storeFor(queue).Get(s.ctx, first.URI, first.Project)
	require.NoError(t, err)
	assert.Equal(t, first, got)
}

// TestValidationFactStore_GetNotFound verifies Get returns ErrNotFound for an unvalidated URI.
func (s *ValidationFactStoreContractSuite) TestValidationFactStore_GetNotFound() {
	t := s.T()

	_, err := s.storeFor("contract/fact-missing").Get(s.ctx, "git://remote/contract/nope", "")
	assert.True(t, storage.IsNotFound(err))
}

// TestValidationFactStore_ProjectIsPartOfIdentity verifies facts for different projects at the
// same URI coexist rather than colliding.
func (s *ValidationFactStoreContractSuite) TestValidationFactStore_ProjectIsPartOfIdentity() {
	t := s.T()
	const queue = "contract/fact-project"
	const uri = "git://remote/contract/fact-project/aaaa"

	wholeRepo := entity.ValidationFact{
		URI:       uri,
		Degree:    entity.DegreeGreen,
		RequestID: "request/contract/fact-project/1",
		CreatedAt: 1735689600000,
	}
	scoped := entity.ValidationFact{
		URI:       uri,
		Project:   "billing",
		Degree:    entity.DegreeBroken,
		RequestID: "request/contract/fact-project/1",
		CreatedAt: 1735689600000,
	}
	require.NoError(t, s.storeFor(queue).Create(s.ctx, wholeRepo))
	require.NoError(t, s.storeFor(queue).Create(s.ctx, scoped))

	gotWhole, err := s.storeFor(queue).Get(s.ctx, uri, "")
	require.NoError(t, err)
	assert.Equal(t, wholeRepo, gotWhole)

	gotScoped, err := s.storeFor(queue).Get(s.ctx, uri, "billing")
	require.NoError(t, err)
	assert.Equal(t, scoped, gotScoped)
}

// TestValidationFactStore_QueueIsolation verifies one queue's fact is unreachable through
// another queue's binding, even at an identical URI.
func (s *ValidationFactStoreContractSuite) TestValidationFactStore_QueueIsolation() {
	t := s.T()
	const (
		queueA = "contract/fact-isolation-a"
		queueB = "contract/fact-isolation-b"
		uri    = "git://remote/shared/aaaa"
	)

	factA := entity.ValidationFact{
		URI:       uri,
		Degree:    entity.DegreeGreen,
		RequestID: "request/contract/fact-isolation-a/1",
		CreatedAt: 1735689600000,
	}
	require.NoError(t, s.storeFor(queueA).Create(s.ctx, factA))

	// The same URI is a distinct identity under a different queue, so it neither
	// collides on Create nor leaks on Get.
	_, err := s.storeFor(queueB).Get(s.ctx, uri, "")
	assert.True(t, storage.IsNotFound(err))

	factB := entity.ValidationFact{
		URI:       uri,
		Degree:    entity.DegreeBroken,
		RequestID: "request/contract/fact-isolation-b/1",
		CreatedAt: 1735689700000,
	}
	require.NoError(t, s.storeFor(queueB).Create(s.ctx, factB))

	gotA, err := s.storeFor(queueA).Get(s.ctx, uri, "")
	require.NoError(t, err)
	assert.Equal(t, factA, gotA)
}
