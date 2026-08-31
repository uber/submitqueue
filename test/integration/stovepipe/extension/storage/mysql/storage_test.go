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

package mysql

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	storagesuite "github.com/uber/submitqueue/test/integration/stovepipe/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

func TestMySQLStorage(t *testing.T) {
	ctx := context.Background()
	log := testutil.NewTestLogger(t)
	stack := testutil.NewComposeStack(
		t,
		log,
		ctx,
		"docker-compose.yml",
		"ext-stovepipe-storage-mysql",
	)

	err := stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	db, err := stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	schemaDir := testutil.SchemaDir("stovepipe/extension/storage/mysql/schema")
	testutil.ApplySchema(t, log, db, schemaDir)

	backend, err := mysqlstorage.NewStorage(db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")
	factory := mysqlFactory{backend: backend}

	t.Run("RequestStore", func(t *testing.T) {
		resetStorage(t, db)
		bound, err := backend.For("monorepo/main")
		require.NoError(t, err)
		suite.Run(t, &MySQLRequestStoreSuite{
			ctx:      ctx,
			backend:  backend,
			store:    bound.GetRequestStore(),
			uriStore: bound.GetRequestURIStore(),
		})
	})

	t.Run("QueueStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLQueueStoreSuite)
		testSuite.SetContext(ctx)
		testSuite.SetFactory(factory)
		testSuite.SetLogger(testutil.NewTestLogger(t))
		suite.Run(t, testSuite)
	})

	t.Run("BuildStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLBuildStoreSuite)
		testSuite.SetContext(ctx)
		bound, err := backend.For("contract/queue")
		require.NoError(t, err)
		testSuite.SetBuildStore(bound.GetBuildStore())
		testSuite.SetLogger(testutil.NewTestLogger(t))
		suite.Run(t, testSuite)
	})

	t.Run("ValidationFactStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLValidationFactStoreSuite)
		testSuite.SetContext(ctx)
		testSuite.SetFactory(factory)
		testSuite.SetLogger(testutil.NewTestLogger(t))
		suite.Run(t, testSuite)
	})
}

func resetStorage(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, statement := range []string{
		"TRUNCATE TABLE request_history",
		"TRUNCATE TABLE request_uri",
		"TRUNCATE TABLE request",
		"TRUNCATE TABLE build",
		"TRUNCATE TABLE queue",
		"TRUNCATE TABLE validation_fact",
	} {
		_, err := db.ExecContext(context.Background(), statement)
		require.NoError(t, err)
	}
}

// MySQLRequestStoreSuite exercises the MySQL-backed RequestStore against a real MySQL instance.
type MySQLRequestStoreSuite struct {
	suite.Suite
	ctx      context.Context
	backend  *mysqlstorage.Storage
	store    storage.RequestStore
	uriStore storage.RequestURIStore
}

func (s *MySQLRequestStoreSuite) TestCreateAndGet() {
	req := entity.Request{
		ID:      "request/monorepo/main/1",
		Queue:   "monorepo/main",
		URI:     "git://remote/monorepo/main/aaaa1111",
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), req, got)
}

func (s *MySQLRequestStoreSuite) TestCreateAndGetWithProcessFields() {
	req := entity.Request{
		ID:            "request/monorepo/main/process-fields",
		Queue:         "monorepo/main",
		URI:           "git://remote/monorepo/main/cccc3333",
		State:         entity.RequestStateProcessing,
		BuildStrategy: entity.BuildStrategyIncrementalSinceGreen,
		BaseURI:       "git://remote/monorepo/main/green-bbbb",
		Version:       2,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), req, got)
}

func (s *MySQLRequestStoreSuite) TestGetNotFound() {
	_, err := s.store.Get(s.ctx, "request/monorepo/main/does-not-exist")
	require.True(s.T(), storage.IsNotFound(err))
}

func (s *MySQLRequestStoreSuite) TestUpdateCAS() {
	req := entity.Request{
		ID:      "request/monorepo/main/update",
		Queue:   "monorepo/main",
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	// Successful CAS: stored version (1) matches oldVersion; advance to processing with strategy.
	updated := req
	updated.URI = "git://remote/monorepo/main/resolved"
	updated.State = entity.RequestStateProcessing
	updated.BuildStrategy = entity.BuildStrategyFull
	require.NoError(s.T(), s.store.Update(s.ctx, updated, 1, 2))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "git://remote/monorepo/main/resolved", got.URI)
	require.Equal(s.T(), entity.RequestStateProcessing, got.State)
	require.Equal(s.T(), entity.BuildStrategyFull, got.BuildStrategy)
	require.Equal(s.T(), int32(2), got.Version)

	// Stale CAS: oldVersion 1 no longer matches the stored version (2).
	err = s.store.Update(s.ctx, updated, 1, 2)
	require.ErrorIs(s.T(), err, storage.ErrVersionMismatch)
}

func (s *MySQLRequestStoreSuite) TestUpdateNotFoundIsVersionMismatch() {
	missing := entity.Request{ID: "request/monorepo/main/missing", Queue: "monorepo/main", State: entity.RequestStateAccepted}
	err := s.store.Update(s.ctx, missing, 1, 2)
	require.ErrorIs(s.T(), err, storage.ErrVersionMismatch)
}

func (s *MySQLRequestStoreSuite) TestCreateDuplicateID() {
	req := entity.Request{
		ID:      "request/monorepo/main/2",
		Queue:   "monorepo/main",
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	err := s.store.Create(s.ctx, req)
	require.ErrorIs(s.T(), err, storage.ErrAlreadyExists)
}

func (s *MySQLRequestStoreSuite) TestURIMappingCreateAndGet() {
	const (
		queue = "monorepo/main"
		uri   = "git://remote/monorepo/main/bbbb2222"
		id    = "request/monorepo/main/3"
	)
	require.NoError(s.T(), s.uriStore.Create(s.ctx, uri, id))

	got, err := s.uriStore.GetIDByURI(s.ctx, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), id, got)
}

func (s *MySQLRequestStoreSuite) TestGetIDByURINotFound() {
	_, err := s.uriStore.GetIDByURI(s.ctx, "git://remote/monorepo/main/unmapped")
	require.True(s.T(), storage.IsNotFound(err))
}

func (s *MySQLRequestStoreSuite) TestURIMappingDuplicate() {
	const (
		queue = "monorepo/main"
		uri   = "git://remote/monorepo/main/cccc3333"
	)
	require.NoError(s.T(), s.uriStore.Create(s.ctx, uri, "request/monorepo/main/4"))

	// A second request claiming the same (queue, uri) is rejected — the dedup signal.
	err := s.uriStore.Create(s.ctx, uri, "request/monorepo/main/5")
	require.ErrorIs(s.T(), err, storage.ErrAlreadyExists)
}

func (s *MySQLRequestStoreSuite) TestURIMappingDistinctAcrossQueues() {
	const uri = "git://remote/monorepo/shared/dddd4444"
	boundA, err := s.backend.For("queue-a")
	require.NoError(s.T(), err)
	boundB, err := s.backend.For("queue-b")
	require.NoError(s.T(), err)
	require.NoError(s.T(), boundA.GetRequestURIStore().Create(s.ctx, uri, "request/queue-a/1"))
	require.NoError(s.T(), boundB.GetRequestURIStore().Create(s.ctx, uri, "request/queue-b/1"))

	idA, err := boundA.GetRequestURIStore().GetIDByURI(s.ctx, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "request/queue-a/1", idA)

	idB, err := boundB.GetRequestURIStore().GetIDByURI(s.ctx, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "request/queue-b/1", idB)

	// The other queue's mapping is invisible through this queue's binding.
	_, err = boundA.GetRequestURIStore().GetIDByURI(s.ctx, "git://remote/monorepo/shared/only-b")
	require.True(s.T(), storage.IsNotFound(err))
}

// MySQLQueueStoreSuite exercises the MySQL-backed QueueStore by embedding the shared contract suite.
type MySQLQueueStoreSuite struct {
	storagesuite.QueueStoreContractSuite
}

// MySQLValidationFactStoreSuite exercises the MySQL-backed ValidationFactStore by embedding the
// shared contract suite.
type MySQLValidationFactStoreSuite struct {
	storagesuite.ValidationFactStoreContractSuite
}

// MySQLBuildStoreSuite exercises the MySQL-backed BuildStore by embedding the shared contract suite.
type MySQLBuildStoreSuite struct {
	storagesuite.BuildStoreContractSuite
}

// mysqlFactory adapts the MySQL storage backend's queue binding to the
// storage.Factory seam for the contract suite, mirroring the host wiring.
type mysqlFactory struct {
	backend *mysqlstorage.Storage
}

// For returns the queue-scoped store aggregate bound to the queue named in config.
func (f mysqlFactory) For(config storage.Config) (storage.Storage, error) {
	return f.backend.For(config.QueueName)
}
