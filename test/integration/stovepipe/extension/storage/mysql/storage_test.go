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
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	storagesuite "github.com/uber/submitqueue/test/integration/stovepipe/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// MySQLRequestStoreSuite exercises the MySQL-backed RequestStore against a real MySQL instance
// started via docker-compose.
type MySQLRequestStoreSuite struct {
	suite.Suite
	stack    *testutil.ComposeStack
	db       *sql.DB
	log      *testutil.TestLogger
	ctx      context.Context
	store    storage.RequestStore
	uriStore storage.RequestURIStore
}

func TestMySQLRequestStore(t *testing.T) {
	suite.Run(t, new(MySQLRequestStoreSuite))
}

func (s *MySQLRequestStoreSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		s.ctx,
		"docker-compose.yml",
		"ext-stovepipe-storage-mysql",
	)

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	schemaDir := testutil.SchemaDir("stovepipe/extension/storage/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")
	s.store = store.GetRequestStore()
	s.uriStore = store.GetRequestURIStore()

	t.Cleanup(func() {
		if s.db != nil {
			s.db.Close()
		}
	})
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
	require.ErrorIs(s.T(), err, errs.ErrNotFound)
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
	require.ErrorIs(s.T(), err, errs.ErrVersionMismatch)
}

func (s *MySQLRequestStoreSuite) TestUpdateNotFoundIsVersionMismatch() {
	missing := entity.Request{ID: "request/monorepo/main/missing", State: entity.RequestStateAccepted}
	err := s.store.Update(s.ctx, missing, 1, 2)
	require.ErrorIs(s.T(), err, errs.ErrVersionMismatch)
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
	require.NoError(s.T(), s.uriStore.Create(s.ctx, queue, uri, id))

	got, err := s.uriStore.GetIDByURI(s.ctx, queue, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), id, got)
}

func (s *MySQLRequestStoreSuite) TestGetIDByURINotFound() {
	_, err := s.uriStore.GetIDByURI(s.ctx, "monorepo/main", "git://remote/monorepo/main/unmapped")
	require.ErrorIs(s.T(), err, errs.ErrNotFound)
}

func (s *MySQLRequestStoreSuite) TestURIMappingDuplicate() {
	const (
		queue = "monorepo/main"
		uri   = "git://remote/monorepo/main/cccc3333"
	)
	require.NoError(s.T(), s.uriStore.Create(s.ctx, queue, uri, "request/monorepo/main/4"))

	// A second request claiming the same (queue, uri) is rejected — the dedup signal.
	err := s.uriStore.Create(s.ctx, queue, uri, "request/monorepo/main/5")
	require.ErrorIs(s.T(), err, storage.ErrAlreadyExists)
}

func (s *MySQLRequestStoreSuite) TestURIMappingDistinctAcrossQueues() {
	const uri = "git://remote/monorepo/shared/dddd4444"
	require.NoError(s.T(), s.uriStore.Create(s.ctx, "queue-a", uri, "request/queue-a/1"))
	require.NoError(s.T(), s.uriStore.Create(s.ctx, "queue-b", uri, "request/queue-b/1"))

	idA, err := s.uriStore.GetIDByURI(s.ctx, "queue-a", uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "request/queue-a/1", idA)

	idB, err := s.uriStore.GetIDByURI(s.ctx, "queue-b", uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "request/queue-b/1", idB)
}

// MySQLQueueStoreSuite exercises the MySQL-backed QueueStore by embedding the shared contract suite.
type MySQLQueueStoreSuite struct {
	storagesuite.QueueStoreContractSuite
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestMySQLQueueStore(t *testing.T) {
	suite.Run(t, new(MySQLQueueStoreSuite))
}

func (s *MySQLQueueStoreSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		ctx,
		"docker-compose.yml",
		"ext-stovepipe-storage-mysql",
	)

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	schemaDir := testutil.SchemaDir("stovepipe/extension/storage/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")

	s.SetContext(ctx)
	s.SetQueueStore(store.GetQueueStore())
	s.SetLogger(s.log)

	t.Cleanup(func() {
		if s.db != nil {
			s.db.Close()
		}
	})
}

// MySQLBuildStoreSuite exercises the MySQL-backed BuildStore by embedding the shared contract suite.
type MySQLBuildStoreSuite struct {
	storagesuite.BuildStoreContractSuite
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestMySQLBuildStore(t *testing.T) {
	suite.Run(t, new(MySQLBuildStoreSuite))
}

func (s *MySQLBuildStoreSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		ctx,
		"docker-compose.yml",
		"ext-stovepipe-storage-mysql",
	)

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	schemaDir := testutil.SchemaDir("stovepipe/extension/storage/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")

	s.SetContext(ctx)
	s.SetBuildStore(store.GetBuildStore())
	s.SetLogger(s.log)

	t.Cleanup(func() {
		if s.db != nil {
			s.db.Close()
		}
	})
}
