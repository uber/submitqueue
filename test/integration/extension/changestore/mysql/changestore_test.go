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
	"sort"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/changestore"
	mysqlchangestore "github.com/uber/submitqueue/extension/changestore/mysql"
	"github.com/uber/submitqueue/test/testutil"
)

// MySQLChangeStoreIntegrationSuite tests the MySQL ChangeStore implementation
// against a real MySQL instance launched via docker-compose.
type MySQLChangeStoreIntegrationSuite struct {
	suite.Suite
	stack *testutil.ComposeStack
	db    *sql.DB
	store changestore.ChangeStore
	log   *testutil.TestLogger
	ctx   context.Context
}

func TestMySQLChangeStoreIntegration(t *testing.T) {
	suite.Run(t, new(MySQLChangeStoreIntegrationSuite))
}

func (s *MySQLChangeStoreIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting MySQL ChangeStore integration test suite using docker-compose")

	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		s.ctx,
		"docker-compose.yml",
		"ext-changestore-mysql",
	)

	require.NoError(t, s.stack.Up(), "failed to start compose stack")
	s.log.Logf("Compose stack started successfully")

	var err error
	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("extension/changestore/mysql/schema"))
	s.log.Logf("Schemas applied successfully")

	s.store = mysqlchangestore.NewChangeStore(s.db, tally.NoopScope)

	t.Cleanup(func() {
		if s.db != nil {
			s.log.Logf("Closing MySQL connection")
			s.db.Close()
		}
	})
}

// SetupTest truncates the change table between tests so cases stay isolated.
func (s *MySQLChangeStoreIntegrationSuite) SetupTest() {
	_, err := s.db.ExecContext(s.ctx, "TRUNCATE TABLE `change`")
	require.NoError(s.T(), err)
}

func (s *MySQLChangeStoreIntegrationSuite) TestCreateAndGet_NoMatch() {
	t := s.T()
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RequestID: "q/1", Queue: "q", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "q", "github://uber/x/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func (s *MySQLChangeStoreIntegrationSuite) TestCreateAndGet_Match() {
	t := s.T()
	uri := "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "q/1", Queue: "q", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "q", uri)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "q/1", got[0].RequestID)
	assert.Equal(t, uri, got[0].URI)
	assert.Equal(t, "q", got[0].Queue)
	assert.Equal(t, int32(1), got[0].Version)
}

func (s *MySQLChangeStoreIntegrationSuite) TestGetByURI_DoesNotExcludeSelf() {
	// The store does not filter by request_id; callers filter self if they wish.
	t := s.T()
	uri := "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "q/1", Queue: "q", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "q", uri)
	require.NoError(t, err)
	require.Len(t, got, 1, "store returns the row even when caller might consider it self")
	assert.Equal(t, "q/1", got[0].RequestID)
}

func (s *MySQLChangeStoreIntegrationSuite) TestGetByURI_QueueScoped() {
	t := s.T()
	uri := "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "qA/1", Queue: "qA", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "qB", uri)
	require.NoError(t, err)
	assert.Empty(t, got, "GetByURI must not return rows from a different queue")
}

func (s *MySQLChangeStoreIntegrationSuite) TestCreate_Idempotent() {
	t := s.T()
	rec := entity.ChangeRecord{URI: "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RequestID: "q/1", Queue: "q", CreatedAt: 1, UpdatedAt: 1, Version: 1}

	require.NoError(t, s.store.Create(s.ctx, rec))
	require.NoError(t, s.store.Create(s.ctx, rec), "second insert with same PK must succeed (INSERT IGNORE)")

	var count int
	require.NoError(t, s.db.QueryRowContext(s.ctx, "SELECT COUNT(*) FROM `change`").Scan(&count))
	assert.Equal(t, 1, count, "INSERT IGNORE must not duplicate rows")
}

func (s *MySQLChangeStoreIntegrationSuite) TestCreate_DifferentRequestSameURI() {
	t := s.T()
	uri := "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "q/1", Queue: "q", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "q/2", Queue: "q", CreatedAt: 2, UpdatedAt: 2, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "q", uri)
	require.NoError(t, err)
	require.Len(t, got, 2)

	ids := []string{got[0].RequestID, got[1].RequestID}
	sort.Strings(ids)
	assert.Equal(t, []string{"q/1", "q/2"}, ids)
}

func (s *MySQLChangeStoreIntegrationSuite) TestCreate_PreservesMetadata() {
	t := s.T()
	const meta = `{"title":"add new feature"}`
	uri := "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "q/1", Queue: "q", Metadata: meta, CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "q", uri)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.JSONEq(t, meta, got[0].Metadata)
}

func (s *MySQLChangeStoreIntegrationSuite) TestCreate_EmptyMetadataStoredAsObject() {
	// metadata is NOT NULL in the schema. The impl substitutes '{}' for an empty
	// Metadata field so callers don't need to know about the column constraint.
	t := s.T()
	uri := "github://uber/x/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, s.store.Create(s.ctx, entity.ChangeRecord{
		URI: uri, RequestID: "q/1", Queue: "q", CreatedAt: 1, UpdatedAt: 1, Version: 1,
	}))

	got, err := s.store.GetByURI(s.ctx, "q", uri)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.JSONEq(t, "{}", got[0].Metadata)
}
