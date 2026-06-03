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
	"github.com/uber-go/tally/v4"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/extension/storage/mysql"
	storagesuite "github.com/uber/submitqueue/test/integration/submitqueue/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// MySQLStorageIntegrationSuite tests the MySQL storage implementation
// by embedding the shared contract suite.
type MySQLStorageIntegrationSuite struct {
	storagesuite.StorageContractSuite
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestMySQLStorageIntegration(t *testing.T) {
	suite.Run(t, new(MySQLStorageIntegrationSuite))
}

func (s *MySQLStorageIntegrationSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting MySQL Storage integration test suite using docker-compose")

	// Use docker-compose to start MySQL (schema applied programmatically)
	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		ctx,
		"docker-compose.yml",
		"ext-submitqueue-storage-mysql", // Test context for meaningful container names
	)

	// Start the compose stack (MySQL only, no schema)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to MySQL for schema application
	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	// Apply schemas programmatically from directory
	schemaDir := testutil.SchemaDir("submitqueue/extension/storage/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	s.log.Logf("Schemas applied successfully")

	// Create storage instance using the existing database connection
	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")

	// Provide the storage instance to the contract suite
	s.SetContext(ctx)
	s.SetStorage(store)
	s.SetLogger(s.log)

	t.Cleanup(func() {
		if s.db != nil {
			s.log.Logf("Closing MySQL connection")
			s.db.Close()
		}
	})

	s.log.Logf("MySQL Storage integration test suite ready")
}

func (s *MySQLStorageIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down MySQL Storage integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}
