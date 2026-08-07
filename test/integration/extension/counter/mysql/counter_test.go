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
	"github.com/uber/submitqueue/platform/extension/counter"
	mysqlcounter "github.com/uber/submitqueue/platform/extension/counter/mysql"
	countersuite "github.com/uber/submitqueue/test/integration/extension/counter"
	"github.com/uber/submitqueue/test/testutil"
)

// counterFactory binds the shared MySQL pool to a queue, mirroring how the service
// wiring adapts the backend to the counter.Factory seam.
type counterFactory struct{ db *sql.DB }

// For returns the Counter bound to the queue named in config.
func (f counterFactory) For(config counter.Config) (counter.Counter, error) {
	return mysqlcounter.NewCounter(f.db, tally.NoopScope, config.QueueName), nil
}

// MySQLCounterIntegrationSuite tests the MySQL counter implementation
// by embedding the shared contract suite.
type MySQLCounterIntegrationSuite struct {
	countersuite.CounterContractSuite
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestMySQLCounterIntegration(t *testing.T) {
	suite.Run(t, new(MySQLCounterIntegrationSuite))
}

func (s *MySQLCounterIntegrationSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting MySQL Counter integration test suite using docker-compose")

	// Use docker-compose to start MySQL (schema applied programmatically)
	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		ctx,
		"docker-compose.yml",
		"ext-counter-mysql", // Test context for meaningful container names
	)

	// Start the compose stack (MySQL only, no schema)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to MySQL using utility
	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	// Apply schemas programmatically from directory
	schemaDir := testutil.SchemaDir("platform/extension/counter/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	s.log.Logf("Schemas applied successfully")

	// Create counter factory
	fty := counterFactory{db: s.db}

	// Provide the counter factory to the contract suite
	s.SetContext(ctx)
	s.SetFactory(fty)
	s.SetLogger(s.log)

	t.Cleanup(func() {
		if s.db != nil {
			s.log.Logf("Closing MySQL connection")
			s.db.Close()
		}
	})

	s.log.Logf("MySQL Counter integration test suite ready")
}

func (s *MySQLCounterIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down MySQL Counter integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}
