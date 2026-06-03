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

package orchestrator

// Orchestrator Integration Tests
//
// These tests use docker-compose from example/submitqueue/orchestrator/server/docker-compose.yml
// which requires pre-built Linux binaries.
//
// Run with make target (builds binary + runs test):
//   make integration-test-orchestrator
//
// For manual testing with docker-compose:
//   make docker-orchestrator

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pb "github.com/uber/submitqueue/submitqueue/orchestrator/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

type OrchestratorIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.SubmitQueueOrchestratorClient
	db      *sql.DB // App database
	queueDB *sql.DB // Queue database
}

func TestOrchestratorIntegration(t *testing.T) {
	suite.Run(t, new(OrchestratorIntegrationSuite))
}

func (s *OrchestratorIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Orchestrator integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from example/submitqueue/orchestrator/server
	// NOTE: Assumes Linux binary is pre-built via make target
	composeFile := filepath.Join(repoRoot, "example/submitqueue/orchestrator/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-submitqueue-orchestrator")

	// Start the compose stack (Orchestrator + 2 MySQL DBs)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to application database
	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err, "failed to connect to MySQL")

	// Connect to queue database
	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	// Apply schemas programmatically to application database
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("extension/queue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// Connect to Orchestrator gRPC service
	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("orchestrator-service", 8080)
	require.NoError(t, err, "failed to connect to orchestrator")
	s.client = pb.NewSubmitQueueOrchestratorClient(conn)

	s.log.Logf("Orchestrator integration test suite ready")
}

func (s *OrchestratorIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Orchestrator integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// TestPingAPI tests the Orchestrator Ping API
func (s *OrchestratorIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Orchestrator Ping failed")
	assert.Equal(t, "orchestrator", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)

	s.log.Logf("Orchestrator Ping test passed: %s", resp.Message)
}
