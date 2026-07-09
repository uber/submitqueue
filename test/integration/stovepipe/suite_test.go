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

package stovepipe

// Stovepipe integration tests
//
// These tests use compose from service/stovepipe/docker-compose.yml and require
// a pre-built Linux binary (make integration-test runs //test/integration/...
// and builds all Linux binaries via build-all-linux). The stack runs the
// Stovepipe gRPC service plus a storage MySQL (request, request_uri) and a queue
// MySQL (process stage).
//
// Run with:
//   make integration-test
// or only this package:
//   bazel test //test/integration/stovepipe:stovepipe_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

type StovepipeIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.StovepipeClient
	db      *sql.DB // storage database (request, request_uri)
	queueDB *sql.DB // queue database
}

func TestStovepipeIntegration(t *testing.T) {
	suite.Run(t, new(StovepipeIntegrationSuite))
}

func (s *StovepipeIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Stovepipe integration test suite using compose")

	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	composeFile := filepath.Join(repoRoot, "service/stovepipe/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-stovepipe")

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err, "failed to connect to storage MySQL")

	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	// Apply schemas after the stack is up; the service connects lazily and the
	// consumer retries, so the boot ordering is tolerated.
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("stovepipe/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("stovepipe-service", 8080)
	require.NoError(t, err, "failed to connect to stovepipe service")
	s.client = pb.NewStovepipeClient(conn)
}

func (s *StovepipeIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Stovepipe integration test suite")
}

func (s *StovepipeIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Stovepipe Ping failed")
	assert.Equal(t, "stovepipe", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)
}

// TestIngestAPI exercises the full ingest path: the controller resolves the head
// URI via the (fake) SourceControl, persists the Request and its (queue, URI)
// mapping, and publishes the request id to the process stage. A second ingest of
// the same queue resolves the same head and dedups to the same id.
func (s *StovepipeIntegrationSuite) TestIngestAPI() {
	t := s.T()

	const queue = "monorepo/main"

	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err, "Ingest failed")
	require.NotEmpty(t, resp.Id, "minted request id should not be empty")
	id := resp.Id

	// Request persisted.
	var reqCount int
	require.NoError(t, s.db.QueryRow("SELECT COUNT(*) FROM request WHERE id = ?", id).Scan(&reqCount))
	assert.Equal(t, 1, reqCount, "request row should be persisted")

	// (queue, URI) mapping persisted and points at the minted id.
	var mappedID string
	require.NoError(t, s.db.QueryRow("SELECT request_id FROM request_uri WHERE queue = ?", queue).Scan(&mappedID))
	assert.Equal(t, id, mappedID, "URI mapping should point at the minted request id")

	// Message published to the process topic.
	var msgCount int
	require.NoError(t, s.queueDB.QueryRow("SELECT COUNT(*) FROM queue_messages WHERE id = ?", id).Scan(&msgCount))
	assert.Equal(t, 1, msgCount, "should have published one process message")

	// Re-ingesting the same queue resolves the same head URI and dedups.
	resp2, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err, "second Ingest failed")
	assert.Equal(t, id, resp2.Id, "re-ingest of the same head should dedup to the same id")

	// latest_request_id on the queue row.
	var latestRequestID string
	require.NoError(t, s.db.QueryRow("SELECT latest_request_id FROM queue WHERE name = ?", queue).Scan(&latestRequestID))
	assert.Equal(t, id, latestRequestID, "queue latest_request_id should point at the minted request")
}

// TestIngestEmptyQueue verifies the request-validation error surfaces over gRPC.
func (s *StovepipeIntegrationSuite) TestIngestEmptyQueue() {
	t := s.T()

	_, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: ""})
	require.Error(t, err, "Ingest with empty queue should fail")
}
