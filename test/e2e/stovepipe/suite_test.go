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

package e2e_test

// Stovepipe end-to-end tests.
//
// These tests use docker-compose from service/stovepipe/docker-compose.yml,
// which requires a pre-built Linux binary. Run with the make target (builds
// binaries + runs the test):
//
//   make e2e-test
//
// or only this package (after building the binary):
//
//   bazel test //test/e2e/stovepipe:stovepipe_test
//
// The stack runs the Stovepipe gRPC service plus storage and queue MySQL
// backends. Scenarios start through the public gRPC API and assert durable
// downstream outcomes, while integration tests may seed backend state directly.

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
	stovepipetest "github.com/uber/submitqueue/test/testutil/stovepipe"
	"google.golang.org/grpc"
)

type StovepipeE2ESuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.StovepipeClient
	db      *sql.DB // storage database (request, request_uri)
	queueDB *sql.DB // queue database (process stage)
}

func TestStovepipeE2E(t *testing.T) {
	suite.Run(t, new(StovepipeE2ESuite))
}

func (s *StovepipeE2ESuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Stovepipe e2e test suite using docker-compose")

	// Set REPO_ROOT for the docker-compose build context.
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	composeFile := filepath.Join(repoRoot, "service/stovepipe/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-stovepipe")

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

	s.log.Logf("Stovepipe e2e test suite ready")
}

func (s *StovepipeE2ESuite) TearDownSuite() {
	// Compose stack cleanup is handled automatically by t.Cleanup (registered in
	// NewComposeStack).
	s.log.Logf("Tearing down Stovepipe e2e test suite")
}

func (s *StovepipeE2ESuite) TestPing() {
	t := s.T()
	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "e2e test"})
	require.NoError(t, err, "Stovepipe Ping failed")
	assert.Equal(t, "stovepipe", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)
}

// TestIngest_HappyPath_Processes drives a queue's head commit through the whole
// pipeline. It verifies the asynchronous process stage admits the request,
// claims one slot, chooses the cold-start strategy, and publishes to build.
func (s *StovepipeE2ESuite) TestIngest_HappyPath_Processes() {
	const queue = "monorepo/main"

	id := s.ingest(queue)
	s.log.Logf("Ingest succeeded: id=%s; waiting for process stage", id)

	// Synchronous side effects of Ingest.
	s.assertIngestPersisted(queue, id)

	// Asynchronous outcome, including the build-stage handoff.
	stovepipetest.AssertColdStartAdmitted(s.T(), s.db, s.queueDB, queue, id)
}

// TestIngest_Idempotent verifies that re-ingesting the same queue resolves the
// same head URI and dedups to the same request id.
func (s *StovepipeE2ESuite) TestIngest_Idempotent() {
	const queue = "monorepo/release"

	id := s.ingest(queue)
	s.log.Logf("First ingest: id=%s", id)

	id2 := s.ingest(queue)
	assert.Equal(s.T(), id, id2, "re-ingest of the same head should dedup to the same id")
}
