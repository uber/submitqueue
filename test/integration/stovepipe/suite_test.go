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

// Stovepipe Integration Tests
//
// These tests use docker-compose from example/server/stovepipe/docker-compose.yml
// which requires a pre-built Linux binary.
//
// Run with make target (builds binary + runs test):
//   make integration-test-stovepipe

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pb "github.com/uber/submitqueue/stovepipe/gateway/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

type StovepipeIntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	log    *testutil.TestLogger
	stack  *testutil.ComposeStack
	client pb.SubmitQueueStovepipeClient
}

func TestStovepipeIntegration(t *testing.T) {
	suite.Run(t, new(StovepipeIntegrationSuite))
}

func (s *StovepipeIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Stovepipe integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from example/server/stovepipe
	// NOTE: Assumes Linux binary is pre-built via make target
	composeFile := filepath.Join(repoRoot, "example/server/stovepipe/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-stovepipe")

	// Start the compose stack (Stovepipe only — stateless service, no DBs)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to Stovepipe gRPC service
	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("stovepipe-service", 8080)
	require.NoError(t, err, "failed to connect to stovepipe")
	s.client = pb.NewSubmitQueueStovepipeClient(conn)

	s.log.Logf("Stovepipe integration test suite ready")
}

func (s *StovepipeIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Stovepipe integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// TestPingAPI tests the Stovepipe Ping API
func (s *StovepipeIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Stovepipe Ping failed")
	assert.Equal(t, "stovepipe", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)

	s.log.Logf("Stovepipe Ping test passed: %s", resp.Message)
}
