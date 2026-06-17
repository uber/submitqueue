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

package gateway

// Stovepipe gateway integration tests
//
// These tests use compose from example/stovepipe/gateway/server/docker-compose.yml
// and require a pre-built Linux gateway binary (make integration-test runs
// //test/integration/... and builds all Linux binaries via build-all-linux).
// Only the queue database schema is applied (no SubmitQueue app schema until
// Stovepipe has its own storage schema).
//
// Run with:
//   make integration-test
// or only this package:
//   bazel test //test/integration/stovepipe/gateway:gateway_test

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

type StovepipeGatewayIntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	log    *testutil.TestLogger
	stack  *testutil.ComposeStack
	client pb.StovepipeGatewayClient
}

func TestStovepipeGatewayIntegration(t *testing.T) {
	suite.Run(t, new(StovepipeGatewayIntegrationSuite))
}

func (s *StovepipeGatewayIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Stovepipe gateway integration test suite using compose")

	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	composeFile := filepath.Join(repoRoot, "example/stovepipe/gateway/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-stovepipe-gateway")

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	queueDB, err := s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	testutil.ApplySchema(t, s.log, queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err, "failed to connect to stovepipe gateway")
	s.client = pb.NewStovepipeGatewayClient(conn)
}

func (s *StovepipeGatewayIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Stovepipe gateway integration test suite")
}

func (s *StovepipeGatewayIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Stovepipe Ping failed")
	assert.Equal(t, "stovepipe-gateway", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)
}
