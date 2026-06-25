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
// These tests use compose from example/stovepipe/docker-compose.yml and require
// a pre-built Linux binary (make integration-test runs //test/integration/...
// and builds all Linux binaries via build-all-linux). Stovepipe is currently a
// Ping-only service with no storage or queue dependencies.
//
// Run with:
//   make integration-test
// or only this package:
//   bazel test //test/integration/stovepipe:stovepipe_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

type StovepipeIntegrationSuite struct {
	suite.Suite
	ctx    context.Context
	log    *testutil.TestLogger
	stack  *testutil.ComposeStack
	client pb.StovepipeClient
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

	composeFile := filepath.Join(repoRoot, "example/stovepipe/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-stovepipe")

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

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
