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

// Ingest scenarios drive the complete pipeline; List scenarios use fixed read-path
// fixtures. Both exercise the real gRPC service and MySQL in a hermetic Compose stack.
// Run with: bazel test //test/e2e/stovepipe:go_default_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	storagemysql "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

const (
	listPaginationQueue = "e2e-stovepipe/list-pagination"
	listTimeBoundsQueue = "e2e-stovepipe/list-time-bounds"
	listPrimaryQueue    = "e2e-stovepipe/list-primary"
	listOtherQueue      = "e2e-stovepipe/list-other"
	listValidationQueue = "e2e-stovepipe/list-validation"
)

type StovepipeE2ESuite struct {
	suite.Suite
	ctx        context.Context
	log        *testutil.TestLogger
	stack      *testutil.ComposeStack
	client     pb.StovepipeClient
	db         *sql.DB // storage database (request, request_uri)
	queueDB    *sql.DB // queue database (process stage)
	appStorage *storagemysql.Storage
}

func TestStovepipeE2E(t *testing.T) {
	suite.Run(t, new(StovepipeE2ESuite))
}

func (s *StovepipeE2ESuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)
	t.Setenv("MQ_TENANTS", strings.Join([]string{
		"monorepo/main", "monorepo/release", "monorepo/slow?buildrunner-fake=build-slow",
		listPaginationQueue, listTimeBoundsQueue, listPrimaryQueue, listOtherQueue, listValidationQueue,
	}, ","))

	s.log.Logf("Starting Stovepipe e2e test suite using docker-compose")

	composeFile := testutil.Runfile("service/stovepipe/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-stovepipe",
		testutil.WithBuildContext(map[string]string{
			".docker-bin/stovepipe":               "service/stovepipe/server/stovepipe_linux",
			"service/stovepipe/server/Dockerfile": "service/stovepipe/server/Dockerfile",
		}))

	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err, "failed to connect to storage MySQL")

	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	// Apply schemas after the stack is up; the service connects lazily and the
	// consumer retries, so the boot ordering is tolerated.
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("stovepipe/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))
	s.appStorage, err = storagemysql.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err)

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
