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

// Gateway Integration Tests
//
// These tests use docker-compose from example/submitqueue/gateway/server/docker-compose.yml
// which requires pre-built Linux binaries.
//
// Run with make target (builds binary + runs test):
//   make integration-test-gateway
//
// For manual testing with docker-compose:
//   make docker-gateway

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GatewayIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.SubmitQueueGatewayClient
	db      *sql.DB // App database
	queueDB *sql.DB // Queue database
}

func TestGatewayIntegration(t *testing.T) {
	suite.Run(t, new(GatewayIntegrationSuite))
}

// The log consumer runs inside the gateway-service container, so this suite can
// only observe persistence black-box through the Status RPC — there is no
// in-process channel/HookSignal to wait on across the container boundary. A
// bounded poll is therefore the deterministic-enough analog: persistTimeout is a
// safety net (a failure here means something is genuinely stuck, not a timing
// race), and persistPollInterval bounds how often we re-query.
const (
	persistTimeout      = 30 * time.Second
	persistPollInterval = 500 * time.Millisecond
)

func (s *GatewayIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Gateway integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from example/submitqueue/gateway/server
	// NOTE: Assumes Linux binary is pre-built via make target
	composeFile := filepath.Join(repoRoot, "example/submitqueue/gateway/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-submitqueue-gateway")

	// Start the compose stack (Gateway + 2 MySQL DBs)
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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// Connect to Gateway gRPC service
	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err, "failed to connect to gateway")
	s.client = pb.NewSubmitQueueGatewayClient(conn)

	s.log.Logf("Gateway integration test suite ready")
}

func (s *GatewayIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Gateway integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// TestPingAPI tests the Gateway Ping API
func (s *GatewayIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Gateway Ping failed")
	assert.Equal(t, "gateway", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)

	s.log.Logf("Gateway Ping test passed: %s", resp.Message)
}

// TestLandAPI tests the Gateway Land API with queue publishing
func (s *GatewayIntegrationSuite) TestLandAPI() {
	t := s.T()

	req := &pb.LandRequest{
		Queue:    "test-queue",
		Change:   &changepb.Change{Uris: []string{"github://uber/integration-test/pull/123/abcdef0123456789abcdef0123456789abcdef01"}},
		Strategy: mergestrategypb.Strategy_REBASE,
	}

	s.log.Logf("Sending Land request for queue=%s", req.Queue)
	resp, err := s.client.Land(s.ctx, req)
	require.NoError(t, err, "Land request failed")
	require.NotEmpty(t, resp.Sqid, "SQID should not be empty")

	s.log.Logf("Land request succeeded: sqid=%s", resp.Sqid)

	// Verify message published to queue
	var msgCount int
	err = s.queueDB.QueryRow("SELECT COUNT(*) FROM queue_messages WHERE id = ?", resp.Sqid).Scan(&msgCount)
	require.NoError(t, err, "failed to query queue messages")
	assert.Equal(t, 1, msgCount, "should have 1 message in queue")

	s.log.Logf("Land API test passed: request stored and message published")
}

// TestRequestLogConsumer verifies the gateway's log-topic consumer in isolation:
// no orchestrator runs in this stack, so the test itself publishes a request log
// entry to the log topic exactly as the orchestrator does in production (via
// submitqueue/core/request.PublishLog). The gateway is the sole writer of the
// request log; this asserts its consumer drains the log topic and persists the
// entry to storage, observable through the Status RPC.
func (s *GatewayIntegrationSuite) TestRequestLogConsumer() {
	t := s.T()

	// Build a publisher against the shared queue database. NewQueue only wires up
	// stores; nothing consumes until a subscriber is started, so this publish-only
	// use does not interfere with the gateway container's consumer.
	queue, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.queueDB,
		Logger:       zap.NewNop(),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err, "failed to create queue publisher")
	defer queue.Close()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue},
	})
	require.NoError(t, err, "failed to create topic registry")

	const sqid = "log-consumer-test/1"
	logEntry := entity.NewRequestLog(sqid, entity.RequestStatusStarted, 1, "", nil)
	require.NoError(t, corerequest.PublishLog(s.ctx, registry, logEntry, sqid),
		"failed to publish request log to log topic")

	s.log.Logf("Published 'started' log for sqid=%s; waiting for gateway consumer to persist it", sqid)

	require.Eventually(t, func() bool {
		resp, statusErr := s.client.Status(s.ctx, &pb.StatusRequest{Sqid: sqid})
		if statusErr != nil {
			return false
		}
		return resp.Status == string(entity.RequestStatusStarted)
	}, persistTimeout, persistPollInterval,
		"gateway log consumer should persist the published request log for sqid=%s", sqid)

	s.log.Logf("Request log consumer test passed: entry persisted and readable via Status")
}
