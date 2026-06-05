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

// E2E Integration Tests
//
// These tests use docker-compose from example/submitqueue/docker-compose.yml
// which requires pre-built Linux binaries.
//
// Run with make target (builds binaries + runs test):
//   make e2e-test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/submitqueue/submitqueue/entity"
	gatewaypb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	orchestratorpb "github.com/uber/submitqueue/submitqueue/orchestrator/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type E2EIntegrationSuite struct {
	suite.Suite
	ctx                context.Context
	log                *testutil.TestLogger
	stack              *testutil.ComposeStack
	gatewayClient      gatewaypb.SubmitQueueGatewayClient
	orchestratorClient orchestratorpb.SubmitQueueOrchestratorClient
	db                 *sql.DB // App database
	queueDB            *sql.DB // Queue database
}

func TestE2EIntegration(t *testing.T) {
	suite.Run(t, new(E2EIntegrationSuite))
}

// The gateway log consumer runs inside the gateway-service container, so this
// suite can only observe persistence black-box through the Status RPC — there is
// no in-process channel/HookSignal to wait on across the container boundary. A
// bounded poll is therefore the deterministic-enough analog: persistTimeout is a
// safety net (a failure here means something is genuinely stuck, not a timing
// race), and persistPollInterval bounds how often we re-query.
const (
	persistTimeout      = 30 * time.Second
	persistPollInterval = 500 * time.Millisecond
)

func (s *E2EIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting E2E integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from example/submitqueue (full stack)
	// NOTE: Assumes Linux binaries are pre-built via make target
	composeFile := filepath.Join(repoRoot, "example/submitqueue/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-submitqueue")

	// Start the compose stack (Gateway + Orchestrator + 2 MySQL DBs)
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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("extension/messagequeue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// Connect to Gateway gRPC service
	var gatewayConn *grpc.ClientConn
	gatewayConn, err = s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err, "failed to connect to gateway")
	s.gatewayClient = gatewaypb.NewSubmitQueueGatewayClient(gatewayConn)

	// Connect to Orchestrator gRPC service
	var orchestratorConn *grpc.ClientConn
	orchestratorConn, err = s.stack.ConnectGRPC("orchestrator-service", 8080)
	require.NoError(t, err, "failed to connect to orchestrator")
	s.orchestratorClient = orchestratorpb.NewSubmitQueueOrchestratorClient(orchestratorConn)

	s.log.Logf("E2E integration test suite ready")
}

func (s *E2EIntegrationSuite) TearDownSuite() {
	t := s.T()
	s.log.Logf("Tearing down E2E integration test suite")

	// Gracefully stop services via SIGTERM and verify exit codes before compose teardown.
	// Use a 60s timeout to exceed the orchestrator's 30s consumer drain window.
	// Stop both services first so their shutdown runs in parallel, then check exit codes.
	const stopTimeoutSec = 60
	const wantExitCode = 143 // 128 + SIGTERM (15)

	gatewayStopErr := s.stack.StopService("gateway-service", stopTimeoutSec)
	orchestratorStopErr := s.stack.StopService("orchestrator-service", stopTimeoutSec)

	if assert.NoError(t, gatewayStopErr, "failed to stop gateway service") {
		exitCode, err := s.stack.ServiceExitCode("gateway-service")
		if assert.NoError(t, err, "failed to get gateway exit code") {
			assert.Equal(t, wantExitCode, exitCode,
				"gateway should exit with 128+SIGTERM (%d) on graceful shutdown", wantExitCode)
		}
	}

	if assert.NoError(t, orchestratorStopErr, "failed to stop orchestrator service") {
		exitCode, err := s.stack.ServiceExitCode("orchestrator-service")
		if assert.NoError(t, err, "failed to get orchestrator exit code") {
			assert.Equal(t, wantExitCode, exitCode,
				"orchestrator should exit with 128+SIGTERM (%d) on graceful shutdown", wantExitCode)
		}
	}

	// Compose stack cleanup handled automatically by t.Cleanup
}

func (s *E2EIntegrationSuite) TestPingGateway() {
	resp, err := s.gatewayClient.Ping(s.ctx, &gatewaypb.PingRequest{Message: "e2e test"})
	require.NoError(s.T(), err, "Gateway Ping failed")
	assert.Equal(s.T(), "gateway", resp.ServiceName)
	s.log.Logf("Gateway ping: %s", resp.Message)
}

func (s *E2EIntegrationSuite) TestPingOrchestrator() {
	resp, err := s.orchestratorClient.Ping(s.ctx, &orchestratorpb.PingRequest{Message: "e2e test"})
	require.NoError(s.T(), err, "Orchestrator Ping failed")
	assert.Equal(s.T(), "orchestrator", resp.ServiceName)
	s.log.Logf("Orchestrator ping: %s", resp.Message)
}

func (s *E2EIntegrationSuite) TestLandRequest_SinglePR() {
	req := &gatewaypb.LandRequest{
		Queue:    "e2e-test-queue",
		Change:   &gatewaypb.Change{Uris: []string{"github://uber/e2e-service/pull/123/abcdef0123456789abcdef0123456789abcdef01"}},
		Strategy: gatewaypb.Strategy_REBASE,
	}

	s.log.Logf("Sending Land request (single PR) for queue=%s", req.Queue)
	resp, err := s.gatewayClient.Land(s.ctx, req)
	require.NoError(s.T(), err, "Land request failed")
	require.NotEmpty(s.T(), resp.Sqid, "SQID should not be empty")
	s.log.Logf("Land request (single PR) succeeded: sqid=%s", resp.Sqid)
}

// TestLandRequest_PersistsStartedLogViaGatewayConsumer verifies the request-log
// ownership invariant end-to-end: the orchestrator only *publishes* request log
// entries to the log topic (it never writes the request log itself), and the
// gateway's log consumer drains that topic and persists them to storage.
//
// We observe this through the gateway Status RPC: immediately after Land the
// status is "accepted" (the gateway's synchronous direct write), and once the
// orchestrator's start controller publishes "started" to the log topic, the
// gateway consumer persists it and Status advances to "started". Seeing
// "started" therefore proves the publish→consume→persist path works across both
// services.
func (s *E2EIntegrationSuite) TestLandRequest_PersistsStartedLogViaGatewayConsumer() {
	t := s.T()

	landResp, err := s.gatewayClient.Land(s.ctx, &gatewaypb.LandRequest{
		Queue:    "e2e-test-queue",
		Change:   &gatewaypb.Change{Uris: []string{"github://uber/e2e-startlog/pull/4242/abcdef0123456789abcdef0123456789abcdef01"}},
		Strategy: gatewaypb.Strategy_REBASE,
	})
	require.NoError(t, err, "Land request failed")
	require.NotEmpty(t, landResp.Sqid, "SQID should not be empty")
	sqid := landResp.Sqid
	s.log.Logf("Land succeeded: sqid=%s; waiting for gateway consumer to persist 'started'", sqid)

	require.Eventually(t, func() bool {
		resp, statusErr := s.gatewayClient.Status(s.ctx, &gatewaypb.StatusRequest{Sqid: sqid})
		if statusErr != nil {
			s.log.Logf("Status(%s) not ready yet: %v", sqid, statusErr)
			return false
		}
		s.log.Logf("Status(%s) = %q", sqid, resp.Status)
		return resp.Status == string(entity.RequestStatusStarted)
	}, persistTimeout, persistPollInterval,
		"request %s should reach status %q via the gateway log consumer", sqid, entity.RequestStatusStarted)

	s.log.Logf("Gateway consumer persisted orchestrator-published 'started' log for sqid=%s", sqid)
}

// TestCancelRequest_InvalidSqid verifies the gateway rejects an empty sqid
// synchronously before publishing anything to the cancel queue.
func (s *E2EIntegrationSuite) TestCancelRequest_InvalidSqid() {
	_, err := s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{Sqid: ""})
	require.Error(s.T(), err, "Cancel with empty sqid should fail")

	st, ok := status.FromError(err)
	require.True(s.T(), ok, "expected a gRPC status error")
	assert.Equal(s.T(), codes.InvalidArgument, st.Code(),
		"empty sqid should map to InvalidArgument; got %s", st.Code())
}

// TestCancelRequest_BeforeBatch is intentionally a thin smoke test of the
// Land + Cancel RPC envelope: Land to mint a sqid, then Cancel that sqid, and
// assert both calls return OK. It does not poll for terminal state, log
// entries, or any orchestrator-side progression.
//
// TODO(e2e): harden this test once the e2e fixture story is in better shape.
// Add async assertions that the request reaches RequestStateCancelled, that
// request_log contains both `cancelling` and `cancelled` entries, and that a
// second Cancel is idempotent.
func (s *E2EIntegrationSuite) TestCancelRequest_BeforeBatch() {
	t := s.T()

	landReq := &gatewaypb.LandRequest{
		Queue:    "e2e-cancel-queue",
		Change:   &gatewaypb.Change{Uris: []string{"github://uber/e2e-nonexistent/pull/9999/deadbeef"}},
		Strategy: gatewaypb.Strategy_REBASE,
	}
	landResp, err := s.gatewayClient.Land(s.ctx, landReq)
	require.NoError(t, err, "Land failed")
	require.NotEmpty(t, landResp.Sqid)

	_, err = s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{
		Sqid:   landResp.Sqid,
		Reason: "e2e cancel smoke test",
	})
	require.NoError(t, err, "Cancel failed")
}
