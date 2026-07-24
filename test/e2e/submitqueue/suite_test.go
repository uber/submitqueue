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
// These tests use docker-compose from service/submitqueue/docker-compose.yml.
// They are hermetic: the service images are built from a staged context whose
// inputs (Bazel-built Linux binaries, Dockerfiles, queues.yaml) are all
// declared data dependencies of the test target.
//
// Run with:
//   make e2e-test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	orchestratorpb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemysql "github.com/uber/submitqueue/submitqueue/extension/storage/mysql"
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
	db                 *sql.DB              // App database
	queueDB            *sql.DB              // Queue database
	requestStore       storage.RequestStore // White-box view of the internal RequestState (app DB)
}

func TestE2EIntegration(t *testing.T) {
	suite.Run(t, new(E2EIntegrationSuite))
}

// The gateway log consumer runs inside the gateway-service container, so there
// is no in-process signal to wait on across the container boundary. A bounded
// GetRequestSummaryByID poll is therefore the deterministic-enough analog: persistTimeout
// is a safety net, and persistPollInterval bounds how often we re-query.
const (
	persistTimeout      = 30 * time.Second
	persistPollInterval = 500 * time.Millisecond
)

func (s *E2EIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting E2E integration test suite using docker-compose")

	// Use docker-compose from service/submitqueue (full stack), resolved from
	// the test runfiles. All three service images are built from a staged
	// build context assembled entirely from declared data dependencies.
	composeFile := testutil.Runfile("service/submitqueue/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e-submitqueue",
		testutil.WithBuildContext(map[string]string{
			".docker-bin/gateway":                                "service/submitqueue/gateway/server/gateway_linux",
			".docker-bin/orchestrator":                           "service/submitqueue/orchestrator/server/orchestrator_linux",
			".docker-bin/runway":                                 "service/runway/server/runway_linux",
			"service/submitqueue/gateway/server/Dockerfile":      "service/submitqueue/gateway/server/Dockerfile",
			"service/submitqueue/gateway/server/queues.yaml":     "service/submitqueue/gateway/server/queues.yaml",
			"service/submitqueue/orchestrator/server/Dockerfile": "service/submitqueue/orchestrator/server/Dockerfile",
			"service/runway/server/Dockerfile":                   "service/runway/server/Dockerfile",
		}))

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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// White-box handle on the operating store for point-in-time RequestState.
	s.requestStore = storagemysql.NewRequestStore(s.db, tally.NoopScope)

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

// TestLand_HappyPath_ReachesLanded drives a single request through the whole
// pipeline to terminal success on the fully-hermetic e2e-test-queue (no
// conflicts, fake build succeeds, noop runway signals SUCCEEDED for both the
// merge-conflict check and the merge). It asserts three views: the black-box
// terminal request summary, the public GetRequestHistoryByID timeline, and the internal RequestState
// in the operating store.
//
// This also exercises the request-log ownership invariant end-to-end: the
// orchestrator only *publishes* log entries to the log topic (it never writes
// the request log itself), and the gateway's log consumer drains that topic and
// persists them. Every status below except the synchronous "accepted" reaches
// storage only via that cross-service publish→consume→persist path, so its
// presence in GetRequestHistoryByID proves the path works.
func (s *E2EIntegrationSuite) TestLand_HappyPath_ReachesLanded() {
	sqid := s.land("e2e-test-queue", "github://github.example.com/uber/e2e-service/pull/123/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (happy path) succeeded: sqid=%s; waiting for landed", sqid)

	// Black-box: the customer-facing status reaches landed.
	s.awaitStatus(sqid, entity.RequestStatusLanded)

	// Black-box history: all status entries for a request share its request_id
	// partition on the log topic, and the terminal "landed" is published last.
	// Once "landed" is observed, GetRequestHistoryByID must expose the earlier statuses.
	// This is a tolerant ordered-subsequence match because the pipeline does not
	// emit every possible display status.
	s.assertStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusStarted,
		entity.RequestStatusBatched,
		entity.RequestStatusLanded,
	)

	// White-box (internal state): the operating store's authoritative
	// RequestState settled on landed. RequestState is point-in-time, so this is a
	// terminal check, not a sequence.
	assert.Equal(s.T(), entity.RequestStateLanded, s.terminalState(sqid),
		"operating store should show request %s in terminal state landed", sqid)
}

// TestReadAPIs validates all five request read endpoints against receipts
// created through the public Land API.
func (s *E2EIntegrationSuite) TestReadAPIs() {
	t := s.T()
	const (
		queue     = "e2e-test-queue"
		changeURI = "github://uber/e2e-read-apis/pull/456/abcdef0123456789abcdef0123456789abcdef01"
	)
	firstSqid := s.land(queue, changeURI)
	secondSqid := s.land(queue, changeURI)
	s.awaitStatus(firstSqid, entity.RequestStatusLanded)
	s.awaitStatus(secondSqid, entity.RequestStatusError)

	firstSummary, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: firstSqid})
	require.NoError(t, err)
	require.NotNil(t, firstSummary.Request)
	assert.Equal(t, firstSqid, firstSummary.Request.Sqid)
	assert.Equal(t, queue, firstSummary.Request.Queue)
	assert.Equal(t, []string{changeURI}, firstSummary.Request.ChangeUris)

	secondSummary, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: secondSqid})
	require.NoError(t, err)
	require.NotNil(t, secondSummary.Request)
	assert.Contains(t, secondSummary.Request.LastError, firstSqid)

	summariesByChange, err := s.gatewayClient.GetRequestSummaryByChangeURI(s.ctx, &gatewaypb.GetRequestSummaryByChangeURIRequest{ChangeUri: changeURI})
	require.NoError(t, err)
	require.Len(t, summariesByChange.Requests, 2)
	expectedNewestFirst := []string{firstSqid, secondSqid}
	if secondSummary.Request.ReceivedAtMs > firstSummary.Request.ReceivedAtMs ||
		(secondSummary.Request.ReceivedAtMs == firstSummary.Request.ReceivedAtMs && secondSqid > firstSqid) {
		expectedNewestFirst[0], expectedNewestFirst[1] = expectedNewestFirst[1], expectedNewestFirst[0]
	}
	assert.Equal(t, expectedNewestFirst, []string{summariesByChange.Requests[0].Sqid, summariesByChange.Requests[1].Sqid})

	receivedAtOrAfterMs := min(firstSummary.Request.ReceivedAtMs, secondSummary.Request.ReceivedAtMs)
	receivedBeforeMs := max(firstSummary.Request.ReceivedAtMs, secondSummary.Request.ReceivedAtMs) + 1
	var listedSqids []string
	var pageToken string
	for {
		listResponse, listErr := s.gatewayClient.List(s.ctx, &gatewaypb.ListRequest{
			Queue:               queue,
			ReceivedAtOrAfterMs: receivedAtOrAfterMs,
			ReceivedBeforeMs:    receivedBeforeMs,
			PageSize:            1,
			PageToken:           pageToken,
		})
		require.NoError(t, listErr)
		for _, request := range listResponse.Requests {
			if request.Sqid == firstSqid || request.Sqid == secondSqid {
				listedSqids = append(listedSqids, request.Sqid)
			}
		}
		pageToken = listResponse.NextPageToken
		if pageToken == "" {
			break
		}
	}
	assert.Equal(t, expectedNewestFirst, listedSqids)

	historyByID, err := s.gatewayClient.GetRequestHistoryByID(s.ctx, &gatewaypb.GetRequestHistoryByIDRequest{Sqid: firstSqid})
	require.NoError(t, err)
	require.NotEmpty(t, historyByID.Events)
	assert.Equal(t, string(entity.RequestStatusAccepted), historyByID.Events[0].Status)

	historyByChange, err := s.gatewayClient.GetRequestHistoryByChangeURI(s.ctx, &gatewaypb.GetRequestHistoryByChangeURIRequest{ChangeUri: changeURI})
	require.NoError(t, err)
	require.Len(t, historyByChange.Histories, 2)
	assert.Equal(t, []string{firstSqid, secondSqid}, []string{historyByChange.Histories[0].Sqid, historyByChange.Histories[1].Sqid})
	require.NotEmpty(t, historyByChange.Histories[0].Events)
	require.NotEmpty(t, historyByChange.Histories[1].Events)
	secondEvents := historyByChange.Histories[1].Events
	assert.Equal(t, string(entity.RequestStatusError), secondEvents[len(secondEvents)-1].Status)
	assert.Equal(t, secondSummary.Request.LastError, secondEvents[len(secondEvents)-1].LastError)
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

// TestCancel_RecordsIntent verifies the deterministic half of the cancel flow:
// Cancel returns OK and the gateway synchronously records a "cancelling" intent
// entry in the request_log (written directly to the app DB before the RPC
// returns, right after the Land "accepted" entry).
//
// It deliberately does NOT assert the terminal "cancelled" outcome. Cancellation
// is best-effort and races the pipeline: on the hermetic stack the happy path
// reaches "landed" in ~2s, and a cancel published before the orchestrator's
// start controller has created the request is rejected to the DLQ and reconciled
// to "error". Asserting a terminal "cancelled" deterministically needs a
// pipeline-pause lever (e.g. a runway "park" marker that withholds the
// merge-conflict-check signal so the request is caught pre-batch) — that is the
// next incremental, per-stage addition on top of this harness.
func (s *E2EIntegrationSuite) TestCancel_RecordsIntent() {
	t := s.T()

	sqid := s.land("e2e-cancel-queue", "github://github.example.com/uber/e2e-cancel/pull/9999/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (cancel path) succeeded: sqid=%s; cancelling", sqid)

	_, err := s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{Sqid: sqid, Reason: "e2e cancel test"})
	require.NoError(t, err, "Cancel failed")

	// The gateway writes "accepted" on Land and "cancelling" on Cancel
	// synchronously, so GetRequestHistoryByID exposes both when Cancel returns.
	s.assertStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusCancelling,
	)
}
