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
// These tests use docker-compose from service/submitqueue/docker-compose.yml
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
	db                 *sql.DB                 // App database
	queueDB            *sql.DB                 // Queue database
	requestLog         storage.RequestLogStore // White-box view of the request_log status timeline (app DB)
	requestStore       storage.RequestStore    // White-box view of the internal RequestState (app DB)
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

	// Use docker-compose from service/submitqueue (full stack)
	// NOTE: Assumes Linux binaries are pre-built via make target
	composeFile := filepath.Join(repoRoot, "service/submitqueue/docker-compose.yml")
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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// White-box handles on the app DB: the request_log audit trail (ordered
	// status history) and the operating store (point-in-time RequestState).
	s.requestLog = storagemysql.NewRequestLogStore(s.db, tally.NoopScope)
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
// terminal Status, the ordered request_log status history, and the internal
// RequestState in the operating store.
//
// This also exercises the request-log ownership invariant end-to-end: the
// orchestrator only *publishes* log entries to the log topic (it never writes
// the request log itself), and the gateway's log consumer drains that topic and
// persists them. Every status below except the synchronous "accepted" reaches
// storage only via that cross-service publish→consume→persist path, so its
// presence in the timeline proves the path works.
func (s *E2EIntegrationSuite) TestLand_HappyPath_ReachesLanded() {
	sqid := s.land("e2e-test-queue", "github://uber/e2e-service/pull/123/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (happy path) succeeded: sqid=%s; waiting for landed", sqid)

	// Black-box: the customer-facing status reaches landed.
	s.awaitStatus(sqid, entity.RequestStatusLanded)

	// White-box (status history): the request_log is the only ordered trail. All
	// status entries for a request share its request_id partition on the log
	// topic (ordered delivery) and the terminal "landed" is published last, so
	// once "landed" is observed the earlier statuses are already persisted. This
	// is a tolerant ordered-subsequence match — display statuses the pipeline
	// does not emit (e.g. validating, speculating, building) are omitted.
	s.assertStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusStarted,
		entity.RequestStatusBatched,
		entity.RequestStatusScored,
		entity.RequestStatusLanded,
	)

	// White-box (internal state): the operating store's authoritative
	// RequestState settled on landed. RequestState is point-in-time, so this is a
	// terminal check, not a sequence.
	assert.Equal(s.T(), entity.RequestStateLanded, s.terminalState(sqid),
		"operating store should show request %s in terminal state landed", sqid)
}

// TestList_ReturnsFilteredPagedSummaries verifies the customer-facing List RPC
// against the full stack. Land writes the initial summary through the gateway,
// later status updates arrive through the request-log topic, and List reads the
// gateway-owned summary read model through the public RPC surface.
func (s *E2EIntegrationSuite) TestList_ReturnsFilteredPagedSummaries() {
	t := s.T()

	beforeWindow := s.land("e2e-list-queue", "github://uber/e2e-list/pull/100/abcdef0123456789abcdef0123456789abcdef00")
	startTimeMs := time.Now().UnixMilli() + 1
	windowTimer := time.NewTimer(time.Until(time.UnixMilli(startTimeMs)))
	<-windowTimer.C
	firstURI := "github://uber/e2e-list/pull/101/abcdef0123456789abcdef0123456789abcdef01"
	secondURI := "github://uber/e2e-list/pull/102/abcdef0123456789abcdef0123456789abcdef02"
	thirdURI := "github://uber/e2e-list/pull/103/abcdef0123456789abcdef0123456789abcdef03"
	otherURI := "github://uber/e2e-list-other/pull/201/abcdef0123456789abcdef0123456789abcdef03"

	first := s.land("e2e-list-queue", firstURI)
	second := s.land("e2e-list-queue", secondURI)
	third := s.land("e2e-list-queue", thirdURI)
	otherQueue := s.land("e2e-cancel-queue", otherURI)
	endTimeMs := time.Now().Add(time.Minute).UnixMilli()

	resp := s.awaitListContains(&gatewaypb.ListRequest{
		Queue:       "e2e-list-queue",
		StartTimeMs: startTimeMs,
		EndTimeMs:   endTimeMs,
		PageSize:    10,
	}, first, second, third)

	assert.NotContains(t, summarySQIDs(resp.Requests), otherQueue,
		"List should not return requests from a different queue")
	assert.NotContains(t, summarySQIDs(resp.Requests), beforeWindow,
		"List should not return requests admitted before the time window")

	bySQID := make(map[string]*gatewaypb.RequestSummary, len(resp.Requests))
	for _, summary := range resp.Requests {
		bySQID[summary.Sqid] = summary
	}

	firstSummary := bySQID[first]
	require.NotNil(t, firstSummary, "List response should include %s", first)
	assert.Equal(t, "e2e-list-queue", firstSummary.Queue)
	assert.Equal(t, []string{firstURI}, firstSummary.ChangeUris)
	assert.NotEmpty(t, firstSummary.Status)
	assert.GreaterOrEqual(t, firstSummary.StartedAtMs, startTimeMs)
	assert.Less(t, firstSummary.StartedAtMs, endTimeMs)
	assert.GreaterOrEqual(t, firstSummary.UpdatedAtMs, firstSummary.StartedAtMs)

	s.awaitStatus(first, entity.RequestStatusLanded)
	s.awaitStatus(second, entity.RequestStatusLanded)
	s.awaitStatus(third, entity.RequestStatusLanded)

	landedResp := s.awaitListContains(&gatewaypb.ListRequest{
		Queue:       "e2e-list-queue",
		StartTimeMs: startTimeMs,
		EndTimeMs:   endTimeMs,
		Statuses:    []string{string(entity.RequestStatusLanded)},
		PageSize:    10,
	}, first, second, third)
	for _, summary := range landedResp.Requests {
		if summary.Sqid != first && summary.Sqid != second && summary.Sqid != third {
			continue
		}
		assert.Equal(t, string(entity.RequestStatusLanded), summary.Status, "landed summary %s should report landed", summary.Sqid)
		assert.Greater(t, summary.CompletedAtMs, int64(0), "landed summary %s should have completion time", summary.Sqid)
	}

	var pagedSQIDs []string
	var pageToken string
	for {
		page := s.list(&gatewaypb.ListRequest{
			Queue:       "e2e-list-queue",
			StartTimeMs: startTimeMs,
			EndTimeMs:   endTimeMs,
			PageSize:    1,
			PageToken:   pageToken,
			Sort:        gatewaypb.ListSort_ADMITTED_DESC,
		})
		require.LessOrEqual(t, len(page.Requests), 1)
		if len(page.Requests) == 1 {
			pagedSQIDs = append(pagedSQIDs, page.Requests[0].Sqid)
		}
		if page.NextPageToken == "" {
			break
		}
		pageToken = page.NextPageToken
	}
	assert.ElementsMatch(t, []string{first, second, third}, pagedSQIDs)
	assert.Len(t, mapFromStrings(pagedSQIDs), len(pagedSQIDs), "descending pagination should not return duplicates")
}

// TestList_ShowsCancelIntent verifies the List summary read model observes the
// gateway-synchronous cancelling log written by Cancel. Terminal cancellation is
// intentionally not asserted here because the e2e stack does not yet have a
// deterministic pipeline pause; the terminal outcome can race with landing.
func (s *E2EIntegrationSuite) TestList_ShowsCancelIntent() {
	t := s.T()

	startTimeMs := time.Now().Add(-time.Second).UnixMilli()
	sqid := s.land("e2e-cancel-queue", "github://uber/e2e-list-cancel/pull/301/abcdef0123456789abcdef0123456789abcdef04")
	endTimeMs := time.Now().Add(time.Minute).UnixMilli()

	_, err := s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{Sqid: sqid, Reason: "e2e list cancel test"})
	require.NoError(t, err, "Cancel failed")

	resp := s.list(&gatewaypb.ListRequest{
		Queue:       "e2e-cancel-queue",
		StartTimeMs: startTimeMs,
		EndTimeMs:   endTimeMs,
		Statuses:    []string{string(entity.RequestStatusCancelling)},
		PageSize:    10,
	})
	var summary *gatewaypb.RequestSummary
	for _, candidate := range resp.Requests {
		if candidate.Sqid == sqid {
			summary = candidate
			break
		}
	}
	require.NotNil(t, summary, "List should include %s with cancelling status immediately after Cancel", sqid)
	assert.Equal(t, string(entity.RequestStatusCancelling), summary.Status)
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

	sqid := s.land("e2e-cancel-queue", "github://uber/e2e-cancel/pull/9999/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (cancel path) succeeded: sqid=%s; cancelling", sqid)

	_, err := s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{Sqid: sqid, Reason: "e2e cancel test"})
	require.NoError(t, err, "Cancel failed")

	// The gateway writes "accepted" (on Land) and "cancelling" (on Cancel)
	// synchronously to the same store, so both are present the moment Cancel
	// returns — no polling needed.
	s.assertStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusCancelling,
	)
}
