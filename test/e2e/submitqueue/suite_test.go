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
// (Gateway + Orchestrator + Runway + 2 MySQL DBs) which requires pre-built
// Linux binaries. All three services start in SetupSuite and stop only in
// TearDownSuite. Tests that need to park the pipeline at a stage boundary use
// StageHold (test/testutil/stagehold.go) to starve individual queue consumer
// controllers without touching the services.
//
// Run with make target (builds binaries + runs test):
//   make e2e-test
//
// Waiting discipline: tests block on events the pipeline pushes onto its own
// queue topics (see observer_test.go) — the log topic for status transitions,
// runway's signal topics for check/merge answers. There are no per-wait timeout
// constants; every wait is bounded by the suite context, whose deadline derives
// from the test binary's own deadline (which Bazel sets from the test target
// timeout) minus a teardown margin. Where a plane is eventually consistent with
// the event plane (the Status RPC and the request_log, both fed by the gateway
// log consumer), helpers poll at a fixed cadence under the same context.

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
	runwaypb "github.com/uber/submitqueue/api/runway/protopb"
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
	ctxCancel          context.CancelFunc
	log                *testutil.TestLogger
	stack              *testutil.ComposeStack
	gatewayClient      gatewaypb.SubmitQueueGatewayClient
	orchestratorClient orchestratorpb.SubmitQueueOrchestratorClient
	runwayClient       runwaypb.RunwayClient
	db                 *sql.DB                 // App database
	queueDB            *sql.DB                 // Queue database
	requestLog         storage.RequestLogStore // White-box view of the request_log status timeline (app DB)
	requestStore       storage.RequestStore    // White-box view of the internal RequestState (app DB)
	batchStore         storage.BatchStore      // White-box view of batch enrollment (app DB)
	observer           *queueObserver          // Push-based event plane (test-owned consumer groups on mysql-queue)
}

func TestE2EIntegration(t *testing.T) {
	suite.Run(t, new(E2EIntegrationSuite))
}

// maxTeardownMargin caps how much of the test deadline is reserved for
// teardown (graceful service stops, compose down, diagnostics).
const maxTeardownMargin = 90 * time.Second

// stopTimeoutSec bounds a graceful service stop (SIGTERM → SIGKILL). It must
// exceed the services' 30s consumer drain window.
const stopTimeoutSec = 60

// suiteContext derives the context that bounds every wait in the suite from
// the test binary's own deadline (go test -timeout; under Bazel, the test
// target's timeout), reserving a slice for teardown. With no deadline set the
// context is unbounded and eventually() falls back to fallbackWaitBudget.
func suiteContext(t *testing.T) (context.Context, context.CancelFunc) {
	deadline, ok := t.Deadline()
	if !ok {
		return context.WithCancel(context.Background())
	}
	margin := time.Until(deadline) / 10
	if margin > maxTeardownMargin {
		margin = maxTeardownMargin
	}
	return context.WithDeadline(context.Background(), deadline.Add(-margin))
}

func (s *E2EIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx, s.ctxCancel = suiteContext(t)
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting E2E integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from service/submitqueue (full stack)
	// NOTE: Assumes Linux binaries are pre-built via make target
	//
	// The stack gets a background context, not the deadline-bounded suite
	// context: compose lifecycle commands (stop, down, diagnostics) must keep
	// working during teardown even when the suite's wait budget has expired.
	// The Bazel test timeout is the hard bound on the whole process.
	composeFile := filepath.Join(repoRoot, "service/submitqueue/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, context.Background(), composeFile, "e2e-submitqueue")

	// Start the compose stack (Gateway + Orchestrator + Runway + 2 MySQL DBs)
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
	// status history), the operating store (point-in-time RequestState), and
	// the batch store (batch enrollment).
	s.requestLog = storagemysql.NewRequestLogStore(s.db, tally.NoopScope)
	s.requestStore = storagemysql.NewRequestStore(s.db, tally.NoopScope)
	s.batchStore = storagemysql.NewBatchStore(s.db, tally.NoopScope)

	// Event plane: subscribe test-owned consumer groups to the pipeline's own
	// topics so the services push their progress to the test (observer_test.go).
	// Must start after the queue schema is applied.
	s.observer, err = startQueueObserver(t, s.log, s.ctx, s.queueDB)
	require.NoError(t, err, "failed to start queue observer")

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

	// Connect to Runway gRPC service — the landing service must be up for any
	// request to progress past validation (it answers the merge-conflict-check
	// and merge queues). All three services start here and stop only in
	// TearDownSuite; individual tests use StageHold to starve controllers
	// without touching the services.
	var runwayConn *grpc.ClientConn
	runwayConn, err = s.stack.ConnectGRPC("runway-service", 8080)
	require.NoError(t, err, "failed to connect to runway")
	s.runwayClient = runwaypb.NewRunwayClient(runwayConn)

	s.log.Logf("E2E integration test suite ready")
}

func (s *E2EIntegrationSuite) TearDownSuite() {
	t := s.T()
	s.log.Logf("Tearing down E2E integration test suite")

	// Stop the observer first: it consumes from the same queue database the
	// services drain on shutdown, and it must not outlive the suite context.
	if s.observer != nil {
		if err := s.observer.stop(5000); err != nil {
			s.log.Logf("Warning: failed to stop queue observer: %v", err)
		}
	}

	// Gracefully stop services via SIGTERM and verify exit codes before compose teardown.
	// Stop all services first so their shutdown runs in parallel, then check exit codes.
	const wantExitCode = 143 // 128 + SIGTERM (15)

	services := []string{"gateway-service", "orchestrator-service", "runway-service"}
	stopErrs := make(map[string]error, len(services))
	for _, svc := range services {
		stopErrs[svc] = s.stack.StopService(svc, stopTimeoutSec)
	}

	for _, svc := range services {
		if assert.NoErrorf(t, stopErrs[svc], "failed to stop %s", svc) {
			exitCode, err := s.stack.ServiceExitCode(svc)
			if assert.NoErrorf(t, err, "failed to get %s exit code", svc) {
				assert.Equalf(t, wantExitCode, exitCode,
					"%s should exit with 128+SIGTERM (%d) on graceful shutdown", svc, wantExitCode)
			}
		}
	}

	if s.ctxCancel != nil {
		s.ctxCancel()
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

func (s *E2EIntegrationSuite) TestPingRunway() {
	resp, err := s.runwayClient.Ping(s.ctx, &runwaypb.PingRequest{Message: "e2e test"})
	require.NoError(s.T(), err, "Runway Ping failed")
	assert.Equal(s.T(), "runway", resp.ServiceName)
	s.log.Logf("Runway ping: %s", resp.Message)
}

// TestLand_HappyPath_ReachesLanded drives a single request through the whole
// pipeline to terminal success on the fully-hermetic e2e-test-queue (no
// conflicts, fake build succeeds, noop runway merger answers SUCCEEDED for both
// the merge-conflict check and the merge — the merge outcome is faked at the
// same fidelity as the build outcome).
//
// It asserts four views: runway's answers on its signal topics (proof the
// landing service participated), the pushed "landed" status event, the internal
// RequestState in the operating store, and the customer-facing Status RPC plus
// the ordered request_log history.
//
// This also exercises the request-log ownership invariant end-to-end: the
// orchestrator only *publishes* log entries to the log topic (it never writes
// the request log itself), and the gateway's log consumer drains that topic and
// persists them. Every status below except the synchronous "accepted" reaches
// storage only via that cross-service publish→consume→persist path, so its
// presence in the timeline proves the path works.
func (s *E2EIntegrationSuite) TestLand_HappyPath_ReachesLanded() {
	sqid := s.land("e2e-test-queue", "github://github.example.com/uber/e2e-service/pull/123/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (happy path) succeeded: sqid=%s; awaiting pipeline events", sqid)

	// Runway round trips, observed on the wire: the orchestrator handed the
	// check to runway, and runway answered both the dry-run check and the
	// committing merge with SUCCEEDED.
	checkSignal := s.awaitCheckSignal(sqid)
	s.assertOutcomeSucceeded(checkSignal, "runway merge-conflict-check signal")
	mergeSignal := s.awaitMergeSignal(sqid)
	s.assertOutcomeSucceeded(mergeSignal, "runway merge signal")

	// Event plane: the pipeline published the terminal "landed" status.
	s.awaitStatusEvent(sqid, entity.RequestStatusLanded)

	// White-box (internal state): the operating store's authoritative
	// RequestState settled on landed. The orchestrator CAS-writes state before
	// publishing the log event, so this read is deterministic after the await.
	assert.Equal(s.T(), entity.RequestStateLanded, s.terminalState(sqid),
		"operating store should show request %s in terminal state landed", sqid)

	// Black-box: the customer-facing status converges to landed once the
	// gateway's log consumer persists the entry the observer already saw.
	s.awaitStatusRPC(sqid, entity.RequestStatusLanded)

	// White-box (status history): the request_log is the only ordered trail.
	// This is a tolerant ordered-subsequence match — display statuses the
	// pipeline does not emit (e.g. validating, speculating, building) are
	// omitted.
	s.awaitStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusStarted,
		entity.RequestStatusBatched,
		entity.RequestStatusScored,
		entity.RequestStatusLanded,
	)
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

// TestCancel_BeforeBatch_ReachesCancelled drives a cancellation to its terminal
// "cancelled" state deterministically. Cancellation is best-effort and normally
// races the pipeline (the hermetic happy path lands in ~2s), so the test parks
// the pipeline first: a StageHold on runway's merge-conflict-check consumer
// starves it for the test's queue partition, preventing it from processing the
// check request. A request advances exactly to the merge-conflict-check
// hand-off and can go no further — the batch stage is unreachable until
// runway's signal arrives. The park point is confirmed by a pushed event
// (validate's check request observed on the runway queue), not by sleeping.
// Runway itself keeps running throughout; only the single partition is starved.
//
// The cancel then wins with certainty: the cancel controller takes its
// request-only fast path (Cancelling → Cancelled) because no batch ever
// enrolled the request. After the terminal event, the hold is released and the
// test verifies the parked merge-conflict check is answered by runway and its
// stale signal does not resurrect the request — cancelled is terminal under the
// CAS state machine, and the mergeconflictsignal controller skips halted
// requests.
func (s *E2EIntegrationSuite) TestCancel_BeforeBatch_ReachesCancelled() {
	t := s.T()
	const queue = "e2e-cancel-queue"

	// Park the pipeline at the runway boundary by holding runway's
	// merge-conflict-check consumer for this queue's partition. The hold must
	// be planted BEFORE the first message (Land publishes the start message
	// which eventually fans out to the merge-conflict-check topic partitioned
	// by queue name). t.Cleanup releases the hold automatically if Release()
	// is not called explicitly below.
	hold := s.holdStage(stageRunwayMergeConflictCheck, queue)

	sqid := s.land(queue, "github://github.example.com/uber/e2e-cancel/pull/9999/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (cancel path) succeeded: sqid=%s; awaiting park at runway boundary", sqid)

	// The orchestrator has started the request and handed its merge-conflict
	// check to runway. Runway's consumer is starved for this partition, so the
	// check request is parked on the wire: no signal can arrive, hence no
	// batch can enroll the request.
	s.awaitCheckRequested(sqid)

	s.cancel(sqid, "e2e cancel test")

	// The intent entry is written synchronously by the gateway before Cancel
	// returns, right after Land's "accepted" — both are readable immediately.
	s.awaitStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusCancelling,
	)

	// Terminal outcome, pushed: the cancel controller's request-only fast path
	// published "cancelled" on the log topic. The state CAS precedes the
	// publish, so the store reads below are deterministic.
	s.awaitStatusEvent(sqid, entity.RequestStatusCancelled)
	assert.Equal(t, entity.RequestStateCancelled, s.terminalState(sqid),
		"operating store should show request %s in terminal state cancelled", sqid)
	s.assertNoBatchContains(queue, sqid)

	// Release the hold: runway's next discovery tick re-acquires the partition
	// and processes the parked check request. Its (now stale) signal is
	// observed on the wire; the mergeconflictsignal controller must drop it
	// for the halted request.
	hold.Release()
	s.awaitCheckSignal(sqid)

	// Cancelled is terminal: the late signal must not move the request. The
	// state machine's CAS transitions make regression impossible by
	// construction; these reads pin the observable outcome.
	assert.Equal(t, entity.RequestStateCancelled, s.terminalState(sqid),
		"request %s must remain cancelled after the stale runway signal", sqid)
	s.assertNoBatchContains(queue, sqid)

	// Black-box view converges to cancelled, and the completed timeline shows
	// the full intent→terminal order with no batch enrollment ever logged.
	s.awaitStatusRPC(sqid, entity.RequestStatusCancelled)
	s.awaitStatusesInOrder(sqid,
		entity.RequestStatusAccepted,
		entity.RequestStatusStarted,
		entity.RequestStatusCancelling,
		entity.RequestStatusCancelled,
	)
	s.assertStatusNeverLogged(sqid, entity.RequestStatusBatched)
}
