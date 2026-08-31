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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	orchestratorpb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
	consumergatefile "github.com/uber/submitqueue/platform/extension/consumergate/file"
	"github.com/uber/submitqueue/submitqueue/entity"
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
	appStorage         *storagemysql.Storage   // White-box view of the operating store (app DB), resolved per queue
	gate               *consumergatefile.Store // Consumer-gate control plane (shared dir bind-mounted into services)
}

func TestE2EIntegration(t *testing.T) {
	suite.Run(t, new(E2EIntegrationSuite))
}

// The gateway log consumer runs inside the gateway-service container, so there
// is no in-process signal to wait on across the container boundary.
// persistPollInterval bounds how often helpers re-query; Bazel's test timeout is
// the only convergence deadline.
const persistPollInterval = 500 * time.Millisecond

func (s *E2EIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting E2E integration test suite using docker-compose")

	// Application services write parked records into a host bind mount. On a
	// rootful daemon they must run as the host test user so those records remain
	// readable and removable by the test. On a rootless daemon, container root
	// already maps to the host user, so keep the container user at 0:0.
	containerUser := dockerContainerUser(t)
	t.Setenv("SQ_CONTAINER_USER", containerUser)
	s.log.Logf("Application containers will run as %s", containerUser)

	// Consumer-gate state is an explicit E2E-only opt-in. The compose file
	// bind-mounts this test-owned directory into every application service, and
	// the suite manipulates the same directory through the file implementation.
	gateDir := t.TempDir()
	t.Setenv("SQ_CONSUMER_GATE_DIR", gateDir)
	s.gate = consumergatefile.New(gateDir)

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
	s.appStorage, err = storagemysql.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err, "failed to create app storage backend")

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

// dockerContainerUser returns the UID:GID that application containers should
// use for host-bind-mounted test artifacts. Rootless Docker maps container root
// to the host user; rootful Docker needs the host UID:GID explicitly.
func dockerContainerUser(t *testing.T) string {
	t.Helper()

	cmd := exec.Command("docker", "info", "--format", "{{json .SecurityOptions}}")
	output, err := cmd.Output()
	require.NoError(t, err, "failed to inspect Docker security options")
	if strings.Contains(string(output), "name=rootless") {
		return "0:0"
	}
	return fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
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
	req := s.land("e2e-test-queue", "github://github.example.com/uber/e2e-service/pull/123/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (happy path) succeeded: sqid=%s; waiting for landed", req.sqid)

	// Black-box: the customer-facing status reaches landed.
	seen := s.awaitStatus(req, entity.RequestStatusLanded)

	// Black-box history: all status entries for a request share its request_id
	// partition on the log topic, and the terminal "landed" is published last.
	// Once "landed" is observed, GetRequestHistoryByID must expose the earlier statuses.
	// This is a tolerant ordered-subsequence match because the pipeline does not
	// emit every possible display status.
	s.assertStatusesInOrder(req,
		entity.RequestStatusAccepted,
		entity.RequestStatusStarted,
		entity.RequestStatusValidating,
		entity.RequestStatusBatched,
		entity.RequestStatusSpeculating,
		entity.RequestStatusSpeculated,
		entity.RequestStatusLanding,
		entity.RequestStatusLanded,
	)

	// The two tiers, observed from outside. Build progress shares the stream but
	// is not in the status trail above, and the summary never sat on it: a
	// request with several paths building at once is speculating, not built.
	assert.Subset(s.T(), s.eventTimeline(req),
		[]entity.RequestEvent{entity.RequestEventBuilding, entity.RequestEventBuilt},
		"GetRequestHistoryByID for %s should record the build that verified it", req.sqid)

	// The types make this unrepresentable in code, so the check is against the
	// raw strings and covers the rest of the path — serialize, persist, project,
	// read back — where a type cannot follow. Sampling a poll cannot prove a
	// value never appeared, so it corroborates the materializer's unit test
	// rather than replacing it.
	for _, status := range seen {
		assert.NotContainsf(s.T(),
			[]entity.RequestStatus{entity.RequestStatus(entity.RequestEventBuilding), entity.RequestStatus(entity.RequestEventBuilt)},
			status,
			"GetRequestSummaryByID for %s reported build progress %q as its current status; observed %v",
			req.sqid, status, seen)
	}

	// White-box (internal state): the operating store's authoritative
	// RequestState settled on landed. RequestState is point-in-time, so this is a
	// terminal check, not a sequence.
	assert.Equal(s.T(), entity.RequestStateLanded, s.terminalState(req),
		"operating store should show request %s in terminal state landed", req.sqid)
}

// TestDependentBatch_BypassesMergingDependency proves that a dependency still
// waiting on Runway is unresolved for strict merge but can be bypassed once
// passed paths cover both of its possible outcomes.
func (s *E2EIntegrationSuite) TestDependentBatch_BypassesMergingDependency() {
	t := s.T()

	const queue = "e2e-chain-queue"
	const gateGroup = "runway-merge"
	gateTopic := runwaymq.TopicKeyMerge.String()

	s.closeGate(gateGroup, queue, "e2e: hold the lead merge so the dependent finishes building first")
	// Reopen even if an assertion below fails, so teardown does not stop the
	// stack with a delivery still parked. Opening twice is a no-op.
	defer s.openGate(gateGroup, queue)

	lead := s.land(queue, "github://github.example.com/uber/e2e-chain/pull/1/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Landed lead request %s; awaiting its merge to park", lead.sqid)

	// The merge request is keyed by batch, so name the batch to prove the
	// parked delivery is this request's merge and not some other.
	leadBatch := s.awaitBatchID(lead)
	parked := s.awaitParked(gateGroup, gateTopic, leadBatch)
	assert.Equal(t, queue, parked.PartitionKey, "merge request should be partitioned by queue")

	// The lead is provably stopped mid-merge. A request landed now serializes
	// behind it.
	dependent := s.land(queue, "github://github.example.com/uber/e2e-chain/pull/2/1234567890abcdef1234567890abcdef12345678")
	dependentBatch := s.awaitBatchID(dependent)
	require.NotEqual(t, leadBatch, dependentBatch, "the two requests must be carried by different batches")

	leadState, err := s.appStorage.For(queue)
	require.NoError(t, err)
	got, err := leadState.GetBatchStore().Get(s.ctx, dependentBatch)
	require.NoError(t, err, "failed to read the dependent batch")
	require.Contains(t, got.Dependencies, leadBatch,
		"batch %s must depend on the in-flight %s for this test to exercise anything", dependentBatch, leadBatch)

	// Both paths pass while the lead is still parked. A Merging dependency is
	// unresolved because its merge can fail, so the durable Merging state proves
	// the dependent advanced through complete coverage rather than strict merge.
	s.awaitBatchState(queue, dependentBatch, entity.BatchStateMerging)
	s.log.Logf("Dependent %s bypassed merging batch %s", dependent.sqid, leadBatch)

	// Start: the lead merges, and its fan-out is now the only thing that can
	// move the dependent.
	s.openGate(gateGroup, queue)
	s.awaitUnparked(gateGroup, gateTopic, leadBatch)

	s.awaitStatus(lead, entity.RequestStatusLanded)
	s.awaitStatus(dependent, entity.RequestStatusLanded)

	assert.Equal(t, entity.RequestStateLanded, s.terminalState(dependent))
}

// TestDependentBatch_BypassedHeadLandsFirst proves the bypass does not just
// dispatch a merge: the dependent lands while its dependency is still held
// mid-build, and only then does the dependency proceed.
func (s *E2EIntegrationSuite) TestDependentBatch_BypassedHeadLandsFirst() {
	t := s.T()

	const queue = "e2e-chain-queue"
	const gateGroup = "orchestrator"

	lead := s.land(queue, "github://github.example.com/uber/e2e-bypass/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	heldBatch := s.awaitBatchID(lead)
	s.closeGate(gateGroup, heldBatch, "e2e: hold the leader's build so the follower can fully cover it")
	defer s.openGate(gateGroup, heldBatch)
	s.awaitBatchState(queue, heldBatch, entity.BatchStateSpeculating)

	follower := s.land(queue, "github://github.example.com/uber/e2e-bypass/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	followerBatch := s.awaitBatchID(follower)
	require.NotEqual(t, heldBatch, followerBatch)

	// The follower builds with and without the held leader while the leader's
	// own build is parked; complete coverage hands it to the merge stage.
	s.awaitBatchState(queue, followerBatch, entity.BatchStateMerging)
	s.awaitStatus(follower, entity.RequestStatusLanded)
	assert.Equal(t, entity.RequestStatusSpeculating, s.mustStatus(lead),
		"the follower must land while its dependency is still held")

	s.openGate(gateGroup, heldBatch)
	s.awaitStatus(lead, entity.RequestStatusLanded)

	s.assertStatusesInOrder(follower,
		entity.RequestStatusSpeculating,
		entity.RequestStatusSpeculated,
		entity.RequestStatusLanding,
		entity.RequestStatusLanded,
	)
}

// TestDependentBatch_NoBypassWhenCoverageIsIncomplete proves the complement:
// with only the "dependency succeeds" side passed, a head waits for its
// dependency to resolve and merges strictly, never dispatching ahead of it.
//
// Partial coverage is seeded directly: the follower's batch is stranded in
// Created (build held), its "succeeds" path is written as passed, and a
// trigger request wakes the queue. The follower's real speculative builds are
// parked on its held batch partition, so the funded set stays exactly one
// path. The single passed path is a live passed path, so the run reports the
// wait — the signal this test then uses to prove nothing else advanced it.
func (s *E2EIntegrationSuite) TestDependentBatch_NoBypassWhenCoverageIsIncomplete() {
	t := s.T()

	const queue = "e2e-chain-queue"
	const gateGroup = "orchestrator"

	lead := s.land(queue, "github://github.example.com/uber/e2e-nobypass/pull/1/cccccccccccccccccccccccccccccccccccccccc")
	leadBatch := s.awaitBatchID(lead)
	s.awaitBatchState(queue, leadBatch, entity.BatchStateSpeculating)

	follower := s.land(queue, "github://github.example.com/uber/e2e-nobypass/pull/2/dddddddddddddddddddddddddddddddddddddddd")
	heldBatch := s.awaitBatchID(follower)
	s.closeGate(gateGroup, heldBatch, "e2e: hold the follower's builds so only the seeded path exists")
	defer s.openGate(gateGroup, heldBatch)

	// Strand the follower in Created, then seed exactly one passed path: the
	// guess that the lead succeeds. Its builds are parked, so nothing can add
	// the "fails" path while the queue is quiet.
	s.strandInCreated(queue, heldBatch)
	s.seedPassedPath(queue, entity.SpeculationPath{
		Head: heldBatch,
		Dependencies: []entity.PathDependency{
			{Batch: leadBatch, Assumption: entity.DependencyAssumptionSucceeds},
		},
	})

	trigger := s.land(queue, "github://github.example.com/uber/e2e-nobypass/pull/3/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	s.awaitBatchID(trigger)

	// The run that admits the follower also reports the wait: its one passed
	// path covers only one of the lead's two outcomes. The follower's batch
	// must still be Speculating — incomplete coverage must never hand it to
	// the merge stage ahead of the lead.
	s.awaitEvent(follower, entity.RequestEventWaiting)
	assert.Equal(t, entity.BatchStateSpeculating, s.batchState(queue, heldBatch),
		"incomplete coverage must not move the batch to merging")

	// Let the queue finish. The follower's own speculative builds were parked
	// on the held partition; releasing it lets the surviving path complete and
	// land after the lead. How it ultimately converges is exercised by the
	// other tests; this one exists to prove the wait, not the landing.
	s.openGate(gateGroup, heldBatch)
	s.awaitStatus(lead, entity.RequestStatusLanded)
	s.awaitStatus(trigger, entity.RequestStatusLanded)
}

// TestReadAPIs validates all five request read endpoints against receipts
// created through the public Land API.
func (s *E2EIntegrationSuite) TestReadAPIs() {
	t := s.T()
	const (
		queue     = "e2e-test-queue"
		changeURI = "github://uber/e2e-read-apis/pull/456/abcdef0123456789abcdef0123456789abcdef01"
	)
	first := s.land(queue, changeURI)
	second := s.land(queue, changeURI)
	firstSqid, secondSqid := first.sqid, second.sqid

	s.awaitStatus(first, entity.RequestStatusLanded)
	s.awaitStatus(second, entity.RequestStatusError)

	firstSummary, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: firstSqid, Queue: queue})
	require.NoError(t, err)
	require.NotNil(t, firstSummary.Request)
	assert.Equal(t, firstSqid, firstSummary.Request.Sqid)
	assert.Equal(t, queue, firstSummary.Request.Queue)
	assert.Equal(t, []string{changeURI}, firstSummary.Request.ChangeUris)

	secondSummary, err := s.gatewayClient.GetRequestSummaryByID(s.ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: secondSqid, Queue: queue})
	require.NoError(t, err)
	require.NotNil(t, secondSummary.Request)
	assert.Contains(t, secondSummary.Request.LastError, firstSqid)

	summariesByChange, err := s.gatewayClient.GetRequestSummaryByChangeURI(s.ctx, &gatewaypb.GetRequestSummaryByChangeURIRequest{ChangeUri: changeURI, Queue: queue})
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

	historyByID, err := s.gatewayClient.GetRequestHistoryByID(s.ctx, &gatewaypb.GetRequestHistoryByIDRequest{Sqid: firstSqid, Queue: queue})
	require.NoError(t, err)
	require.NotEmpty(t, historyByID.Events)
	assert.Equal(t, string(entity.RequestStatusAccepted), historyByID.Events[0].Status)

	historyByChange, err := s.gatewayClient.GetRequestHistoryByChangeURI(s.ctx, &gatewaypb.GetRequestHistoryByChangeURIRequest{ChangeUri: changeURI, Queue: queue})
	require.NoError(t, err)
	require.Len(t, historyByChange.Histories, 2)
	assert.Equal(t, []string{firstSqid, secondSqid}, []string{historyByChange.Histories[0].Sqid, historyByChange.Histories[1].Sqid})
	require.NotEmpty(t, historyByChange.Histories[0].Events)
	require.NotEmpty(t, historyByChange.Histories[1].Events)
	secondEvents := historyByChange.Histories[1].Events
	assert.Equal(t, string(entity.RequestStatusError), secondEvents[len(secondEvents)-1].Status)
	assert.Equal(t, secondSummary.Request.LastError, secondEvents[len(secondEvents)-1].LastError)
}

// TestLand_DependentBatch_BypassesAnUnresolvedDependency proves the complete
// payoff: a follower built both with and without a held leader lands before the
// leader resolves.
//
// The wait is forced rather than raced. Batch IDs come from a per-queue counter
// as "<queue>/batch/<n>", so the leader on a fresh queue is batch/1, and the
// build topic partitions by batch — closing the gate on that partition before
// anything is published holds the leader's build and nothing else, so the
// follower reaches a passed path while its dependency is still outstanding.
func (s *E2EIntegrationSuite) TestLand_DependentBatch_BypassesAnUnresolvedDependency() {
	const queue = "e2e-respeculate-queue"
	const gateGroup = "orchestrator"
	leaderBatch := queue + "/batch/1"

	s.closeGate(gateGroup, leaderBatch, "e2e: hold the leader's build so its dependent speculates first")
	defer s.openGate(gateGroup, leaderBatch)

	leader := s.land(queue, "github://github.example.com/uber/e2e-respeculate/pull/1/1111111111111111111111111111111111111111?sq-fake=build-fail")
	follower := s.land(queue, "github://github.example.com/uber/e2e-respeculate/pull/2/2222222222222222222222222222222222222222")
	s.log.Logf("Landed leader=%s (build held) follower=%s", leader.sqid, follower.sqid)

	// The baseline analyzer serializes the queue, and the build budget lets the
	// follower validate both possible outcomes while the leader is held.
	s.awaitStatus(follower, entity.RequestStatusLanded)
	assert.Equal(s.T(), entity.RequestStatusSpeculating, s.mustStatus(leader),
		"the follower must land before the held leader resolves")

	// Release the leader only after the follower has landed. Its later failure is
	// one of the outcomes the follower already validated.
	s.openGate(gateGroup, leaderBatch)
	assert.Equal(s.T(), entity.RequestStatusError, s.awaitTerminal(leader),
		"the leader's build carries a failure marker, so it must not land")

	s.assertStatusesInOrder(follower,
		entity.RequestStatusSpeculating,
		entity.RequestStatusSpeculated,
		entity.RequestStatusLanding,
		entity.RequestStatusLanded,
	)

	// The point of the exercise: one trip through speculation, however many
	// guesses it took. A second entry renders as the pipeline going backwards.
	s.assertStatusCount(follower, entity.RequestStatusSpeculating, 1)
	s.assertStatusCount(follower, entity.RequestStatusSpeculated, 1)
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

// TestCancel_CaughtPreBatch_NeverLands drives the deterministic cancel
// scenario from doc/rfc/consumer-gate.md as stop → observe → start: the
// consumer gate stops runway's merge-conflict-check controller before the
// request's check message can be answered, so the request is provably held
// pre-batch while the cancel lands. The change must never reach the repo.
//
//  1. Stop: close the gate for runway-mergeconflictcheck, scoped to this
//     queue's partition, before landing — exact by construction, no timing.
//  2. Land: the orchestrator runs the request to the merge-conflict-check
//     hand-off; runway's subscriber delivers the check and the gate parks it.
//  3. Observe: awaiting the parked record proves the controller is stopped and
//     holding exactly this request's check (there is otherwise no signal
//     distinguishing "gated and parked" from "not arrived yet").
//  4. Act while stopped: cancel the request. It is pre-batch by construction,
//     so the cancel controller drives it terminal Cancelled directly.
//  5. Start: open the gate. The postponed check redelivers within a re-check
//     tick and clears the open gate, runway answers the now-stale check, and
//     the orchestrator drops the signal for the halted request.
//
// The drop in step 5 is asserted without sleeping: a sentinel request landed
// on the same queue after the gate opens shares the check and signal
// partitions with the stale message, so the sentinel reaching "landed" proves
// the stale signal was already consumed — at which point the cancelled
// request must still be terminal Cancelled, never batched, never landed.
func (s *E2EIntegrationSuite) TestCancel_CaughtPreBatch_NeverLands() {
	t := s.T()

	const queue = "e2e-cancel-queue"
	const gateGroup = "runway-mergeconflictcheck"
	gateTopic := runwaymq.TopicKeyMergeConflictCheck.String()

	s.closeGate(gateGroup, queue, "e2e: hold merge-conflict check to catch cancel pre-batch")
	// Reopen even if an assertion below fails, so teardown does not stop the
	// stack with a delivery still parked. Opening twice is a no-op.
	defer s.openGate(gateGroup, queue)

	req := s.land(queue, "github://github.example.com/uber/e2e-cancel/pull/9999/abcdef0123456789abcdef0123456789abcdef01")
	s.log.Logf("Land (cancel path) succeeded: sqid=%s; awaiting parked check", req.sqid)

	parked := s.awaitParked(gateGroup, gateTopic, req.sqid)
	assert.Equal(t, queue, parked.PartitionKey, "check message should be partitioned by queue")
	assert.NotEmpty(t, parked.Payload, "parked record should carry the check payload")

	// The controller is provably stopped and holding this request's check;
	// cancel now. The request cannot be batched until the check is answered,
	// so the cancel controller takes the not-batched path to terminal
	// Cancelled.
	_, err := s.gatewayClient.Cancel(s.ctx, &gatewaypb.CancelRequest{Sqid: req.sqid, Queue: queue, Reason: "e2e cancel test"})
	require.NoError(t, err, "Cancel failed")

	s.awaitStatus(req, entity.RequestStatusCancelled)
	s.assertStatusesInOrder(req,
		entity.RequestStatusAccepted,
		entity.RequestStatusCancelling,
		entity.RequestStatusCancelled,
	)
	assert.Equal(t, entity.RequestStateCancelled, s.terminalState(req),
		"operating store should show request %s terminal cancelled while its check is parked", req.sqid)

	// Start the controller again and prove the parked delivery cleared the gate.
	s.openGate(gateGroup, queue)
	s.awaitUnparked(gateGroup, gateTopic, req.sqid)

	// Sentinel on the same queue: its landing proves the stale signal ahead of
	// it on the same partitions was consumed.
	sentinel := s.land(queue, "github://github.example.com/uber/e2e-cancel/pull/10000/1234567890abcdef1234567890abcdef12345678")
	s.awaitStatus(sentinel, entity.RequestStatusLanded)

	// The stale check answer was dropped: the cancelled request never advanced.
	assert.Equal(t, entity.RequestStateCancelled, s.terminalState(req),
		"request %s must stay terminal cancelled after its stale check signal is processed", req.sqid)
	s.assertStatusesNever(req, entity.RequestStatusBatched, entity.RequestStatusLanded)
}

// A batch delivery that is retried after its ack was lost must not enrol the
// request into a second batch. Both batches would be analyzed, promoted and
// admitted, and both would merge the same change.
//
// The build gate is what makes the redelivery land in the window that matters:
// with the build for the first batch held, the request cannot reach a terminal
// state, so the retry finds it exactly as a lost ack would — claimed, carried
// by a live batch, and still in flight.
//
// The second request is the settle signal. The batch topic is partitioned by
// queue and consumed in order, so its batch existing proves the redelivered
// message ahead of it has already been handled, and the association count is
// safe to assert.
func (s *E2EIntegrationSuite) TestBatchRedelivery_DoesNotEnrolTheRequestTwice() {
	t := s.T()

	const queue = "e2e-redelivery-queue"
	const gateGroup = "orchestrator"
	// Nothing has landed on this queue, so the first batch is predictable, and
	// the build topic partitions by batch ID.
	const heldBatch = queue + "/batch/1"

	s.closeGate(gateGroup, heldBatch, "e2e: hold the build so the request stays in flight for the redelivery")
	// Reopen even if an assertion below fails, so teardown does not stop the
	// stack with a delivery still parked. Opening twice is a no-op.
	defer s.openGate(gateGroup, heldBatch)

	req := s.land(queue, "github://github.example.com/uber/e2e-redelivery/pull/1/abcdef0123456789abcdef0123456789abcdef01")
	require.Equal(t, heldBatch, s.awaitBatchID(req), "the first batch of a fresh queue must be batch/1")
	s.awaitStatus(req, entity.RequestStatusSpeculating)

	s.redeliverBatchMessage(req)

	settle := s.land(queue, "github://github.example.com/uber/e2e-redelivery/pull/2/1234567890abcdef1234567890abcdef12345678")
	settleBatch := s.awaitBatchID(settle)
	require.NotEqual(t, heldBatch, settleBatch)

	assert.Equal(t, []string{heldBatch}, s.batchIDsFor(req),
		"the redelivery must resume the existing batch, not mint another")

	s.openGate(gateGroup, heldBatch)
	s.awaitStatus(req, entity.RequestStatusLanded)
	s.awaitStatus(settle, entity.RequestStatusLanded)
}

// A batch left in Created is dependency-eligible, so everything created after
// it serializes behind it — and nothing will name it on the speculate topic
// again. Without a run that looks for such batches the queue wedges here with
// no way out, which is how CODEM-444 stalled a 100-PR run.
//
// The strand is built from a real batch rather than seeded rows so the request,
// its association and its path set are all genuine: the only thing missing is
// the announcement, which is exactly what the bug destroyed.
//
// Against a pipeline without the repair this test does not fail fast — the
// batch simply never moves and the suite runs to Bazel's timeout, which is how
// this harness reports a stall.
func (s *E2EIntegrationSuite) TestStrandedBatch_IsAdmittedByALaterRun() {
	t := s.T()

	const queue = "e2e-strand-queue"
	const gateGroup = "orchestrator"
	const heldBatch = queue + "/batch/1"

	s.closeGate(gateGroup, heldBatch, "e2e: hold the build so the batch can be stranded while still in flight")
	defer s.openGate(gateGroup, heldBatch)

	req := s.land(queue, "github://github.example.com/uber/e2e-strand/pull/1/abcdef0123456789abcdef0123456789abcdef01")
	require.Equal(t, heldBatch, s.awaitBatchID(req), "the first batch of a fresh queue must be batch/1")
	s.awaitStatus(req, entity.RequestStatusSpeculating)

	// Its build is parked, so the queue is quiet and this write cannot race.
	s.strandInCreated(queue, heldBatch)
	require.Equal(t, entity.BatchStateCreated, s.batchState(queue, heldBatch))

	// Any later request wakes the queue; the run it triggers is what has to
	// notice the stranded batch.
	trigger := s.land(queue, "github://github.example.com/uber/e2e-strand/pull/2/1234567890abcdef1234567890abcdef12345678")
	s.awaitBatchID(trigger)

	s.awaitBatchState(queue, heldBatch, entity.BatchStateSpeculating)

	s.openGate(gateGroup, heldBatch)
	s.awaitStatus(req, entity.RequestStatusLanded)
	s.awaitStatus(trigger, entity.RequestStatusLanded)
}
