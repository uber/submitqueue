package e2e_test

// E2E Integration Tests
//
// These tests use docker-compose from example/server/docker-compose.yml
// which requires pre-built Linux binaries.
//
// Run with make target (builds binaries + runs test):
//   make e2e-test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	gatewaypb "github.com/uber/submitqueue/gateway/protopb"
	orchestratorpb "github.com/uber/submitqueue/orchestrator/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

type E2EIntegrationSuite struct {
	suite.Suite
	ctx                context.Context
	log                *testutil.TestLogger
	stack              *testutil.ComposeStack
	gatewayClient      gatewaypb.SubmitQueueGatewayClient
	orchestratorClient orchestratorpb.SubmitQueueOrchestratorClient
	db                 *sql.DB  // App database
	queueDB            *sql.DB  // Queue database
}

func TestE2EIntegration(t *testing.T) {
	suite.Run(t, new(E2EIntegrationSuite))
}

func (s *E2EIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting E2E integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from example/server (full stack)
	// NOTE: Assumes Linux binaries are pre-built via make target
	composeFile := filepath.Join(repoRoot, "example/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "e2e")

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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("extension/queue/mysql/schema"))

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
	s.log.Logf("Tearing down E2E integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
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

func (s *E2EIntegrationSuite) TestLandRequest() {
	req := &gatewaypb.LandRequest{
		Queue:    "e2e-test-queue",
		Change:   &gatewaypb.Change{Source: "github", Uris: []string{"github.com/uber/e2e-service/100/aaa100bbb", "github.com/uber/e2e-service/101/ccc101ddd"}},
		Strategy: gatewaypb.Strategy_REBASE,
	}

	s.log.Logf("Sending Land request for queue=%s", req.Queue)
	resp, err := s.gatewayClient.Land(s.ctx, req)
	require.NoError(s.T(), err, "Land request failed")
	require.NotEmpty(s.T(), resp.Sqid, "SQID should not be empty")
	s.log.Logf("Land request succeeded: sqid=%s", resp.Sqid)
}
