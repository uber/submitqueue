package integration_tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	gatewaypb "github.com/uber/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/integration_tests/testutil"
	orchestratorpb "github.com/uber/submitqueue/orchestrator/protopb"
	speculatorpb "github.com/uber/submitqueue/speculator/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IntegrationSuite struct {
	suite.Suite
	log *testutil.TestLogger

	nw *testcontainers.DockerNetwork

	gatewayClient      gatewaypb.SubmitQueueGatewayClient
	orchestratorClient orchestratorpb.SubmitQueueOrchestratorClient
	speculatorClient   speculatorpb.SubmitQueueSpeculatorClient

	cleanups []func()
}

func TestIntegration(t *testing.T) {
	suite.Run(t, new(IntegrationSuite))
}

func (s *IntegrationSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	// Setup Docker environment and network
	s.nw, ctx = testutil.SetupDockerEnv(t, s.log, ctx)

	// Start MySQL container on the network and apply schemas.
	mysqlContainer, db, _ := testutil.SetupMySQL(t, s.log, s.nw, "extensions/storage/mysql/schema")
	testutil.ApplySchema(t, s.log, db, testutil.SchemaDir("extensions/counter/mysql/schema"))

	// Register MySQL cleanup
	s.addCleanup(func() {
		s.log.Logf("Closing MySQL connection")
		if err := db.Close(); err != nil {
			s.log.Logf("Failed to close MySQL connection: %v", err)
		}
		s.log.Logf("Terminating MySQL container")
		if err := mysqlContainer.Terminate(context.Background()); err != nil {
			s.log.Logf("Failed to terminate MySQL container: %v", err)
		}
		s.log.Logf("MySQL container terminated")
	})

	// Start all server containers.
	gatewayAddr := startGatewayContainer(ctx, t, s.log, s.nw)
	orchestratorAddr := startOrchestratorContainer(ctx, t, s.log, s.nw)
	speculatorAddr := startSpeculatorContainer(ctx, t, s.log, s.nw)

	// Create gRPC client connections.
	opts := grpc.WithTransportCredentials(insecure.NewCredentials())
	s.gatewayClient = gatewaypb.NewSubmitQueueGatewayClient(s.dial(gatewayAddr, opts))
	s.orchestratorClient = orchestratorpb.NewSubmitQueueOrchestratorClient(s.dial(orchestratorAddr, opts))
	s.speculatorClient = speculatorpb.NewSubmitQueueSpeculatorClient(s.dial(speculatorAddr, opts))

	s.log.Logf("All containers started and clients connected")
}

func (s *IntegrationSuite) TearDownSuite() {
	for i := len(s.cleanups) - 1; i >= 0; i-- {
		s.cleanups[i]()
	}
}

func (s *IntegrationSuite) addCleanup(fn func()) {
	s.cleanups = append(s.cleanups, fn)
}

func (s *IntegrationSuite) dial(addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(s.T(), err, "failed to connect to %s", addr)
	s.addCleanup(func() { conn.Close() })
	return conn
}

func (s *IntegrationSuite) TestPingGateway() {
	ctx := context.Background()
	resp, err := s.gatewayClient.Ping(ctx, &gatewaypb.PingRequest{Message: "integration test"})
	require.NoError(s.T(), err, "Gateway Ping failed")
	assert.Equal(s.T(), "gateway", resp.ServiceName)
	s.log.Logf("Gateway ping: %s", resp.Message)
}

func (s *IntegrationSuite) TestPingOrchestrator() {
	ctx := context.Background()
	resp, err := s.orchestratorClient.Ping(ctx, &orchestratorpb.PingRequest{Message: "integration test"})
	require.NoError(s.T(), err, "Orchestrator Ping failed")
	assert.Equal(s.T(), "orchestrator", resp.ServiceName)
	s.log.Logf("Orchestrator ping: %s", resp.Message)
}

func (s *IntegrationSuite) TestPingSpeculator() {
	ctx := context.Background()
	resp, err := s.speculatorClient.Ping(ctx, &speculatorpb.PingRequest{Message: "integration test"})
	require.NoError(s.T(), err, "Speculator Ping failed")
	assert.Equal(s.T(), "speculator", resp.ServiceName)
	s.log.Logf("Speculator ping: %s", resp.Message)
}

func (s *IntegrationSuite) TestLandRequest() {
	ctx := context.Background()
	req := &gatewaypb.LandRequest{
		Queue:    "integration-test-queue",
		Change:   &gatewaypb.Change{Source: "github", Ids: []string{"pr-100", "pr-101"}},
		Strategy: gatewaypb.Strategy_REBASE,
	}

	s.log.Logf("Sending Land request for queue=%s", req.Queue)
	resp, err := s.gatewayClient.Land(ctx, req)
	require.NoError(s.T(), err, "Land request failed")
	require.NotEmpty(s.T(), resp.Sqid, "SQID should not be empty")
	s.log.Logf("Land request succeeded: sqid=%s", resp.Sqid)
}
