package integration_tests

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	gatewaypb "github.com/uber/submitqueue/gateway/protopb"
	orchestratorpb "github.com/uber/submitqueue/orchestrator/protopb"
	speculatorpb "github.com/uber/submitqueue/speculator/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IntegrationSuite struct {
	suite.Suite
	log *testLogger

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
	s.log = newTestLogger(t)

	// Disable Ryuk reaper container which may not work in Docker-in-Docker environments.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	// Ensure HOME is set for Docker config resolution in Bazel sandbox.
	if os.Getenv("HOME") == "" {
		t.Setenv("HOME", t.TempDir())
	}

	// Create Docker network for inter-container communication.
	nw, err := network.New(ctx)
	require.NoError(t, err, "failed to create Docker network")
	s.nw = nw
	t.Cleanup(func() {
		s.log.logf("Removing Docker network")
		require.NoError(t, nw.Remove(ctx), "failed to remove Docker network")
	})
	s.log.logf("Docker network created: %s", nw.Name)

	// Start MySQL container on the network and apply schema.
	setupMySQL(t, s.log, s.nw)

	// Start all server containers.
	gatewayAddr := startGatewayContainer(ctx, t, s.log, s.nw)
	orchestratorAddr := startOrchestratorContainer(ctx, t, s.log, s.nw)
	speculatorAddr := startSpeculatorContainer(ctx, t, s.log, s.nw)

	// Create gRPC client connections.
	opts := grpc.WithTransportCredentials(insecure.NewCredentials())
	s.gatewayClient = gatewaypb.NewSubmitQueueGatewayClient(s.dial(gatewayAddr, opts))
	s.orchestratorClient = orchestratorpb.NewSubmitQueueOrchestratorClient(s.dial(orchestratorAddr, opts))
	s.speculatorClient = speculatorpb.NewSubmitQueueSpeculatorClient(s.dial(speculatorAddr, opts))

	s.log.logf("All containers started and clients connected")
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
	s.log.logf("Gateway ping: %s", resp.Message)
}

func (s *IntegrationSuite) TestPingOrchestrator() {
	ctx := context.Background()
	resp, err := s.orchestratorClient.Ping(ctx, &orchestratorpb.PingRequest{Message: "integration test"})
	require.NoError(s.T(), err, "Orchestrator Ping failed")
	assert.Equal(s.T(), "orchestrator", resp.ServiceName)
	s.log.logf("Orchestrator ping: %s", resp.Message)
}

func (s *IntegrationSuite) TestPingSpeculator() {
	ctx := context.Background()
	resp, err := s.speculatorClient.Ping(ctx, &speculatorpb.PingRequest{Message: "integration test"})
	require.NoError(s.T(), err, "Speculator Ping failed")
	assert.Equal(s.T(), "speculator", resp.ServiceName)
	s.log.logf("Speculator ping: %s", resp.Message)
}

func (s *IntegrationSuite) TestLandRequest() {
	ctx := context.Background()
	req := &gatewaypb.LandRequest{
		Queue:    "integration-test-queue",
		Change:   &gatewaypb.Change{Source: "github", Ids: []string{"pr-100", "pr-101"}},
		Strategy: gatewaypb.Strategy_STRATEGY_REBASE,
	}

	s.log.logf("Sending Land request for queue=%s", req.Queue)
	resp, err := s.gatewayClient.Land(ctx, req)
	require.NoError(s.T(), err, "Land request failed")
	require.NotEmpty(s.T(), resp.Sqid, "SQID should not be empty")
	s.log.logf("Land request succeeded: sqid=%s", resp.Sqid)
}
