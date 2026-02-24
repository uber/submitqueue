package gateway

// Gateway Integration Tests
//
// These tests use docker-compose from example/server/gateway/docker-compose.yml
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
)

type GatewayIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.SubmitQueueGatewayClient
	db      *sql.DB  // App database
	queueDB *sql.DB  // Queue database
}

func TestGatewayIntegration(t *testing.T) {
	suite.Run(t, new(GatewayIntegrationSuite))
}

func (s *GatewayIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Gateway integration test suite using docker-compose")

	// Set REPO_ROOT for docker-compose volume mounts and build context
	repoRoot := testutil.FindRepoRoot(t)
	t.Setenv("REPO_ROOT", repoRoot)

	// Use docker-compose from example/server/gateway
	// NOTE: Assumes Linux binary is pre-built via make target
	composeFile := filepath.Join(repoRoot, "example/server/gateway/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-gateway")

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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("extension/queue/sql/schema"))

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
		Change:   &pb.Change{Source: "github", Ids: []string{"PR-123"}},
		Strategy: pb.Strategy_REBASE,
	}

	s.log.Logf("Sending Land request for queue=%s", req.Queue)
	resp, err := s.client.Land(s.ctx, req)
	require.NoError(t, err, "Land request failed")
	require.NotEmpty(t, resp.Sqid, "SQID should not be empty")

	s.log.Logf("Land request succeeded: sqid=%s", resp.Sqid)

	// Verify request stored in database
	var state string
	err = s.db.QueryRow("SELECT state FROM request WHERE id = ?", resp.Sqid).Scan(&state)
	require.NoError(t, err, "failed to query request from database")
	assert.Equal(t, "new", state, "request state should be new")

	// Verify message published to queue
	var msgCount int
	err = s.queueDB.QueryRow("SELECT COUNT(*) FROM queue_messages WHERE id = ?", resp.Sqid).Scan(&msgCount)
	require.NoError(t, err, "failed to query queue messages")
	assert.Equal(t, 1, msgCount, "should have 1 message in queue")

	s.log.Logf("Land API test passed: request stored and message published")
}
