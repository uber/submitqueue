package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/orchestrator/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTimeout     = 5 * time.Second
	serverReadyTimeout = 30 * time.Second
	retryInterval      = 500 * time.Millisecond
)

// TestPingAPI tests the Orchestrator service Ping API
func TestPingAPI(t *testing.T) {
	addr := getEnvOrDefault("ORCHESTRATOR_ADDR", "localhost:8082")

	// Wait for server to be ready
	conn, err := waitForServer(t, addr, serverReadyTimeout)
	require.NoError(t, err, "Orchestrator server not ready")
	defer conn.Close()

	client := pb.NewSubmitQueueOrchestratorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Test Ping API
	req := &pb.PingRequest{
		Message: "integration test",
	}

	resp, err := client.Ping(ctx, req)
	require.NoError(t, err, "Ping failed")

	// Validate response
	assert.NotEmpty(t, resp.Message, "Response message should not be empty")
	assert.Equal(t, "orchestrator", resp.ServiceName)
	assert.NotZero(t, resp.Timestamp, "Timestamp should not be zero")
	assert.NotEmpty(t, resp.Hostname, "Hostname should not be empty")

	t.Logf("Orchestrator Ping test passed:")
	t.Logf("  Message: %s", resp.Message)
	t.Logf("  Service: %s", resp.ServiceName)
	t.Logf("  Timestamp: %d", resp.Timestamp)
	t.Logf("  Hostname: %s", resp.Hostname)
}

// waitForServer waits for a gRPC server to become ready
func waitForServer(t *testing.T, addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(
			ctx,
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()

		if err == nil {
			t.Logf("Server at %s is ready", addr)
			return conn, nil
		}

		lastErr = err
		time.Sleep(retryInterval)
	}

	return nil, fmt.Errorf("server at %s not ready after %v: %w", addr, timeout, lastErr)
}

// getEnvOrDefault returns the value of an environment variable or a default value
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
