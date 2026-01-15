package gateway_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/uber/submitqueue/gateway/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTimeout     = 5 * time.Second
	serverReadyTimeout = 30 * time.Second
	retryInterval      = 500 * time.Millisecond
)

// TestPingAPI tests the Gateway service Ping API
func TestPingAPI(t *testing.T) {
	addr := getEnvOrDefault("GATEWAY_ADDR", "localhost:8081")

	// Wait for server to be ready
	conn, err := waitForServer(t, addr, serverReadyTimeout)
	if err != nil {
		t.Fatalf("Gateway server not ready: %v", err)
	}
	defer conn.Close()

	client := pb.NewSubmitQueueGatewayClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	// Test Ping API
	req := &pb.PingRequest{
		Message: "integration test",
	}

	resp, err := client.Ping(ctx, req)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Validate response
	if resp.Message == "" {
		t.Error("Response message is empty")
	}
	if resp.ServiceName != "gateway" {
		t.Errorf("Expected service name 'gateway', got '%s'", resp.ServiceName)
	}
	if resp.Timestamp == 0 {
		t.Error("Timestamp is zero")
	}
	if resp.Hostname == "" {
		t.Error("Hostname is empty")
	}

	t.Logf("Gateway Ping test passed:")
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
