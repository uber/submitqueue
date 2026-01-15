package integration_tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	gatewaypb "github.com/uber/submitqueue/gateway/protopb"
	orchestratorpb "github.com/uber/submitqueue/orchestrator/protopb"
	speculatorpb "github.com/uber/submitqueue/speculator/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTimeout     = 5 * time.Second
	serverReadyTimeout = 30 * time.Second
	retryInterval      = 500 * time.Millisecond
)

// TestPingForAllServices tests that all services are running and responding
// This is an end-to-end test that validates the entire system is running
func TestPingForAllServices(t *testing.T) {
	// Test Gateway
	t.Run("Gateway", func(t *testing.T) {
		addr := getEnvOrDefault("GATEWAY_ADDR", "localhost:8081")
		conn, err := waitForServer(t, addr, serverReadyTimeout)
		if err != nil {
			t.Fatalf("Gateway server not ready: %v", err)
		}
		defer conn.Close()

		client := gatewaypb.NewSubmitQueueGatewayClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		resp, err := client.Ping(ctx, &gatewaypb.PingRequest{Message: "e2e test"})
		if err != nil {
			t.Fatalf("Gateway Ping failed: %v", err)
		}
		if resp.ServiceName != "gateway" {
			t.Errorf("Expected service name 'gateway', got '%s'", resp.ServiceName)
		}
		t.Logf("Gateway is healthy: %s", resp.Message)
	})

	// Test Orchestrator
	t.Run("Orchestrator", func(t *testing.T) {
		addr := getEnvOrDefault("ORCHESTRATOR_ADDR", "localhost:8082")
		conn, err := waitForServer(t, addr, serverReadyTimeout)
		if err != nil {
			t.Fatalf("Orchestrator server not ready: %v", err)
		}
		defer conn.Close()

		client := orchestratorpb.NewSubmitQueueOrchestratorClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		resp, err := client.Ping(ctx, &orchestratorpb.PingRequest{Message: "e2e test"})
		if err != nil {
			t.Fatalf("Orchestrator Ping failed: %v", err)
		}
		if resp.ServiceName != "orchestrator" {
			t.Errorf("Expected service name 'orchestrator', got '%s'", resp.ServiceName)
		}
		t.Logf("Orchestrator is healthy: %s", resp.Message)
	})

	// Test Speculator
	t.Run("Speculator", func(t *testing.T) {
		addr := getEnvOrDefault("SPECULATOR_ADDR", "localhost:8083")
		conn, err := waitForServer(t, addr, serverReadyTimeout)
		if err != nil {
			t.Fatalf("Speculator server not ready: %v", err)
		}
		defer conn.Close()

		client := speculatorpb.NewSubmitQueueSpeculatorClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		resp, err := client.Ping(ctx, &speculatorpb.PingRequest{Message: "e2e test"})
		if err != nil {
			t.Fatalf("Speculator Ping failed: %v", err)
		}
		if resp.ServiceName != "speculator" {
			t.Errorf("Expected service name 'speculator', got '%s'", resp.ServiceName)
		}
		t.Logf("Speculator is healthy: %s", resp.Message)
	})

	t.Log("All services are healthy and responding correctly")
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
