package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ComposeStack manages a docker-compose stack for testing.
type ComposeStack struct {
	composeFile string
	projectName string
	t           *testing.T
	log         *TestLogger
	ctx         context.Context
	composeCmd  []string // docker-compose command (either ["docker-compose"] or ["docker", "compose"])
}

// getDockerComposeCommand returns the docker-compose command to use.
// Tries "docker-compose" first (V1), falls back to "docker compose" (V2).
func getDockerComposeCommand() []string {
	// Try docker-compose (V1)
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return []string{"docker-compose"}
	}

	// Fall back to docker compose (V2)
	return []string{"docker", "compose"}
}

// NewComposeStack creates a new compose stack from the given docker-compose file.
// Automatically registers cleanup to tear down the stack.
// testContext should describe what's being tested (e.g., "gateway", "storage", "e2e").
func NewComposeStack(t *testing.T, log *TestLogger, ctx context.Context, composeFile, testContext string) *ComposeStack {
	t.Helper()

	// Setup Docker environment
	setupDockerEnv(t)

	// Get absolute path to compose file
	absPath, err := filepath.Abs(composeFile)
	require.NoError(t, err, "failed to get absolute path to compose file")

	// Generate meaningful project name: sq-test-{context}-{short-timestamp}
	// Results in container names like: sq-test-gateway-a1b2c3d-mysql-app-1
	timestamp := fmt.Sprintf("%x", time.Now().UnixNano()&0xFFFFFF) // Last 6 hex digits
	projectName := fmt.Sprintf("sq-test-%s-%s", testContext, timestamp)

	stack := &ComposeStack{
		composeFile: absPath,
		projectName: projectName,
		t:           t,
		log:         log,
		ctx:         ctx,
		composeCmd:  getDockerComposeCommand(),
	}

	// Register cleanup
	t.Cleanup(func() {
		// Skip cleanup if test failed (for debugging) or SKIP_CLEANUP env var is set
		if t.Failed() {
			log.Logf("Test FAILED - keeping containers for debugging")
			log.Logf("Container prefix: %s", projectName)
			log.Logf("List containers: docker ps -a | grep %s", projectName)
			log.Logf("View logs: docker logs %s-<service>-1", projectName)
			composeCmd := strings.Join(stack.composeCmd, " ")
			log.Logf("Clean up manually: %s -f %s -p %s down -v --rmi local", composeCmd, absPath, projectName)
			return
		}

		if os.Getenv("SKIP_CLEANUP") == "true" {
			log.Logf("SKIP_CLEANUP=true - keeping containers for inspection")
			log.Logf("Container prefix: %s", projectName)
			composeCmd := strings.Join(stack.composeCmd, " ")
			log.Logf("Clean up manually: %s -f %s -p %s down -v --rmi local", composeCmd, absPath, projectName)
			return
		}

		log.Logf("Tearing down compose stack")
		stack.down()
	})

	return stack
}

// Up starts all services in the compose stack.
func (s *ComposeStack) Up() error {
	s.t.Helper()
	s.log.Logf("Starting compose stack from %s", s.composeFile)

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "up", "-d", "--build")
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start compose stack: %w", err)
	}

	// Wait for services to be healthy
	s.log.Logf("Waiting for services to be healthy...")
	time.Sleep(5 * time.Second) // Simple wait for now

	s.log.Logf("Compose stack started successfully")
	return nil
}

// down stops and removes all services in the compose stack.
// Also removes locally built images to prevent accumulation.
func (s *ComposeStack) down() {
	s.log.Logf("Stopping compose stack and removing images")

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "down", "-v", "--rmi", "local")
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		s.log.Logf("Warning: failed to stop compose stack: %v", err)
	}
}

// ServicePort returns the mapped host port for a service's container port.
func (s *ComposeStack) ServicePort(serviceName string, containerPort int) (int, error) {
	s.t.Helper()

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "port", serviceName, fmt.Sprintf("%d", containerPort))
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get port for service %s: %w", serviceName, err)
	}

	// Parse output like "0.0.0.0:49153\n"
	// Strip whitespace and split on colon
	outputStr := strings.TrimSpace(string(output))

	// Find the last colon (port separator)
	colonIdx := strings.LastIndex(outputStr, ":")
	if colonIdx < 0 {
		return 0, fmt.Errorf("failed to parse port output: no colon found in %q", outputStr)
	}

	portStr := outputStr[colonIdx+1:]
	var port int
	_, err = fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		return 0, fmt.Errorf("failed to parse port number from %q: %w", portStr, err)
	}

	return port, nil
}

// ServiceHost returns the host:port address for a service.
func (s *ComposeStack) ServiceHost(serviceName string, containerPort int) (string, error) {
	s.t.Helper()

	port, err := s.ServicePort(serviceName, containerPort)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("localhost:%d", port), nil
}

// ConnectMySQLService connects to a MySQL service by name in the compose stack.
// Retries the connection and registers cleanup automatically.
func (s *ComposeStack) ConnectMySQLService(serviceName string) (*sql.DB, error) {
	s.t.Helper()

	dsn, err := s.MySQLServiceDSN(serviceName)
	if err != nil {
		return nil, err
	}

	// Retry connection a few times as MySQL might still be initializing
	var db *sql.DB
	for i := 0; i < 10; i++ {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open mysql connection: %w", err)
		}

		if err = db.Ping(); err == nil {
			break
		}

		db.Close()
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s mysql after retries: %w", serviceName, err)
	}

	port, _ := s.ServicePort(serviceName, 3306) // We already got the port successfully
	s.log.Logf("Connected to %s MySQL at localhost:%d", serviceName, port)

	// Register cleanup
	s.t.Cleanup(func() {
		s.log.Logf("Closing %s MySQL connection", serviceName)
		db.Close()
	})

	return db, nil
}

// MySQLServiceDSN returns the DSN string for a MySQL service.
// Useful when the implementation manages its own database connection.
func (s *ComposeStack) MySQLServiceDSN(serviceName string) (string, error) {
	s.t.Helper()

	port, err := s.ServicePort(serviceName, 3306)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("root:root@tcp(localhost:%d)/submitqueue?parseTime=true", port), nil
}

// ConnectGRPC creates a gRPC client connection to a service.
func (s *ComposeStack) ConnectGRPC(serviceName string, containerPort int) (*grpc.ClientConn, error) {
	s.t.Helper()

	addr, err := s.ServiceHost(serviceName, containerPort)
	if err != nil {
		return nil, err
	}

	// Retry connection a few times as service might still be starting
	var conn *grpc.ClientConn
	for i := 0; i < 10; i++ {
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s after retries: %w", serviceName, err)
	}

	s.log.Logf("Connected to %s at %s", serviceName, addr)

	// Register cleanup
	s.t.Cleanup(func() {
		s.log.Logf("Closing gRPC connection to %s", serviceName)
		conn.Close()
	})

	return conn, nil
}

// setupDockerEnv configures Docker environment for docker-compose.
func setupDockerEnv(t *testing.T) {
	t.Helper()

	// Ensure HOME is set for Docker config
	if os.Getenv("HOME") == "" {
		t.Setenv("HOME", t.TempDir())
	}
}

// FindRepoRoot finds the repository root.
// Checks REPO_ROOT env var, then git, then walks up to find marker files.
func FindRepoRoot(t *testing.T) string {
	t.Helper()

	// Check if REPO_ROOT is set (from .envrc or test environment)
	if repoRoot := os.Getenv("REPO_ROOT"); repoRoot != "" {
		return repoRoot
	}

	// Try git (works outside Bazel sandbox)
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	if output, err := cmd.Output(); err == nil {
		if repoRoot := strings.TrimSpace(string(output)); repoRoot != "" {
			return repoRoot
		}
	}

	// Walk up from current directory to find marker files
	// In Bazel sandbox, marker files are symlinks - resolve them to get source location
	dir, err := os.Getwd()
	require.NoError(t, err, "failed to get working directory")

	for {
		// Try to find and resolve marker file symlinks
		for _, marker := range []string{"MODULE.bazel", "go.mod"} {
			markerPath := filepath.Join(dir, marker)
			if realMarker, err := filepath.EvalSymlinks(markerPath); err == nil {
				return filepath.Dir(realMarker)
			}
		}

		// Move up one directory
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repository root not found")
		}
		dir = parent
	}
}
