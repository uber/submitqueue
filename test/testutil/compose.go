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

package testutil

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"os/user"
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
	composeCmd  []string  // docker-compose command (either ["docker-compose"] or ["docker", "compose"])
	composeEnv  []string  // environment shared by compose commands
	logCmd      *exec.Cmd // background "docker compose logs -f" process
}

// getDockerComposeCommand returns the docker compose command to use.
// Prefers the Compose V2 plugin ("docker compose") and falls back to a
// standalone docker-compose binary only when the plugin is unavailable: a
// standalone binary is often the legacy Python V1, which lacks flags the
// stack relies on (e.g. `up --wait`).
func getDockerComposeCommand() []string {
	if exec.Command("docker", "compose", "version").Run() == nil {
		return []string{"docker", "compose"}
	}

	return []string{"docker-compose"}
}

// ComposeOption customizes how a ComposeStack is created.
type ComposeOption func(*composeOptions)

type composeOptions struct {
	// buildContextFiles maps a path inside the synthesized docker build
	// context to the workspace-relative runfile it is staged from.
	buildContextFiles map[string]string
}

// WithBuildContext stages the given files into a synthesized docker build
// context directory and points the compose build context variable (REPO_ROOT)
// at it. Keys are context-relative destination paths that must match the
// Dockerfile COPY sources and the compose `dockerfile:` path (e.g.
// ".docker-bin/gateway", "service/submitqueue/gateway/server/Dockerfile");
// values are workspace-relative runfile paths, each of which must be declared
// as a `data` dependency of the test target. This keeps image builds hermetic:
// docker only ever sees files Bazel knows about, never the source checkout.
func WithBuildContext(files map[string]string) ComposeOption {
	return func(o *composeOptions) {
		o.buildContextFiles = files
	}
}

// NewComposeStack creates a new compose stack from the given docker-compose file.
// Automatically registers cleanup to tear down the stack.
// testContext should describe what's being tested (e.g., "gateway", "storage", "e2e-submitqueue").
func NewComposeStack(t *testing.T, log *TestLogger, ctx context.Context, composeFile, testContext string, opts ...ComposeOption) *ComposeStack {
	t.Helper()

	var options composeOptions
	for _, opt := range opts {
		opt(&options)
	}

	// Setup Docker environment
	setupDockerEnv(t)

	// Get absolute path to compose file
	absPath, err := filepath.Abs(composeFile)
	require.NoError(t, err, "failed to get absolute path to compose file")

	buildContextDir := ""
	if len(options.buildContextFiles) > 0 {
		buildContextDir = stageBuildContext(t, options.buildContextFiles)
	}

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
		composeEnv:  composeEnvironment(buildContextDir),
	}

	// Register cleanup
	t.Cleanup(func() {
		stack.stopLogs()

		if os.Getenv("SKIP_CLEANUP") == "true" {
			log.Logf("SKIP_CLEANUP=true - keeping containers for inspection")
			log.Logf("Container prefix: %s", projectName)
			composeCmd := strings.Join(stack.composeCmd, " ")
			log.Logf("Clean up manually: %s -f %s -p %s down -v", composeCmd, absPath, projectName)
			return
		}

		log.Logf("Tearing down compose stack")
		stack.down()
	})

	return stack
}

// Up starts all services in the compose stack.
// Uses --wait to block until all services with healthcheck directives are healthy.
func (s *ComposeStack) Up() error {
	s.t.Helper()
	s.log.Logf("Starting compose stack from %s", s.composeFile)

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "up", "-d", "--build", "--wait")
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Env = s.composeEnv
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start compose stack: %w", err)
	}

	s.log.Logf("Compose stack started successfully")
	s.tailLogs()
	return nil
}

// down stops and removes all services and volumes in the compose stack.
// Locally built images use stable per-worktree names and remain cached for
// subsequent test runs.
func (s *ComposeStack) down() {
	s.log.Logf("Stopping compose stack")

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "down", "-v")
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Env = s.composeEnv
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		s.log.Logf("Warning: failed to stop compose stack: %v", err)
	}
}

// tailLogs starts a background "docker compose logs -f" process that streams
// container logs to stderr in real time. Using os.Stderr instead of t.Log()
// because t.Log() buffers output until the test finishes.
func (s *ComposeStack) tailLogs() {
	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "logs", "-f")
	cmd := exec.Command(s.composeCmd[0], args...)
	cmd.Env = s.composeEnv
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		s.log.Logf("Warning: failed to tail logs: %v", err)
		return
	}
	s.logCmd = cmd
}

// stopLogs kills the background log-tailing process if running.
func (s *ComposeStack) stopLogs() {
	if s.logCmd != nil && s.logCmd.Process != nil {
		s.logCmd.Process.Kill()
		s.logCmd.Wait()
	}
}

// ServicePort returns the mapped host port for a service's container port.
func (s *ComposeStack) ServicePort(serviceName string, containerPort int) (int, error) {
	s.t.Helper()

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName, "port", serviceName, fmt.Sprintf("%d", containerPort))
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Env = s.composeEnv

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
// Requires that Up() has been called first — the TCP-based healthcheck in
// docker-compose ensures MySQL is accepting TCP connections before Up() returns.
func (s *ComposeStack) ConnectMySQLService(serviceName string) (*sql.DB, error) {
	s.t.Helper()

	dsn, err := s.MySQLServiceDSN(serviceName)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping %s mysql: %w", serviceName, err)
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

// StopService sends SIGTERM to a service and waits for it to stop.
// timeoutSec is the maximum time to wait before Docker sends SIGKILL.
func (s *ComposeStack) StopService(serviceName string, timeoutSec int) error {
	s.t.Helper()

	s.log.Logf("Stopping service %s (timeout %ds)", serviceName, timeoutSec)

	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName,
		"stop", "-t", fmt.Sprintf("%d", timeoutSec), serviceName)
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Env = s.composeEnv
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop service %s: %w", serviceName, err)
	}

	s.log.Logf("Service %s stopped", serviceName)
	return nil
}

// ServiceExitCode returns the exit code of a stopped service container.
// Must be called after the service has exited.
func (s *ComposeStack) ServiceExitCode(serviceName string) (int, error) {
	s.t.Helper()

	// Get container ID for the service
	args := append(s.composeCmd[1:], "-f", s.composeFile, "-p", s.projectName,
		"ps", "-a", "-q", serviceName)
	cmd := exec.CommandContext(s.ctx, s.composeCmd[0], args...)
	cmd.Env = s.composeEnv
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get container ID for service %s: %w", serviceName, err)
	}

	containerID := strings.TrimSpace(string(output))
	if containerID == "" {
		return 0, fmt.Errorf("no container found for service %s", serviceName)
	}

	// Get exit code via docker inspect
	inspectCmd := exec.CommandContext(s.ctx, "docker", "inspect",
		"--format", "{{.State.ExitCode}}", containerID)
	inspectOutput, err := inspectCmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to inspect container %s: %w", containerID, err)
	}

	var exitCode int
	_, err = fmt.Sscanf(strings.TrimSpace(string(inspectOutput)), "%d", &exitCode)
	if err != nil {
		return 0, fmt.Errorf("failed to parse exit code from %q: %w", string(inspectOutput), err)
	}

	s.log.Logf("Service %s exit code: %d", serviceName, exitCode)
	return exitCode, nil
}

// setupDockerEnv configures the Docker CLI environment for compose commands.
func setupDockerEnv(t *testing.T) {
	t.Helper()

	// Ensure HOME is set for Docker config
	if os.Getenv("HOME") == "" {
		t.Setenv("HOME", t.TempDir())
	}

	// Bazel gives tests a scratch HOME, which hides user-level Docker CLI
	// plugin installs (e.g. compose v2 in ~/.docker/cli-plugins). Point
	// DOCKER_CONFIG at the invoking user's docker config dir when not set
	// explicitly — mirroring how the docker CLI resolves it outside the
	// sandbox. Machine-level plugin installs are unaffected either way.
	if os.Getenv("DOCKER_CONFIG") == "" {
		if u, err := user.Current(); err == nil && u.HomeDir != "" {
			dockerConfig := filepath.Join(u.HomeDir, ".docker")
			if _, statErr := os.Stat(dockerConfig); statErr == nil {
				t.Setenv("DOCKER_CONFIG", dockerConfig)
			}
		}
	}
}

// stageBuildContext copies the given runfiles into a temporary directory that
// serves as the docker build context. Runfiles are symlinks into Bazel's
// output tree, and docker's context upload does not follow symlinks, so the
// content is copied. Files are staged with the execute bit set because the
// context contains service binaries; docker preserves the mode on COPY.
func stageBuildContext(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for dest, src := range files {
		content, err := os.ReadFile(Runfile(src))
		require.NoError(t, err, "failed to read build context input %s (is it declared as a data dependency?)", src)

		destPath := filepath.Join(dir, dest)
		require.NoError(t, os.MkdirAll(filepath.Dir(destPath), 0o755), "failed to create build context dir for %s", dest)
		require.NoError(t, os.WriteFile(destPath, content, 0o755), "failed to stage build context file %s", dest)
	}
	return dir
}

// composeEnvironment returns the environment used by every compose command.
// The image prefix is stable per test target so --build can reuse the docker
// build cache between runs of the same test. Concurrent runs of the same
// target against one daemon race on the tag, but every run rebuilds via
// `up --build` from its own staged context, so a stale image is never used
// for a completed run.
func composeEnvironment(buildContextDir string) []string {
	seed := os.Getenv("TEST_TARGET")
	if seed == "" {
		seed, _ = os.Getwd()
	}
	sum := sha256.Sum256([]byte(seed))
	imagePrefix := fmt.Sprintf("sq-test-%x", sum[:6])

	env := os.Environ()
	env = append(env, "SQ_DOCKER_IMAGE_PREFIX="+imagePrefix)
	env = append(env, "SQ_MYSQL_DATA_MOUNT_TYPE=tmpfs")
	env = append(env, "SQ_MYSQL_INITDB_SKIP_TZINFO=1")
	if buildContextDir != "" {
		// The compose files resolve their build context from REPO_ROOT; in
		// tests it points at the staged minimal context, not the checkout.
		env = append(env, "REPO_ROOT="+buildContextDir)
	}
	return env
}
