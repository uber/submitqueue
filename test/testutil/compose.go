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
	"hash"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DockerBuildContextFile maps a declared Bazel runfile into a Docker build context.
type DockerBuildContextFile struct {
	// Runfile is the workspace-relative path of a file declared in the test's data.
	Runfile string
	// Path is the relative destination path inside the staged Docker build context.
	Path string
}

// ComposeConfig declares every file a Docker Compose test needs from Bazel runfiles.
type ComposeConfig struct {
	// ComposeFile is the workspace-relative path of the Compose file.
	ComposeFile string
	// DockerBuildContext contains the files needed by services with build directives.
	DockerBuildContext []DockerBuildContextFile
}

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

// getDockerComposeCommand returns the Docker Compose command to use.
// Compose V2 supports the --wait lifecycle used by the test harness.
func getDockerComposeCommand() []string {
	if _, err := exec.LookPath("docker"); err == nil {
		return []string{"docker", "compose"}
	}

	if _, err := exec.LookPath("docker-compose"); err == nil {
		return []string{"docker-compose"}
	}

	return []string{"docker", "compose"}
}

// NewComposeStack creates a new compose stack from declared Bazel runfiles.
// Automatically registers cleanup to tear down the stack.
// testContext should describe what's being tested (e.g., "gateway", "storage", "e2e-submitqueue").
func NewComposeStack(t *testing.T, log *TestLogger, ctx context.Context, config ComposeConfig, testContext string) *ComposeStack {
	t.Helper()

	setupDockerEnv(t)

	composeFile := resolveRunfile(t, config.ComposeFile)
	inputHash := sha256.New()
	hashFile(t, inputHash, config.ComposeFile, composeFile)
	buildContext := stageDockerBuildContext(t, inputHash, config.DockerBuildContext)

	// Generate meaningful project name: sq-test-{context}-{short-timestamp}
	// Results in container names like: sq-test-gateway-a1b2c3d-mysql-app-1
	timestamp := fmt.Sprintf("%x", time.Now().UnixNano()&0xFFFFFF) // Last 6 hex digits
	projectName := fmt.Sprintf("sq-test-%s-%s", testContext, timestamp)

	stack := &ComposeStack{
		composeFile: composeFile,
		projectName: projectName,
		t:           t,
		log:         log,
		ctx:         ctx,
		composeCmd:  getDockerComposeCommand(),
		composeEnv:  composeEnvironment(inputHash.Sum(nil), buildContext),
	}

	// Register cleanup
	t.Cleanup(func() {
		stack.stopLogs()

		if os.Getenv("SKIP_CLEANUP") == "true" {
			log.Logf("SKIP_CLEANUP=true - keeping containers for inspection")
			log.Logf("Container prefix: %s", projectName)
			composeCmd := strings.Join(stack.composeCmd, " ")
			log.Logf("Clean up manually: %s -f %s -p %s down -v", composeCmd, composeFile, projectName)
			return
		}

		log.Logf("Tearing down compose stack")
		stack.down()
	})

	return stack
}

func resolveRunfile(t *testing.T, runfile string) string {
	t.Helper()

	if filepath.IsAbs(runfile) {
		require.FileExists(t, runfile, "runfile does not exist")
		return runfile
	}

	if testSrcDir := os.Getenv("TEST_SRCDIR"); testSrcDir != "" {
		workspace := os.Getenv("TEST_WORKSPACE")
		if workspace == "" {
			workspace = "_main"
		}
		resolved := filepath.Join(testSrcDir, workspace, filepath.FromSlash(runfile))
		require.FileExists(t, resolved, "Bazel runfile does not exist")
		return resolved
	}

	resolved, err := filepath.Abs(runfile)
	require.NoError(t, err, "failed to resolve file %s", runfile)
	require.FileExists(t, resolved, "file does not exist")
	return resolved
}

func stageDockerBuildContext(t *testing.T, inputHash hash.Hash, files []DockerBuildContextFile) string {
	t.Helper()

	if len(files) == 0 {
		return ""
	}

	files = append([]DockerBuildContextFile(nil), files...)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	contextDir := t.TempDir()
	for _, file := range files {
		require.True(t, filepath.IsLocal(file.Path), "Docker build context path must be relative: %s", file.Path)

		source := resolveRunfile(t, file.Runfile)
		destination := filepath.Join(contextDir, filepath.FromSlash(file.Path))
		require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o755), "failed to create Docker build context directory")

		sourceFile, err := os.Open(source)
		require.NoError(t, err, "failed to open Docker build context runfile %s", file.Runfile)

		info, err := sourceFile.Stat()
		require.NoError(t, err, "failed to stat Docker build context runfile %s", file.Runfile)

		destinationFile, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		require.NoError(t, err, "failed to create Docker build context file %s", file.Path)

		_, _ = io.WriteString(inputHash, file.Path)
		_, _ = inputHash.Write([]byte{0})
		_, err = io.Copy(io.MultiWriter(destinationFile, inputHash), sourceFile)
		require.NoError(t, err, "failed to stage Docker build context file %s", file.Path)
		require.NoError(t, destinationFile.Close(), "failed to close Docker build context file %s", file.Path)
		require.NoError(t, sourceFile.Close(), "failed to close Docker build context runfile %s", file.Runfile)
	}

	return contextDir
}

func hashFile(t *testing.T, inputHash hash.Hash, logicalPath, file string) {
	t.Helper()

	_, _ = io.WriteString(inputHash, logicalPath)
	_, _ = inputHash.Write([]byte{0})

	f, err := os.Open(file)
	require.NoError(t, err, "failed to open declared input %s", logicalPath)
	_, err = io.Copy(inputHash, f)
	require.NoError(t, err, "failed to hash declared input %s", logicalPath)
	require.NoError(t, f.Close(), "failed to close declared input %s", logicalPath)
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
// Locally built images use declared-input hashes and remain cached for
// subsequent runs with the same build context.
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

// setupDockerEnv configures Docker environment for docker-compose.
func setupDockerEnv(t *testing.T) {
	t.Helper()

	// Ensure HOME is set for Docker config
	if os.Getenv("HOME") == "" {
		t.Setenv("HOME", t.TempDir())
	}

	// Bazel may replace HOME with a sandbox directory. Preserve access to the
	// host Docker CLI configuration and user-installed Compose plugin without
	// exposing the source checkout.
	if os.Getenv("DOCKER_CONFIG") == "" {
		currentUser, err := user.Current()
		if err == nil {
			dockerConfig := filepath.Join(currentUser.HomeDir, ".docker")
			if info, statErr := os.Stat(dockerConfig); statErr == nil && info.IsDir() {
				t.Setenv("DOCKER_CONFIG", dockerConfig)
			}
		}
	}
}

// composeEnvironment returns the environment used by every compose command.
// The image prefix is derived from declared inputs so identical contexts reuse
// images without sharing stale images across different source revisions.
func composeEnvironment(inputHash []byte, buildContext string) []string {
	env := os.Environ()
	env = setEnvironment(env, "SQ_DOCKER_IMAGE_PREFIX", fmt.Sprintf("sq-test-%x", inputHash[:6]))
	env = setEnvironment(env, "SQ_MYSQL_DATA_MOUNT_TYPE", "tmpfs")
	env = setEnvironment(env, "SQ_MYSQL_INITDB_SKIP_TZINFO", "1")
	if buildContext != "" {
		env = setEnvironment(env, "SQ_DOCKER_BUILD_CONTEXT", buildContext)
	}
	return env
}

func setEnvironment(env []string, key, value string) []string {
	prefix := key + "="
	filtered := env[:0]
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, prefix+value)
}
