package integration_tests

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uber/submitqueue/integration_tests/testutil"
)

const serverPort = "8080"

// serverBinaryPath returns the path to a Bazel-built server binary.
func serverBinaryPath(name string) string {
	if dir := os.Getenv("TEST_SRCDIR"); dir != "" {
		workspace := os.Getenv("TEST_WORKSPACE")
		return filepath.Join(dir, workspace, "examples/server", name, name+"_", name)
	}
	return filepath.Join("examples/server", name, name)
}

// startServerContainer builds a Docker image from the server binary and starts it.
func startServerContainer(
	ctx context.Context,
	t *testing.T,
	log *testutil.TestLogger,
	name string,
	env map[string]string,
	nw *testcontainers.DockerNetwork,
) (testcontainers.Container, string) {
	t.Helper()

	binaryPath := serverBinaryPath(name)
	log.Logf("Resolved %s binary: %s", name, binaryPath)

	// Create temp build context with binary and Dockerfile.
	tmpDir := t.TempDir()
	copyBinary(t, binaryPath, filepath.Join(tmpDir, "server"))

	dockerfile := "FROM debian:bookworm-slim\nCOPY server /usr/local/bin/server\nCMD [\"/usr/local/bin/server\"]\n"
	os.WriteFile(filepath.Join(tmpDir, "Dockerfile"), []byte(dockerfile), 0o644)

	env["PORT"] = ":" + serverPort

	log.Logf("Starting %s container", name)
	ctr, err := testcontainers.Run(ctx, "",
		testcontainers.WithDockerfile(testcontainers.FromDockerfile{
			Context:    tmpDir,
			Dockerfile: "Dockerfile",
		}),
		testcontainers.WithExposedPorts(serverPort+"/tcp"),
		testcontainers.WithEnv(env),
		testcontainers.WithWaitStrategy(wait.ForLog("gRPC server is running")),
		network.WithNetwork([]string{name}, nw),
	)
	if err != nil {
		// Print container logs on failure if container was created
		if ctr != nil {
			if logs, logErr := ctr.Logs(ctx); logErr == nil {
				logBytes, _ := io.ReadAll(logs)
				log.Logf("%s container logs:\n%s", name, string(logBytes))
			}
		}
		require.NoError(t, err, "failed to start %s container", name)
	}
	t.Cleanup(func() {
		log.Logf("Terminating %s container", name)
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("failed to terminate %s container: %v", name, err)
		}
		log.Logf("%s container terminated", name)
	})

	mappedPort, err := ctr.MappedPort(ctx, serverPort+"/tcp")
	require.NoError(t, err, "failed to get mapped port for %s", name)
	host, err := ctr.Host(ctx)
	require.NoError(t, err, "failed to get host for %s", name)
	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	log.Logf("%s container started on %s", name, addr)
	return ctr, addr
}


func startGatewayContainer(ctx context.Context, t *testing.T, log *testutil.TestLogger, nw *testcontainers.DockerNetwork) string {
	t.Helper()
	_, addr := startServerContainer(ctx, t, log, "gateway", map[string]string{
		"MYSQL_DSN": "root:root@tcp(mysql:3306)/submitqueue?parseTime=true",
	}, nw)
	return addr
}

func startOrchestratorContainer(ctx context.Context, t *testing.T, log *testutil.TestLogger, nw *testcontainers.DockerNetwork) string {
	t.Helper()
	_, addr := startServerContainer(ctx, t, log, "orchestrator", map[string]string{}, nw)
	return addr
}

func startSpeculatorContainer(ctx context.Context, t *testing.T, log *testutil.TestLogger, nw *testcontainers.DockerNetwork) string {
	t.Helper()
	_, addr := startServerContainer(ctx, t, log, "speculator", map[string]string{}, nw)
	return addr
}

// copyBinary copies a file from src to dst preserving executable permissions.
func copyBinary(t *testing.T, src, dst string) {
	t.Helper()

	in, err := os.Open(src)
	require.NoError(t, err, "failed to open binary %s", src)
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0o755)
	require.NoError(t, err, "failed to create binary copy %s", dst)
	defer out.Close()

	_, err = io.Copy(out, in)
	require.NoError(t, err, "failed to copy binary from %s to %s", src, dst)
}
