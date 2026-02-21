package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// SetupDockerEnv configures Docker environment for testcontainers and creates a network.
// Automatically registers cleanup to remove the network on test completion.
// Returns the Docker network and the context to use for container operations.
func SetupDockerEnv(t *testing.T, log *TestLogger, ctx context.Context) (*testcontainers.DockerNetwork, context.Context) {
	t.Helper()

	// Disable Ryuk reaper for Docker-in-Docker environments
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	// Ensure HOME is set for Docker config
	if os.Getenv("HOME") == "" {
		t.Setenv("HOME", t.TempDir())
	}

	// Create Docker network
	nw, err := network.New(ctx)
	require.NoError(t, err, "failed to create Docker network")

	log.Logf("Docker network created: %s", nw.Name)

	// Register cleanup
	t.Cleanup(func() {
		log.Logf("Removing Docker network")
		require.NoError(t, nw.Remove(ctx), "failed to remove Docker network")
		log.Logf("Docker network removed")
	})

	return nw, ctx
}
