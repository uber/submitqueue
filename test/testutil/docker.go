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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// defaultDockerHost is the endpoint the Docker CLI and Compose use when
// DOCKER_HOST is unset.
const defaultDockerHost = "unix:///var/run/docker.sock"

// DockerIsRootless reports whether the test's Docker daemon (see
// setActiveDockerContextHost) runs in rootless mode. It asks the Engine API
// directly, so a test needs no Docker CLI on its PATH.
func DockerIsRootless(t *testing.T) bool {
	t.Helper()

	setActiveDockerContextHost(t)
	client, err := dockerEngineClient(os.Getenv("DOCKER_HOST"))
	require.NoError(t, err)
	resp, err := client.Get("http://docker/info")
	require.NoError(t, err, "failed to query Docker daemon info")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "Docker daemon info returned %s", resp.Status)

	var info struct {
		SecurityOptions []string `json:"SecurityOptions"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&info), "failed to decode Docker daemon info")
	for _, option := range info.SecurityOptions {
		if strings.Contains(option, "name=rootless") {
			return true
		}
	}
	return false
}

// setActiveDockerContextHost sets DOCKER_HOST, when it is unset, to the
// endpoint of the invoking user's active Docker context. Only the context's
// endpoint is read from ~/.docker; tests run Docker with an empty config of
// their own (see setupDockerEnv). With no named context, DOCKER_HOST stays
// unset and the default socket applies.
func setActiveDockerContextHost(t *testing.T) {
	t.Helper()

	if os.Getenv("DOCKER_HOST") != "" {
		return
	}
	u, err := user.Current()
	if err != nil || u.HomeDir == "" {
		return
	}
	if host := dockerContextHost(t, filepath.Join(u.HomeDir, ".docker")); host != "" {
		t.Setenv("DOCKER_HOST", host)
	}
}

// dockerContextHost returns the daemon endpoint of the current context in the
// Docker config directory dir, or "" when the default endpoint applies.
func dockerContextHost(t *testing.T, dir string) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return ""
	}
	require.NoError(t, err, "failed to read Docker config in %s", dir)
	var config struct {
		CurrentContext string `json:"currentContext"`
	}
	require.NoError(t, json.Unmarshal(raw, &config), "failed to parse Docker config in %s", dir)
	if config.CurrentContext == "" || config.CurrentContext == "default" {
		return ""
	}

	// The Docker CLI stores each context under the SHA-256 of its name.
	contextID := sha256.Sum256([]byte(config.CurrentContext))
	raw, err = os.ReadFile(filepath.Join(dir, "contexts", "meta", hex.EncodeToString(contextID[:]), "meta.json"))
	require.NoError(t, err, "failed to read Docker context %q", config.CurrentContext)
	var meta struct {
		Endpoints map[string]struct {
			Host string `json:"Host"`
		} `json:"Endpoints"`
	}
	require.NoError(t, json.Unmarshal(raw, &meta), "failed to parse Docker context %q", config.CurrentContext)
	return meta.Endpoints["docker"].Host
}

// dockerEngineClient returns an HTTP client whose every request reaches the
// Engine API at host, a DOCKER_HOST value ("" selects the default socket).
// TLS endpoints are not supported: only the endpoint address reaches a test.
func dockerEngineClient(host string) (*http.Client, error) {
	if host == "" {
		host = defaultDockerHost
	}
	endpoint, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("invalid DOCKER_HOST %q: %w", host, err)
	}

	var network, address string
	switch endpoint.Scheme {
	case "unix":
		network, address = "unix", endpoint.Path
	case "tcp":
		network, address = "tcp", endpoint.Host
	default:
		return nil, fmt.Errorf("unsupported DOCKER_HOST scheme %q in %q", endpoint.Scheme, host)
	}

	var dialer net.Dialer
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	}}, nil
}
