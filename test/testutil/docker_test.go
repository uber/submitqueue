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
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDockerIsRootless(t *testing.T) {
	for _, tt := range []struct {
		name string
		info string
		want bool
	}{
		{
			name: "rootless daemon",
			info: `{"SecurityOptions": ["name=seccomp,profile=builtin", "name=rootless", "name=cgroupns"]}`,
			want: true,
		},
		{
			name: "rootful daemon",
			info: `{"SecurityOptions": ["name=seccomp,profile=builtin", "name=cgroupns"]}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/info" {
					http.NotFound(w, r)
					return
				}
				_, _ = w.Write([]byte(tt.info))
			}))
			defer daemon.Close()
			t.Setenv("DOCKER_HOST", "tcp://"+strings.TrimPrefix(daemon.URL, "http://"))

			assert.Equal(t, tt.want, DockerIsRootless(t))
		})
	}
}

func TestDockerEngineClient_RejectsUnsupportedScheme(t *testing.T) {
	for _, host := range []string{"ssh://user@host", "npipe:////./pipe/docker_engine"} {
		t.Run(host, func(t *testing.T) {
			_, err := dockerEngineClient(host)
			require.Error(t, err)
		})
	}
}

func TestDockerContextHost(t *testing.T) {
	writeFile := func(t *testing.T, path, contents string) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	}
	contextMetaPath := func(dir, name string) string {
		id := sha256.Sum256([]byte(name))
		return filepath.Join(dir, "contexts", "meta", hex.EncodeToString(id[:]), "meta.json")
	}

	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, dir string)
		want  string
	}{
		{
			name:  "no config",
			setup: func(*testing.T, string) {},
		},
		{
			name: "no current context",
			setup: func(t *testing.T, dir string) {
				writeFile(t, filepath.Join(dir, "config.json"), `{"credsStore": "desktop"}`)
			},
		},
		{
			name: "default context",
			setup: func(t *testing.T, dir string) {
				writeFile(t, filepath.Join(dir, "config.json"), `{"currentContext": "default"}`)
			},
		},
		{
			name: "named context",
			setup: func(t *testing.T, dir string) {
				writeFile(t, filepath.Join(dir, "config.json"), `{"currentContext": "desktop-linux"}`)
				writeFile(t, contextMetaPath(dir, "desktop-linux"),
					`{"Name": "desktop-linux", "Endpoints": {"docker": {"Host": "unix:///run/desktop.sock"}}}`)
			},
			want: "unix:///run/desktop.sock",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.setup(t, dir)
			assert.Equal(t, tt.want, dockerContextHost(t, dir))
		})
	}
}
