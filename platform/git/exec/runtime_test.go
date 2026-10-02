// Copyright (c) 2026 Uber Technologies, Inc.
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

package gitexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dummyRuntime(t *testing.T) Runtime {
	t.Helper()
	dir := t.TempDir()
	return Runtime{
		Executable:  filepath.Join(dir, "git"),
		ExecPath:    filepath.Join(dir, "git-core"),
		TemplateDir: filepath.Join(dir, "templates"),
	}
}

func TestRuntimeValidate(t *testing.T) {
	valid := dummyRuntime(t)
	tests := []struct {
		name    string
		mutate  func(*Runtime)
		wantErr bool
	}{
		{name: "valid", mutate: func(*Runtime) {}},
		{name: "missing executable", mutate: func(r *Runtime) { r.Executable = "" }, wantErr: true},
		{name: "relative exec path", mutate: func(r *Runtime) { r.ExecPath = "git-core" }, wantErr: true},
		{name: "relative template dir", mutate: func(r *Runtime) { r.TemplateDir = "templates" }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := valid
			tt.mutate(&runtime)
			err := runtime.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRuntimeCommandDoesNotInheritEnvironment(t *testing.T) {
	t.Setenv("SUBMITQUEUE_GIT_AMBIENT", "ambient")
	runtime := dummyRuntime(t)

	cmd := runtime.Command(context.Background(), t.TempDir(), "--version")

	assert.Equal(t, runtime.Executable, cmd.Path)
	assert.NotContains(t, cmd.Env, "SUBMITQUEUE_GIT_AMBIENT=ambient")
	assert.Contains(t, cmd.Env, "GIT_CONFIG_NOSYSTEM=1")
	assert.Contains(t, cmd.Env, "GIT_EXEC_PATH="+runtime.ExecPath)
}

func TestRuntimeCommandPreservesAuthEnvironment(t *testing.T) {
	// Scrubbing denies git ambient configuration; it must not also deny it the
	// means to reach the remote. Without the agent socket an SSH remote cannot
	// authenticate, and without PATH git cannot even exec ssh.
	t.Setenv("SSH_AUTH_SOCK", "/tmp/agent.sock")
	t.Setenv("PATH", "/usr/bin:/bin")
	t.Setenv("HTTPS_PROXY", "http://proxy.example.com:3128")
	t.Setenv("SUBMITQUEUE_GIT_AMBIENT", "ambient")

	cmd := dummyRuntime(t).Command(context.Background(), t.TempDir(), "--version")

	assert.Contains(t, cmd.Env, "SSH_AUTH_SOCK=/tmp/agent.sock")
	assert.Contains(t, cmd.Env, "PATH=/usr/bin:/bin")
	assert.Contains(t, cmd.Env, "HTTPS_PROXY=http://proxy.example.com:3128")
	assert.NotContains(t, cmd.Env, "SUBMITQUEUE_GIT_AMBIENT=ambient")
}

func TestRuntimeCommandOmitsUnsetAuthEnvironment(t *testing.T) {
	// An unset variable must be omitted rather than exported empty: an empty
	// SSH_AUTH_SOCK tells ssh there is no agent instead of letting it look.
	t.Setenv("SSH_AUTH_SOCK", "")
	require.NoError(t, os.Unsetenv("SSH_AUTH_SOCK"))

	cmd := dummyRuntime(t).Command(context.Background(), t.TempDir(), "--version")

	for _, entry := range cmd.Env {
		assert.False(t, strings.HasPrefix(entry, "SSH_AUTH_SOCK="), "unset variable leaked as %q", entry)
	}
}

func TestRuntimeCommandPassesThroughExtraEnvironment(t *testing.T) {
	t.Setenv("SUBMITQUEUE_CUSTOM_TRANSPORT", "value")
	runtime := dummyRuntime(t)
	runtime.PassthroughEnv = []string{"SUBMITQUEUE_CUSTOM_TRANSPORT"}

	cmd := runtime.Command(context.Background(), t.TempDir(), "--version")

	assert.Contains(t, cmd.Env, "SUBMITQUEUE_CUSTOM_TRANSPORT=value")
}

func TestRuntimeCommandIsolatesHome(t *testing.T) {
	t.Setenv("HOME", "/ambient/home")
	dir := t.TempDir()

	cmd := dummyRuntime(t).Command(context.Background(), dir, "--version")

	assert.Contains(t, cmd.Env, "HOME="+filepath.Join(dir, ".submitqueue-git-home"))
	assert.NotContains(t, cmd.Env, "HOME=/ambient/home")
}
