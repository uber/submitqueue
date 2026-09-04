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

package gitexec

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// value returns the value of NAME=value entry for name, and whether it is
// present at all.
func value(env []string, name string) (string, bool) {
	prefix := name + "="
	found := ""
	ok := false
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			found = strings.TrimPrefix(e, prefix)
			ok = true
		}
	}
	return found, ok
}

func TestEnv_AlwaysScrubs(t *testing.T) {
	env := Env(EnvOptions{})
	for _, want := range scrubEnv {
		assert.Contains(t, env, want)
	}
}

func TestEnv_TransportInheritedOnlyWhenSet(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "/tmp/agent.sock")

	withTransport := Env(EnvOptions{Transport: true})
	got, ok := value(withTransport, "SSH_AUTH_SOCK")
	assert.True(t, ok)
	assert.Equal(t, "/tmp/agent.sock", got)

	withoutTransport := Env(EnvOptions{})
	_, ok = value(withoutTransport, "SSH_AUTH_SOCK")
	assert.False(t, ok)
}

func TestEnv_UnsetTransportVarStaysAbsent(t *testing.T) {
	// An unset SSH_AUTH_SOCK means "there is no agent", so it must be absent
	// rather than exported empty. t.Setenv records the original for restoration;
	// Unsetenv then removes it for the duration of the test.
	t.Setenv("SSH_AUTH_SOCK", "placeholder")
	require.NoError(t, os.Unsetenv("SSH_AUTH_SOCK"))

	env := Env(EnvOptions{Transport: true})
	_, ok := value(env, "SSH_AUTH_SOCK")
	assert.False(t, ok)
}

func TestEnv_LiteralOverridesInherited(t *testing.T) {
	t.Setenv("PATH", "/host/bin")

	env := Env(EnvOptions{Transport: true, Literal: []string{"PATH=/pinned/bin"}})
	got, ok := value(env, "PATH")
	assert.True(t, ok)
	assert.Equal(t, "/pinned/bin", got)
}

func TestEnv_PassthroughDeduplicatesWithTransport(t *testing.T) {
	t.Setenv("PATH", "/host/bin")

	env := Env(EnvOptions{Transport: true, Passthrough: []string{"PATH"}})
	count := 0
	for _, e := range env {
		if strings.HasPrefix(e, "PATH=") {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

func TestEnv_HomeNotInSharedTransportList(t *testing.T) {
	assert.NotContains(t, transportEnvNames, "HOME")
}

func TestCommandError(t *testing.T) {
	cause := errors.New("exit status 128")
	tests := []struct {
		name        string
		err         *CommandError
		wantMessage string
		wantCause   error
	}{
		{
			name:        "supplied diagnostic is rendered",
			err:         NewCommandError("fetch", "connection reset", cause),
			wantMessage: "connection reset",
			wantCause:   cause,
		},
		{
			name:        "cause is rendered when diagnostic is empty",
			err:         NewCommandError("reset", "", cause),
			wantMessage: cause.Error(),
			wantCause:   cause,
		},
		{
			name:        "fallback is rendered without diagnostic or cause",
			err:         NewCommandError("unknown", "", nil),
			wantMessage: "git command failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMessage, tt.err.Error())
			assert.Equal(t, tt.err.message, tt.err.Diagnostic())
			assert.Equal(t, tt.err.operation, tt.err.Operation())
			if tt.wantCause == nil {
				assert.NoError(t, tt.err.Unwrap())
			} else {
				assert.ErrorIs(t, tt.err, tt.wantCause)
			}
			assert.False(t, tt.err.ProcessExited())
		})
	}
}

func TestCommandError_ProcessExited(t *testing.T) {
	err := exec.Command(os.Args[0], "-test.run=[").Run()
	require.Error(t, err)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)

	tests := []struct {
		name string
		err  *CommandError
		want bool
	}{
		{
			name: "non-zero process exit is reported",
			err:  NewCommandError("fetch", "failed", exitErr),
			want: true,
		},
		{
			name: "ordinary cause did not exit a process",
			err:  NewCommandError("fetch", "failed", errors.New("start failure")),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err.ProcessExited())
		})
	}
}

func TestOutput_PreservesCommandFailure(t *testing.T) {
	tests := []struct {
		name          string
		executable    string
		args          []string
		wantOperation string
	}{
		{
			name:          "non-zero process exit retains command provenance and cause",
			executable:    os.Args[0],
			args:          []string{"-test.run=["},
			wantOperation: "-test.run=[",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Output(context.Background(), tt.executable, "", tt.args...)
			require.Error(t, err)

			var commandErr *CommandError
			require.ErrorAs(t, err, &commandErr)
			assert.Equal(t, tt.wantOperation, commandErr.Operation())

			var exitErr *exec.ExitError
			assert.ErrorAs(t, err, &exitErr)
		})
	}
}

func TestCommandOperation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "first argument is the operation",
			args: []string{"fetch", "origin"},
			want: "fetch",
		},
		{
			name: "empty arguments have no operation",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, commandOperation(tt.args))
		})
	}
}
