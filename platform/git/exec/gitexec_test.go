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

func TestOperation(t *testing.T) {
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
			assert.Equal(t, tt.want, Operation(tt.args))
		})
	}
}

func TestCommandFailure(t *testing.T) {
	cause := errors.New("signal: killed")

	t.Run("a live context yields a classifiable command error", func(t *testing.T) {
		err := CommandFailure(context.Background(), []string{"fetch", "origin"}, "connection reset", cause)

		var commandErr *CommandError
		require.ErrorAs(t, err, &commandErr)
		assert.Equal(t, "fetch", commandErr.Operation())
		assert.Equal(t, "connection reset", commandErr.Diagnostic())
	})

	// os/exec reports a context-killed child as a bare *exec.ExitError reading
	// "signal: killed", with the cancellation nowhere in the chain. Surfacing
	// ctx.Err() is what lets the generic classifier recognise it.
	t.Run("an ended context surfaces the cancellation instead", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := CommandFailure(ctx, []string{"fetch", "origin"}, "signal: killed", cause)

		assert.ErrorIs(t, err, context.Canceled)
		var commandErr *CommandError
		assert.NotErrorAs(t, err, &commandErr, "the git node must not shadow the cancellation")
	})

	t.Run("empty arguments do not panic", func(t *testing.T) {
		err := CommandFailure(context.Background(), nil, "boom", cause)

		var commandErr *CommandError
		require.ErrorAs(t, err, &commandErr)
		assert.Empty(t, commandErr.Operation())
	})
}
