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
	"os"
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
