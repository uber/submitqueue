// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePort(t *testing.T) {
	tests := map[string]int{
		"0.0.0.0:49153\n": 49153,
		"[::]:49154":      49154,
	}
	for output, want := range tests {
		got, err := parsePort(output)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestFindRepoRootUsesWorkspaceEnvironment(t *testing.T) {
	t.Setenv("REPO_ROOT", "/tmp/repo")
	root, err := findRepoRoot(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "/tmp/repo", root)
}

func TestLocalStackBoundsQueueDatabaseConnections(t *testing.T) {
	stack := newLocalStack("/repo", "/profile", nil, nil)
	assert.True(t, slices.Contains(stack.baseEnv, "QUEUE_MYSQL_MAX_OPEN_CONNECTIONS=32"))
}
