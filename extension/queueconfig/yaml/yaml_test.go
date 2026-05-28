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

package yaml

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/extension/queueconfig"
)

func writeTempYAML(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queues.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestNewStore(t *testing.T) {
	cases := []struct {
		name     string
		yaml     string
		wantErr  bool
		wantSize int
	}{
		{
			name: "single queue",
			yaml: `queues:
  - name: main
    vcs_type: git
    vcs_address: git@github.com:uber/submitqueue.git
    target: main
`,
			wantSize: 1,
		},
		{
			name: "multiple queues",
			yaml: `queues:
  - name: main
    vcs_type: git
    vcs_address: git@github.com:uber/submitqueue.git
    target: main
  - name: release
    vcs_type: git
    vcs_address: git@github.com:uber/submitqueue.git
    target: release/v2
`,
			wantSize: 2,
		},
		{
			name:     "empty queues list",
			yaml:     `queues: []`,
			wantSize: 0,
		},
		{
			name:     "missing queues key",
			yaml:     `other: value`,
			wantSize: 0,
		},
		{
			name: "duplicate names rejected",
			yaml: `queues:
  - name: main
    target: main
  - name: main
    target: release
`,
			wantErr: true,
		},
		{
			name: "empty name rejected",
			yaml: `queues:
  - target: main
`,
			wantErr: true,
		},
		{
			name:    "malformed yaml rejected",
			yaml:    `queues: [`,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempYAML(t, tc.yaml)
			store, err := NewStore(path)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			got, err := store.List(context.Background())
			require.NoError(t, err)
			assert.Len(t, got, tc.wantSize)
		})
	}
}

func TestNewStore_MissingFile(t *testing.T) {
	_, err := NewStore(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	require.Error(t, err)
}

func TestStore_Get(t *testing.T) {
	path := writeTempYAML(t, `queues:
  - name: main
    vcs_type: git
    vcs_address: git@github.com:uber/submitqueue.git
    target: main
    change_provider: github
`)
	store, err := NewStore(path)
	require.NoError(t, err)

	t.Run("known queue returns config", func(t *testing.T) {
		cfg, err := store.Get(context.Background(), "main")
		require.NoError(t, err)
		assert.Equal(t, "main", cfg.Name)
		assert.Equal(t, "git", cfg.VCSType)
		assert.Equal(t, "github", cfg.ChangeProvider)
	})

	t.Run("unknown queue returns ErrNotFound", func(t *testing.T) {
		_, err := store.Get(context.Background(), "nope")
		require.Error(t, err)
		assert.True(t, errors.Is(err, queueconfig.ErrNotFound))
	})
}

func TestStore_List_ReturnsCopy(t *testing.T) {
	path := writeTempYAML(t, `queues:
  - name: main
    target: main
`)
	store, err := NewStore(path)
	require.NoError(t, err)

	first, err := store.List(context.Background())
	require.NoError(t, err)
	require.Len(t, first, 1)
	first[0].Name = "mutated"

	second, err := store.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "main", second[0].Name, "mutating returned slice must not affect store")
}
