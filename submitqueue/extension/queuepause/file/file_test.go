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

package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func writeFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pause.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestGet(t *testing.T) {
	const contents = `
queues:
  - name: rejecting
    reject_lands: true
  - name: freezing
    freeze_processing: true
  - name: both
    reject_lands: true
    freeze_processing: true
  - name: with-reason
    reject_lands: true
    reason: repo freeze until 17:00
  - name: listed-but-running
`
	tests := []struct {
		name  string
		queue string
		want  entity.QueuePause
	}{
		{name: "reject only", queue: "rejecting", want: entity.QueuePause{RejectLands: true}},
		{name: "freeze only", queue: "freezing", want: entity.QueuePause{FreezeProcessing: true}},
		{name: "both switches", queue: "both", want: entity.QueuePause{RejectLands: true, FreezeProcessing: true}},
		{name: "reason is carried", queue: "with-reason", want: entity.QueuePause{RejectLands: true, Reason: "repo freeze until 17:00"}},
		{name: "listed with no switches set", queue: "listed-but-running", want: entity.QueuePause{}},
		{name: "unlisted queue is not paused", queue: "other", want: entity.QueuePause{}},
	}
	store, err := NewStore(writeFile(t, contents))
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Get(context.Background(), tt.queue)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewStoreEmptyFile(t *testing.T) {
	store, err := NewStore(writeFile(t, ""))
	require.NoError(t, err)

	got, err := store.Get(context.Background(), "any")
	require.NoError(t, err)
	assert.Equal(t, entity.QueuePause{}, got)
}

func TestNewStoreRejectsInvalidFile(t *testing.T) {
	tests := []struct {
		name     string
		contents string
	}{
		{name: "malformed yaml", contents: "queues: [unterminated"},
		{name: "empty queue name", contents: "queues:\n  - reject_lands: true\n"},
		{name: "duplicate queue name", contents: "queues:\n  - name: a\n  - name: a\n"},
		{name: "misspelled switch", contents: "queues:\n  - name: a\n    reject_land: true\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStore(writeFile(t, tt.contents))
			require.Error(t, err)
		})
	}

	t.Run("missing file", func(t *testing.T) {
		_, err := NewStore(filepath.Join(t.TempDir(), "absent.yaml"))
		require.Error(t, err)
	})
}

func TestGetReflectsFileChanges(t *testing.T) {
	path := writeFile(t, "queues:\n  - name: q\n")
	store, err := NewStore(path)
	require.NoError(t, err)

	got, err := store.Get(context.Background(), "q")
	require.NoError(t, err)
	assert.Equal(t, entity.QueuePause{}, got)

	require.NoError(t, os.WriteFile(path, []byte("queues:\n  - name: q\n    reject_lands: true\n"), 0o600))
	got, err = store.Get(context.Background(), "q")
	require.NoError(t, err)
	assert.Equal(t, entity.QueuePause{RejectLands: true}, got)
}

func TestGetFailsWhenFileBecomesUnreadable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, path string)
	}{
		{name: "file removed", mutate: func(t *testing.T, path string) { require.NoError(t, os.Remove(path)) }},
		{name: "file corrupted", mutate: func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("queues: [unterminated"), 0o600))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeFile(t, "queues:\n  - name: q\n    reject_lands: true\n")
			store, err := NewStore(path)
			require.NoError(t, err)

			tt.mutate(t, path)

			_, err = store.Get(context.Background(), "q")
			require.Error(t, err)
		})
	}
}
