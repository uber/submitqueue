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

package yaml

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueconfig"
)

func writeQueueConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queues.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestNewStore(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "loads admission policies",
			content: `queues:
  - name: monorepo/main
    max_concurrent: 2
    gate_wait_delay_ms: 5000
    minimum_build_admission_interval_ms: 3600000
    failure_cooldown_ms: 900000
`,
		},
		{name: "empty list", content: "queues: []\n"},
		{name: "empty document", content: "", wantErr: true},
		{name: "malformed YAML", content: "queues: [", wantErr: true},
		{name: "unknown field", content: validQueueYAML("main", 1, 5000, 0, 0) + "unexpected: true\n", wantErr: true},
		{name: "empty name", content: validQueueYAML("", 1, 5000, 0, 0), wantErr: true},
		{name: "non-positive concurrency", content: validQueueYAML("main", 0, 5000, 0, 0), wantErr: true},
		{name: "non-positive gate delay", content: validQueueYAML("main", 1, 0, 0, 0), wantErr: true},
		{name: "negative minimum interval disables policy", content: validQueueYAML("main", 1, 5000, -1, 0)},
		{name: "negative failure cooldown disables policy", content: validQueueYAML("main", 1, 5000, 0, -1)},
		{
			name: "duplicate name",
			content: validQueueYAML("main", 1, 5000, 0, 0) + `  - name: main
    max_concurrent: 1
    gate_wait_delay_ms: 5000
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewStore(writeQueueConfig(t, tt.content))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			_, err = store.List(context.Background())
			require.NoError(t, err)
		})
	}
}

func validQueueYAML(name string, maxConcurrent int32, gateWaitDelayMs, minimumIntervalMs, failureCooldownMs int64) string {
	return fmt.Sprintf(`queues:
  - name: %s
    max_concurrent: %d
    gate_wait_delay_ms: %d
    minimum_build_admission_interval_ms: %d
    failure_cooldown_ms: %d
`, name, maxConcurrent, gateWaitDelayMs, minimumIntervalMs, failureCooldownMs)
}

func TestStoreGetAndList(t *testing.T) {
	store, err := NewStore(writeQueueConfig(t, validQueueYAML("monorepo/main", 2, 5000, 3_600_000, 900_000)))
	require.NoError(t, err)

	got, err := store.Get(context.Background(), "monorepo/main")
	require.NoError(t, err)
	assert.Equal(t, entity.QueueConfig{
		Name:                            "monorepo/main",
		MaxConcurrent:                   2,
		GateWaitDelayMs:                 5000,
		MinimumBuildAdmissionIntervalMs: 3_600_000,
		FailureCooldownMs:               900_000,
	}, got)

	_, err = store.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, queueconfig.ErrNotFound)

	first, err := store.List(context.Background())
	require.NoError(t, err)
	first[0].Name = "changed"
	second, err := store.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "monorepo/main", second[0].Name)
}

func TestNewStoreMissingFile(t *testing.T) {
	_, err := NewStore(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
	assert.False(t, errors.Is(err, queueconfig.ErrNotFound))
}
