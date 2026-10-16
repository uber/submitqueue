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

package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"go.uber.org/zap"
)

func TestQueuePauseStoreFromEnv(t *testing.T) {
	pausedFile := filepath.Join(t.TempDir(), "pause.yaml")
	require.NoError(t, os.WriteFile(pausedFile, []byte("queues:\n  - name: q\n    freeze_processing: true\n"), 0o600))

	tests := []struct {
		name      string
		path      string
		wantErr   bool
		wantState entity.QueuePause
	}{
		{name: "unset pauses nothing", path: "", wantState: entity.QueuePause{}},
		{name: "file store reads the configured file", path: pausedFile, wantState: entity.QueuePause{FreezeProcessing: true}},
		{name: "unreadable path fails", path: filepath.Join(t.TempDir(), "absent.yaml"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(pathEnv, tt.path)

			store, err := QueuePauseStoreFromEnv(zap.NewNop())

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			got, err := store.Get(context.Background(), "q")
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, got)
		})
	}
}
