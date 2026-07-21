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

package defaultconfig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
)

func TestStore_Get(t *testing.T) {
	store := NewStore()

	t.Run("returns defaults for named queue", func(t *testing.T) {
		cfg, err := store.Get(context.Background(), "monorepo/main")
		require.NoError(t, err)
		assert.Equal(t, "monorepo/main", cfg.Name)
		assert.Equal(t, int32(1), cfg.MaxConcurrent)
		assert.Equal(t, int64(5000), cfg.GateWaitDelayMs)
	})

	t.Run("empty name is not found", func(t *testing.T) {
		_, err := store.Get(context.Background(), "")
		require.ErrorIs(t, err, errs.ErrNotFound)
	})
}

func TestStore_List(t *testing.T) {
	store := NewStore()

	cfg, err := store.List(context.Background())
	require.NoError(t, err)
	assert.Empty(t, cfg)
}
