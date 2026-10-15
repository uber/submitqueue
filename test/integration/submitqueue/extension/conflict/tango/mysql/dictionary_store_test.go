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

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func (s *StoreIntegrationSuite) TestDictionaryStore() {
	t := s.T()
	ctx := context.Background()
	storeA := s.forQueueDictionary("dict-queue-a")
	storeB := s.forQueueDictionary("dict-queue-b")

	// An ID with the high bit set must survive the unsigned column round trip.
	const highID = uint64(1) << 63
	label := func(id uint64, name string) tango.TargetLabel {
		return tango.TargetLabel{Queue: "dict-queue-a", ID: id, Label: name, CreatedAt: 1700000000000}
	}

	t.Run("round trip", func(t *testing.T) {
		require.NoError(t, storeA.Create(ctx, label(7, "//app/server:lib")))

		got, found, err := storeA.Get(ctx, 7)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, label(7, "//app/server:lib"), got)
	})

	t.Run("ID with the high bit set", func(t *testing.T) {
		require.NoError(t, storeA.Create(ctx, label(highID, "//high:bit")))

		got, found, err := storeA.Get(ctx, highID)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, label(highID, "//high:bit"), got)
	})

	t.Run("long label", func(t *testing.T) {
		long := "//" + strings.Repeat("p", 900) + ":target"
		require.NoError(t, storeA.Create(ctx, label(11, long)))

		got, found, err := storeA.Get(ctx, 11)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, label(11, long), got)
	})

	t.Run("a second label for a recorded ID is rejected and the first stays", func(t *testing.T) {
		require.NoError(t, storeA.Create(ctx, label(9, "//a:x")))
		require.ErrorIs(t, storeA.Create(ctx, label(9, "//b:y")), storage.ErrAlreadyExists)

		got, found, err := storeA.Get(ctx, 9)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, label(9, "//a:x"), got)
	})

	t.Run("repeated create returns ErrAlreadyExists", func(t *testing.T) {
		require.ErrorIs(t, storeA.Create(ctx, label(7, "//app/server:lib")), storage.ErrAlreadyExists)
	})

	t.Run("unknown ID is not found", func(t *testing.T) {
		_, found, err := storeA.Get(ctx, 12345)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("invisible through another queue", func(t *testing.T) {
		_, found, err := storeB.Get(ctx, 7)
		require.NoError(t, err)
		assert.False(t, found)
		require.Error(t, storeB.Create(ctx, label(7, "//app/server:lib")), "a mismatched-queue write must be rejected")
	})
}
