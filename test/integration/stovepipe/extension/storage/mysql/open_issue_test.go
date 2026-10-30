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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testOpenIssueStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	bound, err := factory.For(storage.Config{QueueName: "open-issue/main"})
	require.NoError(t, err)
	store := bound.GetOpenIssueStore()
	first := entity.OpenIssue{TaskID: "task", UnitID: "unit", FirstSeenBuildID: "build/1"}
	_, err = store.Get(ctx, first.TaskID, first.UnitID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	changed, err := store.Delete(ctx, first)
	require.NoError(t, err)
	require.False(t, changed)
	changed, err = store.Replace(ctx, first, "missing")
	require.NoError(t, err)
	require.False(t, changed)
	require.NoError(t, store.Create(ctx, first))
	require.ErrorIs(t, store.Create(ctx, first), storage.ErrAlreadyExists)
	got, err := store.Get(ctx, first.TaskID, first.UnitID)
	require.NoError(t, err)
	require.Equal(t, first, got)

	second := first
	second.FirstSeenBuildID = "build/2"
	require.ErrorIs(t, store.Create(ctx, second), storage.ErrAlreadyExists)
	changed, err = store.Replace(ctx, second, first.FirstSeenBuildID)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = store.Replace(ctx, second, second.FirstSeenBuildID)
	require.NoError(t, err)
	require.False(t, changed)
	changed, err = store.Delete(ctx, first)
	require.NoError(t, err)
	require.False(t, changed)
	reopened := first
	reopened.FirstSeenBuildID = "build/3"
	changed, err = store.Replace(ctx, reopened, first.FirstSeenBuildID)
	require.NoError(t, err)
	require.False(t, changed)
	got, err = store.Get(ctx, first.TaskID, first.UnitID)
	require.NoError(t, err)
	require.Equal(t, second, got)

	changed, err = store.Delete(ctx, second)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = store.Delete(ctx, second)
	require.NoError(t, err)
	require.False(t, changed)
	require.NoError(t, store.Create(ctx, reopened))
	for _, staleOpening := range []string{first.FirstSeenBuildID, second.FirstSeenBuildID, "Build/3", "build/3 "} {
		stale := first
		stale.FirstSeenBuildID = staleOpening
		changed, err = store.Delete(ctx, stale)
		require.NoError(t, err)
		require.False(t, changed)
		changed, err = store.Replace(ctx, first, staleOpening)
		require.NoError(t, err)
		require.False(t, changed)
	}
	got, err = store.Get(ctx, first.TaskID, first.UnitID)
	require.NoError(t, err)
	require.Equal(t, reopened, got)

	t.Run("queue isolation", func(t *testing.T) {
		other, err := factory.For(storage.Config{QueueName: "Open-issue/main"})
		require.NoError(t, err)
		otherStore := other.GetOpenIssueStore()
		_, err = otherStore.Get(ctx, first.TaskID, first.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.NoError(t, otherStore.Create(ctx, first))
		changed, err := otherStore.Replace(ctx, second, first.FirstSeenBuildID)
		require.NoError(t, err)
		require.True(t, changed)
		changed, err = otherStore.Delete(ctx, second)
		require.NoError(t, err)
		require.True(t, changed)
		got, err := store.Get(ctx, first.TaskID, first.UnitID)
		require.NoError(t, err)
		require.Equal(t, reopened, got)
	})

	t.Run("bounded bytewise listing", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: "open-issue/list"})
		require.NoError(t, err)
		store := bound.GetOpenIssueStore()
		empty, err := store.List(ctx, storage.OpenIssueRange{Limit: 1})
		require.NoError(t, err)
		require.Empty(t, empty)
		expected := []entity.OpenIssue{
			{TaskID: "A"}, {TaskID: "a"},
			{TaskID: "a", UnitID: "\x00"}, {TaskID: "a", UnitID: "Unit"},
			{TaskID: "a", UnitID: "unit"}, {TaskID: "a", UnitID: "unit "},
			{TaskID: "a ", UnitID: "unit"}, {TaskID: "a\x00b", UnitID: "c"},
			{TaskID: "a", UnitID: "b\x00c"}, {TaskID: "é", UnitID: "unit"},
		}
		for i := range expected {
			expected[i].FirstSeenBuildID = fmt.Sprintf("build/%d", i)
		}
		for i := len(expected) - 1; i >= 0; i-- {
			require.NoError(t, store.Create(ctx, expected[i]))
		}
		sort.Slice(expected, func(i, j int) bool {
			if expected[i].TaskID != expected[j].TaskID {
				return expected[i].TaskID < expected[j].TaskID
			}
			return expected[i].UnitID < expected[j].UnitID
		})
		for _, limit := range []int{1, 3, len(expected) + 1} {
			var all []entity.OpenIssue
			bounds := storage.OpenIssueRange{Limit: limit}
			for {
				batch, err := store.List(ctx, bounds)
				require.NoError(t, err)
				require.LessOrEqual(t, len(batch), limit)
				if len(batch) == 0 {
					break
				}
				all = append(all, batch...)
				require.LessOrEqual(t, len(all), len(expected))
				last := batch[len(batch)-1]
				bounds.After = storage.OpenIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
			}
			require.Equal(t, expected, all)
		}
		changed, err := store.Delete(ctx, expected[0])
		require.NoError(t, err)
		require.True(t, changed)
		rest, err := store.List(ctx, storage.OpenIssueRange{Limit: len(expected), After: storage.OpenIssueCursor{TaskID: expected[0].TaskID, UnitID: expected[0].UnitID}})
		require.NoError(t, err)
		require.Equal(t, expected[1:], rest)
	})

	t.Run("identifier boundaries", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		store := bound.GetOpenIssueStore()
		issue := entity.OpenIssue{TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256), FirstSeenBuildID: strings.Repeat("😀", 255)}
		require.NoError(t, store.Create(ctx, issue))
		got, err := store.Get(ctx, issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, issue, got)
		batch, err := store.List(ctx, storage.OpenIssueRange{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, []entity.OpenIssue{issue}, batch)
		issue.UnitID += "x"
		require.Error(t, store.Create(ctx, issue))
	})
}
