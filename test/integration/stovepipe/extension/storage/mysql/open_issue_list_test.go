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

func testOpenIssueList(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()

	bound, err := factory.For(storage.Config{QueueName: "open-issue-list/main"})
	require.NoError(t, err)
	store := bound.GetOpenIssueStore()
	empty, err := store.List(ctx, storage.OpenIssueRange{Limit: 2})
	require.NoError(t, err)
	require.Empty(t, empty)

	expected := []entity.OpenIssue{
		{TaskID: "A", UnitID: ""},
		{TaskID: "a", UnitID: ""},
		{TaskID: "a", UnitID: "\x00"},
		{TaskID: "a", UnitID: "Unit"},
		{TaskID: "a", UnitID: "unit"},
		{TaskID: "a", UnitID: "unit "},
		{TaskID: "a", UnitID: "unit\x00"},
		{TaskID: "a ", UnitID: "unit"},
		{TaskID: "a\x00b", UnitID: "c"},
		{TaskID: "a", UnitID: "b\x00c"},
		{TaskID: "é", UnitID: "unit"},
		{TaskID: "😀", UnitID: "unit"},
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

	other, err := factory.For(storage.Config{QueueName: "Open-issue-list/main"})
	require.NoError(t, err)
	require.NoError(t, other.GetOpenIssueStore().Create(ctx, entity.OpenIssue{
		TaskID: "other", UnitID: "unit", FirstSeenBuildID: "other-build",
	}))

	for _, limit := range []int{1, 2, 3, len(expected), len(expected) + 1} {
		t.Run(fmt.Sprintf("page size %d", limit), func(t *testing.T) {
			bounds := storage.OpenIssueRange{Limit: limit}
			var got []entity.OpenIssue
			for {
				page, err := store.List(ctx, bounds)
				require.NoError(t, err)
				require.LessOrEqual(t, len(page), limit)
				if len(page) == 0 {
					break
				}
				got = append(got, page...)
				require.LessOrEqual(t, len(got), len(expected))
				last := page[len(page)-1]
				bounds.After = storage.OpenIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
			}
			require.Equal(t, expected, got)
		})
	}

	t.Run("empty task IDs are rejected", func(t *testing.T) {
		for _, unitID := range []string{"", "unit"} {
			issue := entity.OpenIssue{UnitID: unitID, FirstSeenBuildID: "build/invalid"}
			require.Error(t, store.Create(ctx, issue))
			_, err := store.Get(ctx, "", unitID)
			require.Error(t, err)
			require.Error(t, store.Delete(ctx, issue))
		}

		_, err := store.List(ctx, storage.OpenIssueRange{
			After: storage.OpenIssueCursor{UnitID: "unit"}, Limit: 1,
		})
		require.Error(t, err)
	})

	t.Run("cursor row need not exist", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: "open-issue-list/deletion"})
		require.NoError(t, err)
		store := bound.GetOpenIssueStore()
		var expected []entity.OpenIssue
		for _, unit := range []string{"a", "b", "c"} {
			issue := entity.OpenIssue{TaskID: "task", UnitID: unit, FirstSeenBuildID: "build/1"}
			expected = append(expected, issue)
			require.NoError(t, store.Create(ctx, issue))
		}
		first, err := store.List(ctx, storage.OpenIssueRange{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, expected[:1], first)
		require.NoError(t, store.Delete(ctx, first[0]))

		bounds := storage.OpenIssueRange{Limit: 2,
			After: storage.OpenIssueCursor{TaskID: first[0].TaskID, UnitID: first[0].UnitID}}
		rest, err := store.List(ctx, bounds)
		require.NoError(t, err)
		require.Equal(t, expected[1:], rest)

		bounds.After.UnitID = "aa"
		rest, err = store.List(ctx, bounds)
		require.NoError(t, err)
		require.Equal(t, expected[1:], rest)
	})

	t.Run("identifier boundaries", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		store := bound.GetOpenIssueStore()
		issue := entity.OpenIssue{TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256),
			FirstSeenBuildID: strings.Repeat("😀", 255)}
		require.NoError(t, store.Create(ctx, issue))
		page, err := store.List(ctx, storage.OpenIssueRange{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, []entity.OpenIssue{issue}, page)

		bounds := storage.OpenIssueRange{Limit: 1,
			After: storage.OpenIssueCursor{TaskID: issue.TaskID, UnitID: issue.UnitID}}
		page, err = store.List(ctx, bounds)
		require.NoError(t, err)
		require.Empty(t, page)

		oversized := bounds
		oversized.After.TaskID += "x"
		_, err = store.List(ctx, oversized)
		require.Error(t, err)
		oversized = bounds
		oversized.After.UnitID += "x"
		_, err = store.List(ctx, oversized)
		require.Error(t, err)
	})
}
