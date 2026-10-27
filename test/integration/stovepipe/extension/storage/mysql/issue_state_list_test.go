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

func testIssueStateList(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()

	bound, err := factory.For(storage.Config{QueueName: "issue-state-list/main"})
	require.NoError(t, err)
	store := bound.GetIssueStateStore()
	empty, err := store.List(ctx, storage.IssueStateRange{Limit: 2})
	require.NoError(t, err)
	require.Empty(t, empty)

	expected := []entity.IssueState{
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
		expected[i].LastProcessedBuildID = expected[i].FirstSeenBuildID
		expected[i].LastChangedBuildID = expected[i].FirstSeenBuildID
		expected[i].ChangedAtMs = 1000
		expected[i].Version = 1
		expected[i].Status = entity.IssueStatusOpen
		if i%2 == 0 {
			expected[i].Status = entity.IssueStatusClosed
		}
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

	other, err := factory.For(storage.Config{QueueName: "Issue-state-list/main"})
	require.NoError(t, err)
	require.NoError(t, other.GetIssueStateStore().Create(ctx, entity.IssueState{
		TaskID: "other", UnitID: "unit", FirstSeenBuildID: "other-build", Status: entity.IssueStatusOpen, Version: 1,
		LastProcessedBuildID: "other-build", LastChangedBuildID: "other-build", ChangedAtMs: 1000}))

	for _, limit := range []int{1, 2, 3, len(expected), len(expected) + 1} {
		t.Run(fmt.Sprintf("page size %d", limit), func(t *testing.T) {
			bounds := storage.IssueStateRange{Limit: limit}
			var got []entity.IssueState
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
				bounds.After = storage.IssueStateCursor{TaskID: last.TaskID, UnitID: last.UnitID}
			}
			require.Equal(t, expected, got)
		})
	}

	t.Run("empty task IDs are rejected", func(t *testing.T) {
		for _, unitID := range []string{"", "unit"} {
			issue := entity.IssueState{UnitID: unitID, FirstSeenBuildID: "build/invalid", LastProcessedBuildID: "build/invalid", LastChangedBuildID: "build/invalid", ChangedAtMs: 1000}
			require.Error(t, store.Create(ctx, issue))
			_, err := store.Get(ctx, "", unitID)
			require.Error(t, err)
			require.Error(t, store.Update(ctx, issue, 1, 2))
		}

		_, err := store.List(ctx, storage.IssueStateRange{
			After: storage.IssueStateCursor{UnitID: "unit"}, Limit: 1,
		})
		require.Error(t, err)
	})

	t.Run("closed cursor and absent boundaries remain traversable", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: "issue-state-list/deletion"})
		require.NoError(t, err)
		store := bound.GetIssueStateStore()
		var expected []entity.IssueState
		for _, unit := range []string{"a", "b", "c"} {
			issue := entity.IssueState{TaskID: "task", UnitID: unit, FirstSeenBuildID: "build/1", Status: entity.IssueStatusOpen, Version: 1, LastProcessedBuildID: "build/1", LastChangedBuildID: "build/1", ChangedAtMs: 1000}
			expected = append(expected, issue)
			require.NoError(t, store.Create(ctx, issue))
		}
		first, err := store.List(ctx, storage.IssueStateRange{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, expected[:1], first)
		closed := first[0]
		closed.Status = entity.IssueStatusClosed
		require.NoError(t, store.Update(ctx, closed, 1, 2))

		bounds := storage.IssueStateRange{Limit: 2,
			After: storage.IssueStateCursor{TaskID: first[0].TaskID, UnitID: first[0].UnitID}}
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
		store := bound.GetIssueStateStore()
		issue := entity.IssueState{TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256),
			FirstSeenBuildID: strings.Repeat("😀", 255), Status: entity.IssueStatusOpen, Version: 1, LastProcessedBuildID: strings.Repeat("😀", 255), LastChangedBuildID: strings.Repeat("😀", 255), ChangedAtMs: 1000}
		require.NoError(t, store.Create(ctx, issue))
		page, err := store.List(ctx, storage.IssueStateRange{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, []entity.IssueState{issue}, page)

		bounds := storage.IssueStateRange{Limit: 1,
			After: storage.IssueStateCursor{TaskID: issue.TaskID, UnitID: issue.UnitID}}
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
