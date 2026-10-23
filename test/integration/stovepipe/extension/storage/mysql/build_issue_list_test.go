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

func testBuildIssueList(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()

	bound, err := factory.For(storage.Config{QueueName: "build-issue-list/main"})
	require.NoError(t, err)
	store := bound.GetBuildIssueStore()
	empty, err := store.List(ctx, "build/1", storage.BuildIssueRange{Limit: 2})
	require.NoError(t, err)
	require.Empty(t, empty)

	expected := []entity.BuildIssue{
		{TaskID: "A"},
		{TaskID: "a"},
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
	for i := len(expected) - 1; i >= 0; i-- {
		require.NoError(t, store.Create(ctx, "build/1", expected[i]))
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].TaskID != expected[j].TaskID {
			return expected[i].TaskID < expected[j].TaskID
		}
		return expected[i].UnitID < expected[j].UnitID
	})

	t.Run("queue and build isolation", func(t *testing.T) {
		otherQueue, err := factory.For(storage.Config{QueueName: "Build-issue-list/main"})
		require.NoError(t, err)
		otherStore := otherQueue.GetBuildIssueStore()
		empty, err := otherStore.List(ctx, "build/1", storage.BuildIssueRange{Limit: 2})
		require.NoError(t, err)
		require.Empty(t, empty)
		require.NoError(t, otherStore.Create(ctx, "build/1", expected[0]))
		got, err := otherStore.List(ctx, "build/1", storage.BuildIssueRange{Limit: 2})
		require.NoError(t, err)
		require.Equal(t, expected[:1], got)

		for _, buildID := range []string{"build/2", "Build/1", "build/1 "} {
			require.NoError(t, store.Create(ctx, buildID, expected[0]))
			got, err := store.List(ctx, buildID, storage.BuildIssueRange{Limit: 2})
			require.NoError(t, err)
			require.Equal(t, expected[:1], got)
		}
	})

	for _, limit := range []int{1, 2, 3, len(expected), len(expected) + 1} {
		t.Run(fmt.Sprintf("page size %d", limit), func(t *testing.T) {
			bounds := storage.BuildIssueRange{Limit: limit}
			var got []entity.BuildIssue
			for {
				page, err := store.List(ctx, "build/1", bounds)
				require.NoError(t, err)
				require.LessOrEqual(t, len(page), limit)
				if len(page) == 0 {
					break
				}
				got = append(got, page...)
				require.LessOrEqual(t, len(got), len(expected))
				last := page[len(page)-1]
				bounds.After = storage.BuildIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
			}
			require.Equal(t, expected, got)
		})
	}

	t.Run("cursor need not identify an observation", func(t *testing.T) {
		cursor := storage.BuildIssueCursor{TaskID: "a", UnitID: "missing"}
		var want []entity.BuildIssue
		for _, issue := range expected {
			if issue.TaskID > cursor.TaskID || (issue.TaskID == cursor.TaskID && issue.UnitID > cursor.UnitID) {
				want = append(want, issue)
			}
		}
		got, err := store.List(ctx, "build/1", storage.BuildIssueRange{After: cursor, Limit: len(expected)})
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("identifier boundaries", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		maxStore := bound.GetBuildIssueStore()
		buildID := strings.Repeat("😀", 255)
		issue := entity.BuildIssue{TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256)}
		require.NoError(t, maxStore.Create(ctx, buildID, issue))
		got, err := maxStore.List(ctx, buildID, storage.BuildIssueRange{Limit: 1})
		require.NoError(t, err)
		require.Equal(t, []entity.BuildIssue{issue}, got)

		bounds := storage.BuildIssueRange{Limit: 1,
			After: storage.BuildIssueCursor{TaskID: issue.TaskID, UnitID: issue.UnitID}}
		got, err = maxStore.List(ctx, buildID, bounds)
		require.NoError(t, err)
		require.Empty(t, got)
		_, err = maxStore.List(ctx, buildID+"x", bounds)
		require.Error(t, err)
		oversized := bounds
		oversized.After.TaskID += "x"
		_, err = maxStore.List(ctx, buildID, oversized)
		require.Error(t, err)
		oversized = bounds
		oversized.After.UnitID += "x"
		_, err = maxStore.List(ctx, buildID, oversized)
		require.Error(t, err)
	})

	t.Run("missing identifiers are rejected", func(t *testing.T) {
		for _, tt := range []struct{ buildID, taskID string }{
			{taskID: "task"}, {buildID: "build/invalid"},
		} {
			issue := entity.BuildIssue{TaskID: tt.taskID, UnitID: "unit"}
			require.Error(t, store.Create(ctx, tt.buildID, issue))
			_, err := store.Get(ctx, tt.buildID, tt.taskID, "unit")
			require.Error(t, err)
		}
		_, err := store.List(ctx, "", storage.BuildIssueRange{Limit: 1})
		require.Error(t, err)
	})
}
