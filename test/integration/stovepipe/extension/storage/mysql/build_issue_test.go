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
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testBuildIssueStore(t *testing.T, ctx context.Context, db *sql.DB, factory storage.Factory) {
	t.Helper()

	bound, err := factory.For(storage.Config{QueueName: "issues/main"})
	require.NoError(t, err)
	store := bound.GetBuildIssueStore()

	fixtures := []entity.BuildIssue{
		{TaskID: "validation", UnitID: "unit"},
		{TaskID: "validation", UnitID: "Unit"},
		{TaskID: "validation", UnitID: "unit "},
		{TaskID: "Validation", UnitID: "unit"},
		{TaskID: "validation ", UnitID: "unit"},
		{TaskID: "validation"},
		{TaskID: "other", UnitID: "unit"},
		{TaskID: "a\x00b", UnitID: "c"},
		{TaskID: "a", UnitID: "b\x00c"},
		{TaskID: strings.Repeat("検証", 42), UnitID: strings.Repeat("対象/", 146)},
	}

	for _, issue := range fixtures {
		require.NoError(t, store.Create(ctx, "build", issue))
		require.ErrorIs(t, store.Create(ctx, "build", issue), storage.ErrAlreadyExists)

		got, err := store.Get(ctx, "build", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, issue, got)
	}

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM build_issue WHERE queue = ? AND build_id = ?", "issues/main", "build").Scan(&count))
	require.Equal(t, len(fixtures), count)

	issue := fixtures[0]
	for _, buildID := range []string{"next-build", "Build", "build "} {
		_, err := store.Get(ctx, buildID, issue.TaskID, issue.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)

		require.NoError(t, store.Create(ctx, buildID, issue))

		got, err := store.Get(ctx, buildID, issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, issue, got)
	}

	for _, queue := range []string{"issues/other", "Issues/main", "issues/main "} {
		other, err := factory.For(storage.Config{QueueName: queue})
		require.NoError(t, err)
		_, err = other.GetBuildIssueStore().Get(ctx, "build", issue.TaskID, issue.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)

		require.NoError(t, other.GetBuildIssueStore().Create(ctx, "build", issue))

		got, err := other.GetBuildIssueStore().Get(ctx, "build", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, issue, got)
	}

	for _, missing := range []entity.BuildIssue{
		{TaskID: "missing", UnitID: "unit"},
		{TaskID: "validation", UnitID: "missing"},
		{TaskID: "other"},
	} {
		_, err := store.Get(ctx, "build", missing.TaskID, missing.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)
	}

	t.Run("full primary key at byte limits", func(t *testing.T) {
		maxQueue := strings.Repeat("q", 255)
		maxBuildID := strings.Repeat("😀", 255)
		maxIssue := entity.BuildIssue{TaskID: strings.Repeat("é", 127) + "t", UnitID: strings.Repeat("😀", 256)}

		maxBound, err := factory.For(storage.Config{QueueName: maxQueue})
		require.NoError(t, err)
		maxStore := maxBound.GetBuildIssueStore()

		require.NoError(t, maxStore.Create(ctx, maxBuildID, maxIssue))
		require.ErrorIs(t, maxStore.Create(ctx, maxBuildID, maxIssue), storage.ErrAlreadyExists)

		got, err := maxStore.Get(ctx, maxBuildID, maxIssue.TaskID, maxIssue.UnitID)
		require.NoError(t, err)
		require.Equal(t, maxIssue, got)

		for _, tt := range []struct {
			name    string
			queue   string
			buildID string
			issue   entity.BuildIssue
		}{
			{name: "queue", queue: maxQueue + "q", buildID: maxBuildID, issue: maxIssue},
			{name: "build ID", queue: maxQueue, buildID: maxBuildID + "b", issue: maxIssue},
			{name: "task ID", queue: maxQueue, buildID: maxBuildID, issue: entity.BuildIssue{TaskID: maxIssue.TaskID + "t", UnitID: maxIssue.UnitID}},
			{name: "unit ID", queue: maxQueue, buildID: maxBuildID, issue: entity.BuildIssue{TaskID: maxIssue.TaskID, UnitID: maxIssue.UnitID + "u"}},
		} {
			t.Run("reject oversized "+tt.name, func(t *testing.T) {
				invalidBound, err := factory.For(storage.Config{QueueName: tt.queue})
				require.NoError(t, err)
				invalidStore := invalidBound.GetBuildIssueStore()

				err = invalidStore.Create(ctx, tt.buildID, tt.issue)
				require.Error(t, err)
				require.NotErrorIs(t, err, storage.ErrAlreadyExists)

				_, err = invalidStore.Get(ctx, tt.buildID, tt.issue.TaskID, tt.issue.UnitID)
				require.Error(t, err)
				require.NotErrorIs(t, err, storage.ErrNotFound)
			})
		}

		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM build_issue WHERE queue = ?", maxQueue).Scan(&count))
		require.Equal(t, 1, count)

		got, err = maxStore.Get(ctx, maxBuildID, maxIssue.TaskID, maxIssue.UnitID)
		require.NoError(t, err)
		require.Equal(t, maxIssue, got)
	})

	t.Run("accumulate individual observations", func(t *testing.T) {
		for i := 0; i < 128; i++ {
			issue := entity.BuildIssue{TaskID: "validation", UnitID: fmt.Sprintf("unit-%d", i)}
			require.NoError(t, store.Create(ctx, "many-issues", issue))

			got, err := store.Get(ctx, "many-issues", issue.TaskID, issue.UnitID)
			require.NoError(t, err)
			require.Equal(t, issue, got)
		}

		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM build_issue WHERE queue = ? AND build_id = ?", "issues/main", "many-issues").Scan(&count))
		require.Equal(t, 128, count)
	})

	t.Run("concurrent duplicate observations", func(t *testing.T) {
		const writers = 8
		results := make(chan error, writers)
		for i := 0; i < writers; i++ {
			go func() { results <- store.Create(ctx, "concurrent", issue) }()
		}

		created := 0
		for i := 0; i < writers; i++ {
			if err := <-results; err == nil {
				created++
			} else {
				require.ErrorIs(t, err, storage.ErrAlreadyExists)
			}
		}

		require.Equal(t, 1, created)

		got, err := store.Get(ctx, "concurrent", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, issue, got)
	})
}
