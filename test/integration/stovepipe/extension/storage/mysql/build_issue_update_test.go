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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testBuildIssueUpdate(t *testing.T, ctx context.Context, db *sql.DB, factory storage.Factory) {
	bound, err := factory.For(storage.Config{QueueName: "build-issue-update/main"})
	require.NoError(t, err)
	store := bound.GetBuildIssueStore()
	issue := entity.BuildIssue{TaskID: "Task ", UnitID: "Unit "}
	require.NoError(t, store.Create(ctx, "Build ", issue))
	updated := issue
	updated.FixID = "Fix/1 "
	require.NoError(t, store.Update(ctx, "Build ", updated, issue.Version, issue.Version+1))
	updated.Version = issue.Version + 1
	got, err := store.Get(ctx, "Build ", issue.TaskID, issue.UnitID)
	require.NoError(t, err)
	require.Equal(t, updated, got)
	require.ErrorIs(t, store.Update(ctx, "Build ", issue, 0, 1), storage.ErrVersionMismatch)
	require.ErrorIs(t, store.Create(ctx, "Build ", issue), storage.ErrAlreadyExists)

	open := entity.OpenIssue{TaskID: issue.TaskID, UnitID: issue.UnitID, FirstSeenBuildID: "Build "}
	require.NoError(t, bound.GetOpenIssueStore().Create(ctx, open))
	require.NoError(t, bound.GetOpenIssueStore().Delete(ctx, open))
	page, err := store.List(ctx, "Build ", storage.BuildIssueRange{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, []entity.BuildIssue{updated}, page)

	for _, buildID := range []string{"build ", "Build", "missing"} {
		require.ErrorIs(t, store.Update(ctx, buildID, updated, 0, 1), storage.ErrVersionMismatch)
	}
	other, err := factory.For(storage.Config{QueueName: "Build-issue-update/main"})
	require.NoError(t, err)
	require.ErrorIs(t, other.GetBuildIssueStore().Update(ctx, "Build ", updated, 0, 1), storage.ErrVersionMismatch)
	got, err = store.Get(ctx, "Build ", issue.TaskID, issue.UnitID)
	require.NoError(t, err)
	require.Equal(t, updated, got)

	t.Run("legacy rows start without a repair association", func(t *testing.T) {
		_, err := db.ExecContext(ctx, `INSERT INTO build_issue (queue, build_id, task_id, unit_id) VALUES (?, ?, ?, ?)`,
			"build-issue-update/main", "legacy", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		got, err := store.Get(ctx, "legacy", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, issue, got)
	})

	t.Run("create stores the entity", func(t *testing.T) {
		require.NoError(t, store.Create(ctx, "seeded", updated))
		got, err := store.Get(ctx, "seeded", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, updated, got)
	})

	t.Run("only one concurrent update succeeds", func(t *testing.T) {
		require.NoError(t, store.Create(ctx, "concurrent", issue))
		var wg sync.WaitGroup
		var winners atomic.Int32
		start := make(chan struct{})
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				candidate := issue
				candidate.FixID = fmt.Sprintf("fix/%d", i)
				err := store.Update(ctx, "concurrent", candidate, 0, 1)
				if err == nil {
					winners.Add(1)
				} else {
					assert.ErrorIs(t, err, storage.ErrVersionMismatch)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		require.Equal(t, int32(1), winners.Load())
		got, err := store.Get(ctx, "concurrent", issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.NotEmpty(t, got.FixID)
		require.Equal(t, int32(1), got.Version)
	})
}
