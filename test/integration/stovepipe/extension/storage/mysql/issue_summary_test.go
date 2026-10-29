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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testIssueSummaryStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	bound, err := factory.For(storage.Config{QueueName: "issue-summary/main"})
	require.NoError(t, err)
	store := bound.GetIssueSummaryStore()
	summary := entity.IssueSummary{TaskID: "validation", UnitID: "unit", Status: entity.IssueStatusOpen,
		FirstSeenBuildID: "build/1", RequestID: "1", BuildID: "build/1", ChangedAtMs: 1000, Version: 1}

	_, err = store.Get(ctx, summary.TaskID, summary.UnitID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.ErrorIs(t, store.Update(ctx, summary, 1, 2), storage.ErrVersionMismatch)
	require.NoError(t, store.Create(ctx, summary))
	require.ErrorIs(t, store.Create(ctx, summary), storage.ErrAlreadyExists)
	got, err := store.Get(ctx, summary.TaskID, summary.UnitID)
	require.NoError(t, err)
	require.Equal(t, summary, got)

	closed := summary
	closed.Status, closed.RequestID, closed.BuildID = entity.IssueStatusClosed, "2", "build/2"
	closed.ChangedAtMs = 2000
	require.NoError(t, store.Update(ctx, closed, 1, 2))
	closed.Version = 2
	got, err = store.Get(ctx, summary.TaskID, summary.UnitID)
	require.NoError(t, err)
	require.Equal(t, closed, got)
	require.ErrorIs(t, store.Create(ctx, summary), storage.ErrAlreadyExists)
	require.ErrorIs(t, store.Update(ctx, summary, 1, 2), storage.ErrVersionMismatch)

	reopened := closed
	reopened.Status, reopened.RequestID, reopened.BuildID = entity.IssueStatusOpen, "3", "build/3"
	reopened.FirstSeenBuildID, reopened.ChangedAtMs = "build/3", 3000
	require.NoError(t, store.Update(ctx, reopened, 2, 3))
	reopened.Version = 3
	require.ErrorIs(t, store.Update(ctx, closed, 2, 3), storage.ErrVersionMismatch)
	got, err = store.Get(ctx, summary.TaskID, summary.UnitID)
	require.NoError(t, err)
	require.Equal(t, reopened, got)

	t.Run("byte exact keys and queue isolation", func(t *testing.T) {
		other, err := factory.For(storage.Config{QueueName: "Issue-summary/main"})
		require.NoError(t, err)
		_, err = other.GetIssueSummaryStore().Get(ctx, summary.TaskID, summary.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.NoError(t, other.GetIssueSummaryStore().Create(ctx, summary))
		require.NoError(t, other.GetIssueSummaryStore().Update(ctx, closed, 1, 2))
		got, err := store.Get(ctx, summary.TaskID, summary.UnitID)
		require.NoError(t, err)
		require.Equal(t, reopened, got)

		for _, key := range []entity.BuildIssue{
			{TaskID: "validation", UnitID: "Unit"},
			{TaskID: "validation", UnitID: "unit "},
			{TaskID: "Validation", UnitID: "unit"},
			{TaskID: "validation"},
			{TaskID: "a\x00b", UnitID: "c"},
			{TaskID: "a", UnitID: "b\x00c"},
		} {
			value := summary
			value.TaskID, value.UnitID = key.TaskID, key.UnitID
			require.NoError(t, store.Create(ctx, value))
			got, err := store.Get(ctx, key.TaskID, key.UnitID)
			require.NoError(t, err)
			require.Equal(t, value, got)
		}
	})

	t.Run("one competing replacement wins", func(t *testing.T) {
		results := make(chan error, 2)
		for _, status := range []entity.IssueStatus{entity.IssueStatusOpen, entity.IssueStatusClosed} {
			replacement := reopened
			replacement.Status = status
			go func() { results <- store.Update(ctx, replacement, 3, 4) }()
		}
		successes, conflicts := 0, 0
		for range 2 {
			err := <-results
			switch {
			case err == nil:
				successes++
			case errors.Is(err, storage.ErrVersionMismatch):
				conflicts++
			default:
				require.NoError(t, err)
			}
		}
		require.Equal(t, 1, successes)
		require.Equal(t, 1, conflicts)
	})

	t.Run("identifier boundaries", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		value := summary
		value.TaskID, value.UnitID = strings.Repeat("t", 255), strings.Repeat("😀", 256)
		value.FirstSeenBuildID, value.BuildID = strings.Repeat("😀", 255), strings.Repeat("😀", 255)
		value.RequestID = "9223372036854775807"
		store := bound.GetIssueSummaryStore()
		require.NoError(t, store.Create(ctx, value))
		got, err := store.Get(ctx, value.TaskID, value.UnitID)
		require.NoError(t, err)
		require.Equal(t, value, got)
		value.UnitID += "x"
		require.Error(t, store.Create(ctx, value))
		require.Error(t, store.Update(ctx, value, 1, 2))
	})
}
