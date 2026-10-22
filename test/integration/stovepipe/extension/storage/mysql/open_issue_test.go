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

func testOpenIssueStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()

	bound, err := factory.For(storage.Config{QueueName: "open-issues/main"})
	require.NoError(t, err)
	store := bound.GetOpenIssueStore()
	issue := entity.OpenIssue{
		TaskID: "validation", UnitID: "unit", FirstSeenBuildID: "build/1",
	}

	require.NoError(t, store.Create(ctx, issue))
	require.ErrorIs(t, store.Create(ctx, issue), storage.ErrAlreadyExists)
	got, err := store.Get(ctx, issue.TaskID, issue.UnitID)
	require.NoError(t, err)
	require.Equal(t, issue, got)

	require.NoError(t, bound.GetBuildIssueStore().Create(ctx, "build/1", entity.BuildIssue{TaskID: issue.TaskID, UnitID: issue.UnitID}))
	require.NoError(t, store.Delete(ctx, got))
	require.NoError(t, store.Delete(ctx, got))
	_, err = store.Get(ctx, issue.TaskID, issue.UnitID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = bound.GetBuildIssueStore().Get(ctx, "build/1", issue.TaskID, issue.UnitID)
	require.NoError(t, err)

	t.Run("stale deletion cannot remove a reopened issue", func(t *testing.T) {
		reopened := issue
		reopened.FirstSeenBuildID = "build/3"
		require.NoError(t, store.Create(ctx, reopened))

		require.NoError(t, store.Delete(ctx, issue))
		require.NoError(t, store.Delete(ctx, issue))
		require.ErrorIs(t, store.Create(ctx, issue), storage.ErrAlreadyExists)
		got, err := store.Get(ctx, issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, reopened, got)
	})

	t.Run("queue and byte-exact key isolation", func(t *testing.T) {
		other, err := factory.For(storage.Config{QueueName: "Open-issues/main"})
		require.NoError(t, err)
		_, err = other.GetOpenIssueStore().Get(ctx, issue.TaskID, issue.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.NoError(t, other.GetOpenIssueStore().Delete(ctx, issue))
		require.NoError(t, other.GetOpenIssueStore().Create(ctx, issue))
		require.NoError(t, other.GetOpenIssueStore().Delete(ctx, issue))

		got, err := store.Get(ctx, issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, "build/3", got.FirstSeenBuildID)

		for _, key := range []entity.BuildIssue{
			{TaskID: "validation", UnitID: "Unit"},
			{TaskID: "validation", UnitID: "unit "},
			{TaskID: "Validation", UnitID: "unit"},
			{TaskID: "validation ", UnitID: "unit"},
			{TaskID: "validation"},
			{TaskID: "a\x00b", UnitID: "c"},
			{TaskID: "a", UnitID: "b\x00c"},
		} {
			value := issue
			value.TaskID, value.UnitID = key.TaskID, key.UnitID
			require.NoError(t, store.Create(ctx, value))

			got, err := store.Get(ctx, key.TaskID, key.UnitID)
			require.NoError(t, err)
			require.Equal(t, value, got)
			require.NoError(t, store.Delete(ctx, got))
		}
	})

	t.Run("only one concurrent opening wins", func(t *testing.T) {
		results := make(chan error, 2)
		for _, buildID := range []string{"build/4", "build/5"} {
			value := issue
			value.UnitID = "concurrent"
			value.FirstSeenBuildID = buildID
			go func() { results <- store.Create(ctx, value) }()
		}

		successes, duplicates := 0, 0
		for i := 0; i < 2; i++ {
			err := <-results
			switch {
			case err == nil:
				successes++
			case errors.Is(err, storage.ErrAlreadyExists):
				duplicates++
			default:
				require.NoError(t, err)
			}
		}
		require.Equal(t, 1, successes)
		require.Equal(t, 1, duplicates)

		got, err := store.Get(ctx, issue.TaskID, "concurrent")
		require.NoError(t, err)
		require.Contains(t, []string{"build/4", "build/5"}, got.FirstSeenBuildID)
	})

	t.Run("binary identifier limits", func(t *testing.T) {
		maxBound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		maxStore := maxBound.GetOpenIssueStore()
		value := entity.OpenIssue{
			TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256),
			FirstSeenBuildID: strings.Repeat("😀", 255),
		}
		require.NoError(t, maxStore.Create(ctx, value))

		got, err := maxStore.Get(ctx, value.TaskID, value.UnitID)
		require.NoError(t, err)
		require.Equal(t, value, got)

		oversized := value
		oversized.FirstSeenBuildID += "x"
		require.Error(t, maxStore.Create(ctx, oversized))
		require.Error(t, maxStore.Delete(ctx, oversized))
		oversized = value
		oversized.UnitID += "x"
		require.Error(t, maxStore.Create(ctx, oversized))
		require.Error(t, maxStore.Delete(ctx, oversized))
		_, err = maxStore.Get(ctx, oversized.TaskID, oversized.UnitID)
		require.Error(t, err)

		got, err = maxStore.Get(ctx, value.TaskID, value.UnitID)
		require.NoError(t, err)
		require.Equal(t, value, got)
	})
}
