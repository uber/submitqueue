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

func testIssueStateStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	bound, err := factory.For(storage.Config{QueueName: "issue-state/main"})
	require.NoError(t, err)
	store := bound.GetIssueStateStore()
	issue := entity.IssueState{TaskID: "validation", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/1", Version: 1}
	require.NoError(t, store.Create(ctx, issue))
	require.ErrorIs(t, store.Create(ctx, issue), storage.ErrAlreadyExists)
	got, err := store.Get(ctx, issue.TaskID, issue.UnitID)
	require.NoError(t, err)
	require.Equal(t, issue, got)

	closed := issue
	closed.Status = entity.IssueStatusClosed
	require.NoError(t, store.Update(ctx, closed, 1, 2))
	closed.Version = 2
	got, err = store.Get(ctx, issue.TaskID, issue.UnitID)
	require.NoError(t, err)
	require.Equal(t, closed, got)
	require.ErrorIs(t, store.Create(ctx, issue), storage.ErrAlreadyExists)
	require.ErrorIs(t, store.Update(ctx, issue, 1, 2), storage.ErrVersionMismatch)

	reopened := closed
	reopened.Status = entity.IssueStatusOpen
	reopened.FirstSeenBuildID = "build/3"
	require.NoError(t, store.Update(ctx, reopened, 2, 3))
	reopened.Version = 3
	require.ErrorIs(t, store.Update(ctx, closed, 2, 3), storage.ErrVersionMismatch)
	got, err = store.Get(ctx, issue.TaskID, issue.UnitID)
	require.NoError(t, err)
	require.Equal(t, reopened, got)

	t.Run("queue and byte-exact key isolation", func(t *testing.T) {
		other, err := factory.For(storage.Config{QueueName: "Issue-state/main"})
		require.NoError(t, err)
		_, err = other.GetIssueStateStore().Get(ctx, issue.TaskID, issue.UnitID)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.NoError(t, other.GetIssueStateStore().Create(ctx, issue))
		for _, key := range []entity.BuildIssue{{TaskID: "validation", UnitID: "Unit"}, {TaskID: "validation", UnitID: "unit "}, {TaskID: "Validation", UnitID: "unit"}, {TaskID: "validation"}, {TaskID: "a\x00b", UnitID: "c"}, {TaskID: "a", UnitID: "b\x00c"}} {
			value := issue
			value.TaskID, value.UnitID = key.TaskID, key.UnitID
			require.NoError(t, store.Create(ctx, value))
			got, err := store.Get(ctx, key.TaskID, key.UnitID)
			require.NoError(t, err)
			require.Equal(t, value, got)
		}
	})
	t.Run("only one competing version wins", func(t *testing.T) {
		results := make(chan error, 2)
		for _, status := range []entity.IssueStatus{entity.IssueStatusOpen, entity.IssueStatusClosed} {
			value := reopened
			value.Status = status
			go func() { results <- store.Update(ctx, value, 3, 4) }()
		}
		successes, conflicts := 0, 0
		for i := 0; i < 2; i++ {
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
		value := issue
		value.TaskID = strings.Repeat("t", 255)
		value.UnitID = strings.Repeat("😀", 256)
		value.FirstSeenBuildID = strings.Repeat("😀", 255)
		store := bound.GetIssueStateStore()
		require.NoError(t, store.Create(ctx, value))
		got, err := store.Get(ctx, value.TaskID, value.UnitID)
		require.NoError(t, err)
		require.Equal(t, value, got)
		value.UnitID += "x"
		require.Error(t, store.Create(ctx, value))
		require.Error(t, store.Update(ctx, value, 1, 2))
	})
}
