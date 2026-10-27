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

func testIssueLogStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()

	bound, err := factory.For(storage.Config{QueueName: "issue-history/main"})
	require.NoError(t, err)
	store := bound.GetIssueLogStore()
	opened := entity.IssueLog{TaskID: "validation", UnitID: "unit",
		Event: entity.IssueEventOpened, BuildID: "build/1", TimestampMs: 1000}

	t.Run("history survives closure and reopening", func(t *testing.T) {
		issue := entity.IssueState{TaskID: opened.TaskID, UnitID: opened.UnitID, FirstSeenBuildID: opened.BuildID, Status: entity.IssueStatusOpen, Version: 1}
		require.NoError(t, bound.GetIssueStateStore().Create(ctx, issue))
		require.NoError(t, bound.GetBuildIssueStore().Create(ctx, opened.BuildID, entity.BuildIssue{TaskID: opened.TaskID, UnitID: opened.UnitID}))
		require.NoError(t, store.Create(ctx, opened))

		resolved := opened
		resolved.Event, resolved.BuildID, resolved.TimestampMs = entity.IssueEventResolved, "build/2", 2000
		require.NoError(t, store.Create(ctx, resolved))
		issue.Status = entity.IssueStatusClosed
		require.NoError(t, bound.GetIssueStateStore().Update(ctx, issue, 1, 2))
		retained, err := bound.GetIssueStateStore().Get(ctx, issue.TaskID, issue.UnitID)
		require.NoError(t, err)
		require.Equal(t, entity.IssueStatusClosed, retained.Status)

		reopened := opened
		reopened.BuildID, reopened.TimestampMs = "build/3", 3000
		issue.FirstSeenBuildID = reopened.BuildID
		issue.Status = entity.IssueStatusOpen
		require.NoError(t, bound.GetIssueStateStore().Update(ctx, issue, 2, 3))
		require.NoError(t, store.Create(ctx, reopened))

		for _, want := range []entity.IssueLog{opened, resolved, reopened} {
			got, err := store.Get(ctx, want.TaskID, want.UnitID, want.BuildID, want.Event)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		_, err = store.Get(ctx, reopened.TaskID, reopened.UnitID, reopened.BuildID, entity.IssueEventResolved)
		require.ErrorIs(t, err, storage.ErrNotFound)
		_, err = bound.GetBuildIssueStore().Get(ctx, opened.BuildID, opened.TaskID, opened.UnitID)
		require.NoError(t, err)
	})

	t.Run("duplicate preserves original timestamp", func(t *testing.T) {
		duplicate := opened
		duplicate.TimestampMs = 4000
		require.ErrorIs(t, store.Create(ctx, duplicate), storage.ErrAlreadyExists)
		got, err := store.Get(ctx, opened.TaskID, opened.UnitID, opened.BuildID, opened.Event)
		require.NoError(t, err)
		require.Equal(t, opened, got)
	})

	t.Run("event distinguishes transitions within one build", func(t *testing.T) {
		for _, event := range []entity.IssueEvent{entity.IssueEventOpened, entity.IssueEventResolved} {
			log := opened
			log.UnitID, log.Event = "same-build", event
			require.NoError(t, store.Create(ctx, log))

			got, err := store.Get(ctx, log.TaskID, log.UnitID, log.BuildID, log.Event)
			require.NoError(t, err)
			require.Equal(t, log, got)
		}
	})

	t.Run("queue and byte-exact identity isolation", func(t *testing.T) {
		other, err := factory.For(storage.Config{QueueName: "Issue-history/main"})
		require.NoError(t, err)
		_, err = other.GetIssueLogStore().Get(ctx, opened.TaskID, opened.UnitID, opened.BuildID, opened.Event)
		require.ErrorIs(t, err, storage.ErrNotFound)
		otherLog := opened
		otherLog.TimestampMs++
		require.NoError(t, other.GetIssueLogStore().Create(ctx, otherLog))
		got, err := other.GetIssueLogStore().Get(ctx, opened.TaskID, opened.UnitID, opened.BuildID, opened.Event)
		require.NoError(t, err)
		require.Equal(t, otherLog, got)

		for _, tt := range []struct {
			name                    string
			taskID, unitID, buildID string
		}{
			{"task case", "Validation", "unit", "build/1"},
			{"task trailing space", "validation ", "unit", "build/1"},
			{"unit case", "validation", "Unit", "build/1"},
			{"unit trailing space", "validation", "unit ", "build/1"},
			{"task-wide issue", "validation", "", "build/1"},
			{"build case", "validation", "unit", "Build/1"},
			{"build trailing space", "validation", "unit", "build/1 "},
			{"embedded separator in task", "a\x00b", "c", "build/1"},
			{"embedded separator in unit", "a", "b\x00c", "build/1"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				log := opened
				log.TaskID, log.UnitID, log.BuildID = tt.taskID, tt.unitID, tt.buildID
				require.NoError(t, store.Create(ctx, log))
				got, err := store.Get(ctx, log.TaskID, log.UnitID, log.BuildID, log.Event)
				require.NoError(t, err)
				require.Equal(t, log, got)
			})
		}
		got, err = store.Get(ctx, opened.TaskID, opened.UnitID, opened.BuildID, opened.Event)
		require.NoError(t, err)
		require.Equal(t, opened, got)
	})

	t.Run("concurrent replay retains one event", func(t *testing.T) {
		log := opened
		log.UnitID = "concurrent"
		results := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() { results <- store.Create(ctx, log) }()
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
		got, err := store.Get(ctx, log.TaskID, log.UnitID, log.BuildID, log.Event)
		require.NoError(t, err)
		require.Equal(t, log, got)
	})

	t.Run("binary identifier limits", func(t *testing.T) {
		maxBound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		maxStore := maxBound.GetIssueLogStore()
		log := entity.IssueLog{TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256),
			Event:   entity.IssueEventResolved,
			BuildID: strings.Repeat("😀", 255), TimestampMs: 1000}
		require.NoError(t, maxStore.Create(ctx, log))
		got, err := maxStore.Get(ctx, log.TaskID, log.UnitID, log.BuildID, log.Event)
		require.NoError(t, err)
		require.Equal(t, log, got)

		oversized := log
		oversized.BuildID += "x"
		require.Error(t, maxStore.Create(ctx, oversized))
		_, err = maxStore.Get(ctx, oversized.TaskID, oversized.UnitID, oversized.BuildID, oversized.Event)
		require.Error(t, err)
		oversized = log
		oversized.UnitID += "x"
		require.Error(t, maxStore.Create(ctx, oversized))
		_, err = maxStore.Get(ctx, oversized.TaskID, oversized.UnitID, oversized.BuildID, oversized.Event)
		require.Error(t, err)

		got, err = maxStore.Get(ctx, log.TaskID, log.UnitID, log.BuildID, log.Event)
		require.NoError(t, err)
		require.Equal(t, log, got)
	})
}
