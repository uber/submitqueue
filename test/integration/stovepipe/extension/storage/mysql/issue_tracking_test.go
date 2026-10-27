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

func testIssueTrackingStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	bound, err := factory.For(storage.Config{QueueName: "tracking/main"})
	require.NoError(t, err)
	store := bound.GetIssueTrackingStore()
	_, err = store.Get(ctx)
	require.ErrorIs(t, err, storage.ErrNotFound)
	state := entity.IssueTrackingState{RequestID: "10", BuildID: "build/10", NotificationID: "notification/10", Phase: entity.IssueTrackingPhaseRefreshing, Version: 1}
	require.NoError(t, store.Create(ctx, state))
	require.ErrorIs(t, store.Create(ctx, state), storage.ErrAlreadyExists)
	got, err := store.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, state, got)

	state.CursorTaskID, state.CursorUnitID = "task", "unit"
	state.Phase = entity.IssueTrackingPhaseRefreshed
	require.NoError(t, store.Update(ctx, state, 1, 2))
	state.Version = 2
	require.ErrorIs(t, store.Update(ctx, state, 1, 3), storage.ErrVersionMismatch)
	got, err = store.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, state, got)

	state.RequestID, state.BuildID, state.Phase = "11", "build/11", entity.IssueTrackingPhaseFinalizing
	require.NoError(t, store.Update(ctx, state, 2, 3))
	state.Version = 3
	state.Phase = entity.IssueTrackingPhaseFinalized
	require.NoError(t, store.Update(ctx, state, 3, 4))
	state.Version = 4
	got, err = store.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, state, got)
	opens, err := bound.GetIssueStateStore().List(ctx, storage.IssueStateRange{Limit: 1})
	require.NoError(t, err)
	require.Empty(t, opens)

	t.Run("clearing a planned change retains its cursor", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: "tracking/planned"})
		require.NoError(t, err)
		store := bound.GetIssueTrackingStore()
		progress := entity.IssueTrackingState{RequestID: "10", BuildID: "build/10", NotificationID: "notice", CursorTaskID: "task", CursorUnitID: "unit", Phase: entity.IssueTrackingPhaseFinalizing, Version: 1,
			PlannedChange: entity.PlannedIssueChange{Status: entity.IssueStatusClosed, FirstSeenBuildID: "older", ExpectedVersion: 7, TimestampMs: 1234}}
		require.NoError(t, store.Create(ctx, progress))
		retained, err := store.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, progress, retained)
		next := progress
		next.PlannedChange = entity.PlannedIssueChange{}
		require.NoError(t, store.Update(ctx, next, 1, 2))
		require.ErrorIs(t, store.Update(ctx, progress, 1, 2), storage.ErrVersionMismatch)
		retained, err = store.Get(ctx)
		require.NoError(t, err)
		require.False(t, retained.HasPlannedChange())
		require.Equal(t, "task", retained.CursorTaskID)
		require.Equal(t, "unit", retained.CursorUnitID)
	})
	t.Run("queue isolation", func(t *testing.T) {
		other, err := factory.For(storage.Config{QueueName: "Tracking/main"})
		require.NoError(t, err)
		_, err = other.GetIssueTrackingStore().Get(ctx)
		require.ErrorIs(t, err, storage.ErrNotFound)
		initial := entity.IssueTrackingState{RequestID: "1", BuildID: "other", NotificationID: "other", Phase: entity.IssueTrackingPhaseRefreshing, Version: 1}
		require.NoError(t, other.GetIssueTrackingStore().Create(ctx, initial))
		got, err := store.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, state, got)
	})

	t.Run("one conditional update wins", func(t *testing.T) {
		results := make(chan error, 2)
		for _, requestID := range []string{"12", "13"} {
			next := state
			next.RequestID = requestID
			next.Phase = entity.IssueTrackingPhaseRefreshing
			go func() { results <- store.Update(ctx, next, 4, 5) }()
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
		got, err := store.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, int32(5), got.Version)
		require.Contains(t, []string{"12", "13"}, got.RequestID)
	})

	t.Run("identifier limits", func(t *testing.T) {
		bound, err := factory.For(storage.Config{QueueName: strings.Repeat("q", 255)})
		require.NoError(t, err)
		store := bound.GetIssueTrackingStore()
		value := entity.IssueTrackingState{RequestID: "9223372036854775807", BuildID: strings.Repeat("😀", 255), NotificationID: strings.Repeat("😀", 255), CursorTaskID: strings.Repeat("t", 255), CursorUnitID: strings.Repeat("u", 1024), Phase: entity.IssueTrackingPhaseRefreshing, Version: 1}
		require.NoError(t, store.Create(ctx, value))
		got, err := store.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, value, got)
		value.BuildID += "x"
		require.Error(t, store.Update(ctx, value, 1, 2))
	})
}
