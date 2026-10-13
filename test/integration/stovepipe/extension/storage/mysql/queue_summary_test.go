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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
)

func testQueueSummaryStores(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	t.Helper()
	bound, err := backend.For("state/Main")
	require.NoError(t, err)
	other, err := backend.For("state/main")
	require.NoError(t, err)
	states := bound.GetQueueSummaryStore()
	logs := bound.GetQueueLogStore()
	_, err = states.Get(ctx)
	require.ErrorIs(t, err, storage.ErrNotFound)
	state := entity.QueueSummary{Queue: "state/Main", State: entity.QueueEnablementDisabled, Version: 1, QueueStateVersion: 1, StateTimestampMs: 1000}
	require.NoError(t, states.Create(ctx, state))
	require.ErrorIs(t, states.Create(ctx, state), storage.ErrAlreadyExists)
	_, err = other.GetQueueSummaryStore().Get(ctx)
	require.ErrorIs(t, err, storage.ErrNotFound)
	state.State = entity.QueueEnablementEnabled
	state.EffectiveFromCommitURI = "A"
	state.StateTimestampMs = 2000
	state.QueueStateVersion = 2
	require.NoError(t, states.Update(ctx, state, 1, 2))
	state.Version = 2
	got, err := states.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, state, got)
	losing := state
	losing.State = entity.QueueEnablementDisabled
	losing.EffectiveFromCommitURI = "B"
	require.ErrorIs(t, states.Update(ctx, losing, 1, 3), storage.ErrVersionMismatch)
	got, err = states.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, state, got)
	log := entity.QueueLog{ID: entity.QueueLogID(2), Queue: state.Queue, State: state.State, TimestampMs: state.StateTimestampMs, QueueStateVersion: 2, EffectiveFromCommitURI: "A"}
	require.NoError(t, logs.Create(ctx, log))
	conflicting := log
	conflicting.State = entity.QueueEnablementDisabled
	require.ErrorIs(t, logs.Create(ctx, conflicting), storage.ErrAlreadyExists)
	retained, err := logs.Get(ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, log, retained)
	_, err = other.GetQueueLogStore().Get(ctx, log.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Error(t, other.GetQueueLogStore().Create(ctx, log))
	_, err = logs.Get(ctx, entity.QueueLogID(1))
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func testQueueSummaryKeyIsolation(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	t.Helper()
	for _, tt := range []struct {
		name       string
		queue      string
		otherQueue string
	}{
		{"case difference", "summary/case/main", "summary/case/Main"},
		{"trailing space", "summary/space/main", "summary/space/main "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bound, err := backend.For(tt.queue)
			require.NoError(t, err)
			other, err := backend.For(tt.otherQueue)
			require.NoError(t, err)
			store, otherStore := bound.GetQueueSummaryStore(), other.GetQueueSummaryStore()
			initial := entity.QueueSummary{Queue: tt.queue, State: entity.QueueEnablementDisabled, Version: 1, QueueStateVersion: 1, StateTimestampMs: 1000}
			require.NoError(t, store.Create(ctx, initial))
			_, err = otherStore.Get(ctx)
			require.ErrorIs(t, err, storage.ErrNotFound)
			otherState := initial
			otherState.Queue = tt.otherQueue
			require.NoError(t, otherStore.Create(ctx, otherState))
			otherState.State = entity.QueueEnablementEnabled
			otherState.EffectiveFromCommitURI = "B"
			otherState.StateTimestampMs = 2000
			otherState.QueueStateVersion = 2
			require.NoError(t, otherStore.Update(ctx, otherState, 1, 2))
			otherState.Version = 2
			got, err := store.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, initial, got)
			updated := initial
			updated.State = entity.QueueEnablementEnabled
			updated.EffectiveFromCommitURI = "A"
			updated.StateTimestampMs = 3000
			updated.QueueStateVersion = 2
			require.NoError(t, store.Update(ctx, updated, 1, 2))
			updated.Version = 2
			require.ErrorIs(t, store.Update(ctx, initial, 1, 3), storage.ErrVersionMismatch)
			require.ErrorIs(t, otherStore.Update(ctx, otherState, 1, 3), storage.ErrVersionMismatch)
			got, err = store.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, updated, got)
			got, err = otherStore.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, otherState, got)
		})
	}
}
