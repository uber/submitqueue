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
)

func testQueuePolicyStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	bound, err := factory.For(storage.Config{QueueName: "repo/main"})
	require.NoError(t, err)
	store := bound.GetQueuePolicyStore()
	head := entity.QueuePolicyHead{Queue: "repo/main", TransitionID: "initial", Revision: 1, Version: 1}
	initial := entity.QueuePolicyTransition{ID: "initial", Queue: "repo/main", Policy: entity.QueuePolicy{Revision: 1, State: entity.QueuePolicyStateDisabled, ChangedAtMs: 1000}}
	_, err = store.GetCurrent(ctx)
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, store.CreateTransition(ctx, initial))
	require.NoError(t, store.CreateCurrent(ctx, head))
	require.ErrorIs(t, store.CreateCurrent(ctx, head), storage.ErrAlreadyExists)
	require.ErrorIs(t, store.CreateTransition(ctx, initial), storage.ErrAlreadyExists)
	for _, operationID := range []string{"enable", "Enable", "enable "} {
		transition := entity.QueuePolicyTransition{ID: operationID, Queue: "repo/main", PreviousID: "initial", Policy: entity.QueuePolicy{Revision: 2, State: entity.QueuePolicyStateEnabled, EffectiveFromCommitURI: "git://repo/main/A", ChangedAtMs: 2000}}
		require.NoError(t, store.CreateTransition(ctx, transition))
		got, err := store.GetTransition(ctx, operationID)
		require.NoError(t, err)
		require.Equal(t, transition, got)
	}
	head.TransitionID = "enable"
	head.Revision = 2
	require.NoError(t, store.UpdateCurrent(ctx, head, 1, 9))
	got, err := store.GetCurrent(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(9), got.Version)
	require.ErrorIs(t, store.UpdateCurrent(ctx, head, 1, 10), storage.ErrVersionMismatch)
	after, err := store.GetCurrent(ctx)
	require.NoError(t, err)
	require.Equal(t, got, after)
	other, err := factory.For(storage.Config{QueueName: "other/main"})
	require.NoError(t, err)
	_, err = other.GetQueuePolicyStore().GetCurrent(ctx)
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = other.GetQueuePolicyStore().GetTransition(ctx, "initial")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Error(t, other.GetQueuePolicyStore().CreateCurrent(ctx, head))
	require.Error(t, other.GetQueuePolicyStore().UpdateCurrent(ctx, head, 9, 10))
	require.Error(t, other.GetQueuePolicyStore().CreateTransition(ctx, initial))
	foreign := initial
	foreign.Queue = "other/main"
	require.NoError(t, other.GetQueuePolicyStore().CreateTransition(ctx, foreign))
	initialAgain, err := store.GetTransition(ctx, "initial")
	require.NoError(t, err)
	require.Equal(t, initial, initialAgain)
}
