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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
)

func testFixStores(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	t.Helper()
	bound, err := backend.For("repair/main")
	require.NoError(t, err)
	other, err := backend.For("repair/other")
	require.NoError(t, err)
	ref := entity.OpenIssue{TaskID: "compile", UnitID: "library", FirstSeenBuildID: "build-1"}

	t.Run("repair lifecycle", func(t *testing.T) {
		store := bound.GetFixStore()
		_, err := store.Get(ctx, "missing")
		require.ErrorIs(t, err, storage.ErrNotFound)
		fix := entity.Fix{ID: "fix-1", BuildID: "build-1", Queue: "repair/main", State: entity.FixQueued,
			Issues: []entity.OpenIssue{ref}, Metadata: map[string]string{}, Version: 1, CreatedAt: 1000, UpdatedAt: 1000}
		require.ErrorIs(t, store.Update(ctx, fix, 1, 2), storage.ErrVersionMismatch)
		require.NoError(t, store.Create(ctx, fix))
		require.ErrorIs(t, store.Create(ctx, fix), storage.ErrAlreadyExists)
		got, err := store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, fix, got)

		updated := fix
		updated.State = entity.FixSubmitted
		updated.Issues = append([]entity.OpenIssue{ref}, entity.OpenIssue{TaskID: "compile", UnitID: "consumer", FirstSeenBuildID: "build-2"})
		updated.ExecutionRef = "execution-1"
		updated.Metadata = map[string]string{"change_uri": "git://repo/change/1", "native_run_id": "run-1", "landed_uri": "git://repo/main/revision-1", "detail": "repair submitted", "custom": "opaque context"}
		updated.UpdatedAt = 2000
		require.NoError(t, store.Update(ctx, updated, 1, 7))
		require.Equal(t, int32(1), updated.Version, "the store must not mutate its input")
		updated.Version = 7
		got, err = store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, updated, got)
		require.ErrorIs(t, store.Update(ctx, fix, 1, 2), storage.ErrVersionMismatch)
		got, err = store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, updated, got, "a stale update must preserve the newer repair")

		_, err = other.GetFixStore().Get(ctx, fix.ID)
		require.ErrorIs(t, err, storage.ErrNotFound)
		require.Error(t, other.GetFixStore().Create(ctx, fix))
		require.Error(t, other.GetFixStore().Update(ctx, updated, 7, 8))
		foreign := fix
		foreign.Queue = "repair/other"
		foreign.Metadata = map[string]string{"detail": "other queue"}
		require.NoError(t, other.GetFixStore().Create(ctx, foreign))
		foreign.State = entity.FixSubmissionUnknown
		require.NoError(t, other.GetFixStore().Update(ctx, foreign, 1, 2))
		got, err = store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, updated, got)
	})

	t.Run("metadata normalization and replacement", func(t *testing.T) {
		for _, tt := range []struct {
			name     string
			metadata map[string]string
		}{
			{name: "nil"},
			{name: "empty", metadata: map[string]string{}},
			{name: "provider context", metadata: map[string]string{"provider.custom": "opaque", "detail": "progress"}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				store := bound.GetFixStore()
				fix := entity.Fix{ID: "metadata-" + tt.name, Queue: "repair/main", Metadata: tt.metadata, Version: 1}
				require.NoError(t, store.Create(ctx, fix))
				got, err := store.Get(ctx, fix.ID)
				require.NoError(t, err)
				require.NotNil(t, got.Metadata)
				if tt.metadata == nil {
					require.Empty(t, got.Metadata)
					require.Nil(t, fix.Metadata)
				} else {
					require.Equal(t, tt.metadata, got.Metadata)
				}
				got.State = entity.FixSubmitted
				require.NoError(t, store.Update(ctx, got, 1, 2))
				reloaded, err := store.Get(ctx, fix.ID)
				require.NoError(t, err)
				require.Equal(t, got.Metadata, reloaded.Metadata)
				got.Metadata = nil
				require.NoError(t, store.Update(ctx, got, 2, 3))
				reloaded, err = store.Get(ctx, fix.ID)
				require.NoError(t, err)
				require.NotNil(t, reloaded.Metadata)
				require.Empty(t, reloaded.Metadata)
			})
		}
	})

	t.Run("submission lifecycle and stale results", func(t *testing.T) {
		store := bound.GetFixStore()
		fix := entity.Fix{ID: "submission", Queue: "repair/main", State: entity.FixQueued,
			Issues: []entity.OpenIssue{ref}, Metadata: map[string]string{}, Version: 1}
		require.NoError(t, store.Create(ctx, fix))

		fix.State = entity.FixSubmitting
		require.NoError(t, store.Update(ctx, fix, 1, 2))
		fix.Version = 2
		got, err := store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, fix, got, "submission intent must survive a reload")
		stale := fix

		fix.State = entity.FixSubmitted
		fix.ExecutionRef = "execution-1"
		require.NoError(t, store.Update(ctx, fix, 2, 3))
		fix.Version = 3
		got, err = store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, fix, got)

		fix.State = entity.FixSubmissionUnknown
		require.NoError(t, store.Update(ctx, fix, 3, 4))
		fix.Version = 4
		got, err = store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, fix, got)

		stale.State = entity.FixSubmitted
		stale.ExecutionRef = "late-execution-1"
		require.ErrorIs(t, store.Update(ctx, stale, 2, 3), storage.ErrVersionMismatch)
		got, err = store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, fix, got)
		nextAttempt := entity.Fix{ID: "next-build-submission", Queue: fix.Queue, BuildID: "build-2", State: entity.FixQueued, Issues: fix.Issues, Version: 1}
		require.NoError(t, store.Create(ctx, nextAttempt))
		original, err := store.Get(ctx, fix.ID)
		require.NoError(t, err)
		require.Equal(t, entity.FixSubmissionUnknown, original.State)

	})

	t.Run("opaque identities", func(t *testing.T) {
		for _, key := range []string{"Check", "check", "check ", strings.Repeat("é", 300)} {
			fix := entity.Fix{ID: key, Queue: "repair/main", State: entity.FixQueued, Metadata: map[string]string{}, Version: 1}
			require.NoError(t, bound.GetFixStore().Create(ctx, fix))
			got, err := bound.GetFixStore().Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, fix, got)
		}
	})
}
