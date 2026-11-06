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

func testRequestIssueSummaryStore(t *testing.T, ctx context.Context, factory storage.Factory) {
	bound, err := factory.For(storage.Config{QueueName: "request-issues"})
	require.NoError(t, err)
	store := bound.GetRequestIssueSummaryStore()
	for _, issue := range []entity.RequestIssueSummary{
		{RequestID: "12", TaskID: "test", UnitID: "z", Kind: entity.RequestIssueKindClosed, BuildID: "b/12", FirstSeenBuildID: "b/10"},
		{RequestID: "12", TaskID: "test", Kind: entity.RequestIssueKindNew, BuildID: "b/12", FirstSeenBuildID: "b/12"},
		{RequestID: "12", TaskID: "test", UnitID: "a", Kind: entity.RequestIssueKindExisting, BuildID: "b/12", FirstSeenBuildID: "b/10"},
	} {
		require.NoError(t, store.Create(ctx, issue))
	}
	first, err := store.List(ctx, "12", storage.RequestIssueSummaryRange{Limit: 2})
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, "", first[0].UnitID)
	require.Equal(t, "a", first[1].UnitID)
	last, err := store.List(ctx, "12", storage.RequestIssueSummaryRange{After: storage.RequestIssueSummaryCursor{TaskID: "test", UnitID: "a"}, Limit: 2})
	require.NoError(t, err)
	require.Len(t, last, 1)
	require.Equal(t, "z", last[0].UnitID)
	duplicate := first[0]
	duplicate.Kind = entity.RequestIssueKindExisting
	require.ErrorIs(t, store.Create(ctx, duplicate), storage.ErrAlreadyExists)
	retained, err := store.Get(ctx, "12", "test", "")
	require.NoError(t, err)
	require.Equal(t, first[0], retained)
	other, err := factory.For(storage.Config{QueueName: "other-request-issues"})
	require.NoError(t, err)
	_, err = other.GetRequestIssueSummaryStore().Get(ctx, "12", "test", "")
	require.ErrorIs(t, err, storage.ErrNotFound)
	empty, err := store.List(ctx, "13", storage.RequestIssueSummaryRange{Limit: 2})
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NoError(t, store.Create(ctx, entity.RequestIssueSummary{RequestID: "13", TaskID: "test", Kind: entity.RequestIssueKindExisting, BuildID: "b/13", FirstSeenBuildID: "b/12"}))
	retained, err = store.Get(ctx, "12", "test", "")
	require.NoError(t, err)
	require.Equal(t, first[0], retained)

	closed := retained
	closed.Kind = entity.RequestIssueKindClosed
	require.NoError(t, store.Update(ctx, closed, 0, 1))
	require.ErrorIs(t, store.Update(ctx, retained, 0, 1), storage.ErrVersionMismatch)
	got, err := store.Get(ctx, "12", "test", "")
	require.NoError(t, err)
	require.Equal(t, entity.RequestIssueKindClosed, got.Kind)
	require.Equal(t, int32(1), got.Version)
}
