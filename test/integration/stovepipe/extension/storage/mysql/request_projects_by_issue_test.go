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

func testRequestProjectsByIssue(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	bound, err := backend.For("attribution/main")
	require.NoError(t, err)
	store := bound.GetRequestProjectsByIssueStore()
	entries := []entity.ProjectIssueAttribution{
		{TaskID: "z", UnitID: "last", Project: "last"},
		{TaskID: "test", UnitID: "x", Project: "b"},
		{TaskID: "test", UnitID: "x", Project: "a"},
		{TaskID: "Test", UnitID: "x", Project: "a"},
		{TaskID: "test", UnitID: "x", Project: "A"},
		{TaskID: "test", UnitID: "x\x00", Project: "after"},
	}
	require.NoError(t, store.Create(ctx, "1", entries[0]))
	_, err = bound.GetRequestLogStore().Get(ctx, "1", "project-issues/all")
	require.ErrorIs(t, err, storage.ErrNotFound)
	for i, entry := range entries {
		err := store.Create(ctx, "1", entry)
		if i == 0 {
			require.ErrorIs(t, err, storage.ErrAlreadyExists)
		} else {
			require.NoError(t, err)
		}
		require.ErrorIs(t, store.Create(ctx, "1", entry), storage.ErrAlreadyExists)
	}

	other, err := backend.For("other")
	require.NoError(t, err)
	unrelated := entity.ProjectIssueAttribution{TaskID: "other", UnitID: "unit", Project: "service"}
	require.NoError(t, other.GetRequestProjectsByIssueStore().Create(ctx, "1", unrelated))
	require.NoError(t, store.Create(ctx, "2", unrelated))

	want := []entity.ProjectIssueAttribution{entries[3], entries[4], entries[2], entries[1], entries[5], entries[0]}
	for _, pageSize := range []int{1, 2, 3, 20} {
		var got []entity.ProjectIssueAttribution
		var after entity.ProjectIssueAttribution
		for {
			page, err := store.List(ctx, "1", after, pageSize)
			require.NoError(t, err)
			require.LessOrEqual(t, len(page), pageSize)
			if len(page) == 0 {
				break
			}
			got = append(got, page...)
			require.LessOrEqual(t, len(got), len(want))
			after = page[len(page)-1]
		}
		require.Equal(t, want, got)
	}
	for _, pageSize := range []int{1, 2, 3, 20} {
		var got []entity.ProjectIssueAttribution
		after := ""
		for {
			page, err := store.ListByIssue(ctx, "1", "test", "x", after, pageSize)
			require.NoError(t, err)
			if len(page) == 0 {
				break
			}
			got = append(got, page...)
			require.LessOrEqual(t, len(got), 3)
			after = page[len(page)-1].Project
		}
		require.Equal(t, []entity.ProjectIssueAttribution{entries[4], entries[2], entries[1]}, got)
	}
	missing, err := store.ListByIssue(ctx, "1", "test", "missing", "", 2)
	require.NoError(t, err)
	require.Empty(t, missing)
	page, err := store.List(ctx, "1", entity.ProjectIssueAttribution{TaskID: "test", UnitID: "w", Project: "service"}, 2)
	require.NoError(t, err)
	require.Equal(t, want[1:3], page)
	page, err = store.List(ctx, "4", entity.ProjectIssueAttribution{}, 2)
	require.NoError(t, err)
	require.Empty(t, page)

	log := entity.RequestLog{
		ID: "project-issues/all", Queue: "attribution/main", RequestID: "1", TimestampMs: 1000,
		Event:    entity.RequestEventProjectIssuesRecorded,
		Metadata: map[string]string{"build_id": "build-1"},
	}
	require.NoError(t, bound.GetRequestLogStore().Create(ctx, log))
	duplicate := log
	duplicate.TimestampMs = 2000
	require.ErrorIs(t, bound.GetRequestLogStore().Create(ctx, duplicate), storage.ErrAlreadyExists)
	retained, err := bound.GetRequestLogStore().Get(ctx, log.RequestID, log.ID)
	require.NoError(t, err)
	require.Equal(t, log, retained)
	page, err = store.List(ctx, retained.RequestID, entity.ProjectIssueAttribution{}, 20)
	require.NoError(t, err)
	require.Equal(t, want, page)

	empty := log
	empty.RequestID = "3"
	empty.Metadata = map[string]string{"build_id": "build-3"}
	require.NoError(t, bound.GetRequestLogStore().Create(ctx, empty))
	retained, err = bound.GetRequestLogStore().Get(ctx, empty.RequestID, empty.ID)
	require.NoError(t, err)
	page, err = store.List(ctx, retained.RequestID, entity.ProjectIssueAttribution{}, 2)
	require.NoError(t, err)
	require.Empty(t, page)
	_, err = bound.GetRequestLogStore().Get(ctx, "4", empty.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func testRequestProjectsByIssueKeyLimits(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	bound, err := backend.For(strings.Repeat("q", 255))
	require.NoError(t, err)
	store := bound.GetRequestProjectsByIssueStore()
	entry := entity.ProjectIssueAttribution{
		TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("u", 1024), Project: strings.Repeat("界", 340),
	}
	require.NoError(t, store.Create(ctx, "9223372036854775807", entry))
	page, err := store.List(ctx, "9223372036854775807", entity.ProjectIssueAttribution{}, 1)
	require.NoError(t, err)
	require.Equal(t, []entity.ProjectIssueAttribution{entry}, page)
	page, err = store.List(ctx, "9223372036854775807", entry, 1)
	require.NoError(t, err)
	require.Empty(t, page)
}
