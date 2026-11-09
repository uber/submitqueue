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

func testRequestIssuesByProject(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	bound, err := backend.For("projects/main")
	require.NoError(t, err)
	store := bound.GetRequestIssuesByProjectStore()
	entries := []entity.ProjectIssueAttribution{
		{TaskID: "test", UnitID: "b", Project: "service"},
		{TaskID: "Test", UnitID: "x", Project: "service"},
		{TaskID: "test", UnitID: "a", Project: "service"},
		{TaskID: "test", UnitID: "a\x00", Project: "service"},
		{TaskID: "z", UnitID: "end", Project: "service"},
		{TaskID: "test", UnitID: "a", Project: "Service"},
		{TaskID: "test", UnitID: "a", Project: "other"},
	}
	for _, entry := range entries {
		require.NoError(t, bound.GetRequestProjectsByIssueStore().Create(ctx, "1", entry))
	}
	page, err := store.List(ctx, "1", "service", entity.ProjectIssueAttribution{}, 2)
	require.NoError(t, err)
	require.Empty(t, page)
	for _, entry := range entries {
		require.NoError(t, store.Create(ctx, "1", entry))
		require.ErrorIs(t, store.Create(ctx, "1", entry), storage.ErrAlreadyExists)
	}

	other, err := backend.For("other")
	require.NoError(t, err)
	unrelated := entity.ProjectIssueAttribution{TaskID: "other", UnitID: "unit", Project: "service"}
	require.NoError(t, other.GetRequestIssuesByProjectStore().Create(ctx, "1", unrelated))
	require.NoError(t, store.Create(ctx, "2", unrelated))

	want := []entity.ProjectIssueAttribution{entries[1], entries[2], entries[3], entries[0], entries[4]}
	for _, pageSize := range []int{1, 2, 3, 20} {
		var got []entity.ProjectIssueAttribution
		var after entity.ProjectIssueAttribution
		for {
			page, err := store.List(ctx, "1", "service", after, pageSize)
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
	page, err = store.List(ctx, "1", "Service", entity.ProjectIssueAttribution{}, 2)
	require.NoError(t, err)
	require.Equal(t, []entity.ProjectIssueAttribution{entries[5]}, page)
	page, err = store.List(ctx, "1", "service", entity.ProjectIssueAttribution{TaskID: "test", UnitID: "az", Project: "service"}, 2)
	require.NoError(t, err)
	require.Equal(t, want[3:], page)
	page, err = store.List(ctx, "1", "missing", entity.ProjectIssueAttribution{}, 2)
	require.NoError(t, err)
	require.Empty(t, page)
	page, err = store.List(ctx, "3", "service", entity.ProjectIssueAttribution{}, 2)
	require.NoError(t, err)
	require.Empty(t, page)
}

func testRequestIssuesByProjectKeyLimits(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	bound, err := backend.For(strings.Repeat("q", 255))
	require.NoError(t, err)
	store := bound.GetRequestIssuesByProjectStore()
	entry := entity.ProjectIssueAttribution{
		TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("u", 1024), Project: strings.Repeat("界", 340),
	}
	require.NoError(t, store.Create(ctx, "9223372036854775807", entry))
	page, err := store.List(ctx, "9223372036854775807", entry.Project, entity.ProjectIssueAttribution{}, 1)
	require.NoError(t, err)
	require.Equal(t, []entity.ProjectIssueAttribution{entry}, page)
	page, err = store.List(ctx, "9223372036854775807", entry.Project, entry, 1)
	require.NoError(t, err)
	require.Empty(t, page)
}
