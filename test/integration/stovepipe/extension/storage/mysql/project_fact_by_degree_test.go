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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
)

func testProjectFactByDegree(t *testing.T, ctx context.Context, backend *mysqlstorage.Storage) {
	const queue, uri = "index/main", "commit"
	bound, err := backend.For(queue)
	require.NoError(t, err)
	index := bound.GetProjectFactByDegreeStore()
	facts := []entity.ValidationFact{
		{URI: uri, Project: "z", Degree: 1},
		{URI: uri, Project: "y", Degree: 1},
		{URI: uri, Project: "a", Degree: 0.5},
		{URI: uri, Project: "b", Degree: math.Nextafter(0.5, 1)},
		{URI: uri, Project: "green", Degree: 0},
		{URI: uri, Project: "another-green", Degree: 0},
	}
	var entries []entity.ProjectFactByDegree
	for _, fact := range facts {
		fact.RequestID = "1"
		require.NoError(t, bound.GetValidationFactStore().Create(ctx, fact))
		entry, err := entity.NewProjectFactByDegree(fact)
		require.NoError(t, err)
		require.NoError(t, index.Create(ctx, entry))
		require.ErrorIs(t, index.Create(ctx, entry), storage.ErrAlreadyExists)
		entries = append(entries, entry)
	}
	require.NoError(t, bound.GetValidationFactStore().Create(ctx, entity.ValidationFact{URI: uri, Degree: 1, RequestID: "1"}))

	historical := entity.ValidationFact{URI: "older-commit", Project: "old-project", Degree: 1, RequestID: "2"}
	require.NoError(t, bound.GetValidationFactStore().Create(ctx, historical))
	unindexed, err := index.List(ctx, historical.URI, entity.ProjectFactByDegree{}, 2)
	require.NoError(t, err)
	assert.Empty(t, unindexed)
	preserved, err := bound.GetValidationFactStore().Get(ctx, historical.URI, historical.Project)
	require.NoError(t, err)
	assert.Equal(t, historical, preserved)

	other, err := backend.For("other")
	require.NoError(t, err)
	require.NoError(t, other.GetProjectFactByDegreeStore().Create(ctx, entries[0]))
	otherURI := entries[0]
	otherURI.URI = "other"
	require.NoError(t, index.Create(ctx, otherURI))
	want := []entity.ProjectFactByDegree{entries[1], entries[0], entries[3], entries[2], entries[5], entries[4]}
	for _, pageSize := range []int{1, 2, 10} {
		var got []entity.ProjectFactByDegree
		var after entity.ProjectFactByDegree
		for {
			page, err := index.List(ctx, uri, after, pageSize)
			require.NoError(t, err)
			if len(page) == 0 {
				break
			}
			got = append(got, page...)
			require.LessOrEqual(t, len(got), len(want))
			after = page[len(page)-1]
		}
		assert.Equal(t, want, got)
	}
	missing, err := index.List(ctx, "missing", entity.ProjectFactByDegree{}, 2)
	require.NoError(t, err)
	assert.Empty(t, missing)
}
