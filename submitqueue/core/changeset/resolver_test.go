// Copyright (c) 2025 Uber Technologies, Inc.
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

package changeset

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
)

func req(id string, uris ...string) entity.Request {
	return entity.Request{ID: id, Change: entity.Change{URIs: uris}}
}

func TestResolverChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	reqs := storagemock.NewMockRequestStore(ctrl)
	changes := storagemock.NewMockChangeStore(ctrl)
	r := New(reqs, changes)

	reqs.EXPECT().Get(gomock.Any(), "r2").Return(req("r2", "u2"), nil)
	reqs.EXPECT().Get(gomock.Any(), "r3").Return(req("r3", "u3"), nil)

	got, err := r.ChangesForBatch(context.Background(), entity.Batch{ID: "q/batch/2", Contains: []string{"r2", "r3"}})
	require.NoError(t, err)
	// request order within the batch is preserved.
	assert.Equal(t, []entity.Change{{URIs: []string{"u2"}}, {URIs: []string{"u3"}}}, got)
}

func TestResolverChangesEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	r := New(storagemock.NewMockRequestStore(ctrl), storagemock.NewMockChangeStore(ctrl))

	got, err := r.ChangesForBatch(context.Background(), entity.Batch{ID: "q/batch/1"})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestResolverChangesRequestError(t *testing.T) {
	ctrl := gomock.NewController(t)
	reqs := storagemock.NewMockRequestStore(ctrl)
	r := New(reqs, storagemock.NewMockChangeStore(ctrl))

	sentinel := errors.New("not found")
	reqs.EXPECT().Get(gomock.Any(), "r1").Return(entity.Request{}, sentinel)

	_, err := r.ChangesForBatch(context.Background(), entity.Batch{ID: "q/batch/1", Contains: []string{"r1"}})
	require.ErrorIs(t, err, sentinel)
}

func TestResolverDetailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	reqs := storagemock.NewMockRequestStore(ctrl)
	changes := storagemock.NewMockChangeStore(ctrl)
	r := New(reqs, changes)

	batch := entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{"r1", "r2"}}
	reqs.EXPECT().Get(gomock.Any(), "r1").Return(req("r1", "u1"), nil)
	reqs.EXPECT().Get(gomock.Any(), "r2").Return(req("r2", "u2"), nil)

	d1 := entity.ChangeDetails{ChangedFiles: []entity.ChangedFile{{Path: "a.go", LinesAdded: 3}}}
	d2 := entity.ChangeDetails{ChangedFiles: []entity.ChangedFile{{Path: "b.go", LinesAdded: 5}}}
	// GetByURI returns rows for every request that ever claimed the URI; the
	// resolver must pick the row owned by the requesting request.
	changes.EXPECT().GetByURI(gomock.Any(), "q", "u1").Return([]entity.ChangeRecord{
		{URI: "u1", RequestID: "other", Details: entity.ChangeDetails{}},
		{URI: "u1", RequestID: "r1", Details: d1},
	}, nil)
	changes.EXPECT().GetByURI(gomock.Any(), "q", "u2").Return([]entity.ChangeRecord{
		{URI: "u2", RequestID: "r2", Details: d2},
	}, nil)

	got, err := r.DetailedForBatch(context.Background(), batch)
	require.NoError(t, err)
	assert.Equal(t, entity.BatchChanges{
		BatchID: "q/batch/1",
		Queue:   "q",
		Changes: []entity.ChangeInfo{{URI: "u1", Details: d1}, {URI: "u2", Details: d2}},
	}, got)
}

func TestResolverDetailedChangeStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	reqs := storagemock.NewMockRequestStore(ctrl)
	changes := storagemock.NewMockChangeStore(ctrl)
	r := New(reqs, changes)

	sentinel := errors.New("read failed")
	reqs.EXPECT().Get(gomock.Any(), "r1").Return(req("r1", "u1"), nil)
	changes.EXPECT().GetByURI(gomock.Any(), "q", "u1").Return(nil, sentinel)

	_, err := r.DetailedForBatch(context.Background(), entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{"r1"}})
	require.ErrorIs(t, err, sentinel)
}
