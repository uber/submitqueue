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

package batchchanges

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestCollect(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := entity.Batch{ID: "q/batch/1", Queue: "q", Contains: []string{"q/1", "q/2"}}
	req1 := entity.Request{ID: "q/1", Change: entity.Change{URIs: []string{"github://o/r/pull/1/a"}}}
	req2 := entity.Request{ID: "q/2", Change: entity.Change{URIs: []string{"github://o/r/pull/2/b"}}}
	rec1 := entity.ChangeRecord{Queue: "q", URI: "github://o/r/pull/1/a", RequestID: "q/1",
		Details: entity.ChangeDetails{ChangedFiles: []entity.ChangedFile{{Path: "f1", LinesAdded: 3}}}}
	// A stray record for the same URI owned by a different request must be skipped.
	recOther := entity.ChangeRecord{Queue: "q", URI: "github://o/r/pull/1/a", RequestID: "q/999"}
	rec2 := entity.ChangeRecord{Queue: "q", URI: "github://o/r/pull/2/b", RequestID: "q/2",
		Details: entity.ChangeDetails{ChangedFiles: []entity.ChangedFile{{Path: "f2", LinesAdded: 5}}}}

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), "q/1").Return(req1, nil)
	reqStore.EXPECT().Get(gomock.Any(), "q/2").Return(req2, nil)

	changeStore := storagemock.NewMockChangeStore(ctrl)
	changeStore.EXPECT().GetByURI(gomock.Any(), "q", "github://o/r/pull/1/a").Return([]entity.ChangeRecord{recOther, rec1}, nil)
	changeStore.EXPECT().GetByURI(gomock.Any(), "q", "github://o/r/pull/2/b").Return([]entity.ChangeRecord{rec2}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
	store.EXPECT().GetChangeStore().Return(changeStore).AnyTimes()

	got, err := Collect(context.Background(), store, batch)
	require.NoError(t, err)
	assert.Equal(t, "q/batch/1", got.BatchID)
	assert.Equal(t, "q", got.Queue)
	require.Len(t, got.Changes, 2)
	assert.Equal(t, "github://o/r/pull/1/a", got.Changes[0].URI)
	assert.Equal(t, "github://o/r/pull/2/b", got.Changes[1].URI)
	assert.Equal(t, 8, got.TotalLinesChanged())
}
