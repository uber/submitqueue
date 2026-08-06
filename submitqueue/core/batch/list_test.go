// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
)

const testQueue = "monorepo"

// record builds a QueueBatchState for testQueue.
func record(state entity.BatchState, batchID string) entity.QueueBatchState {
	return entity.QueueBatchState{Queue: testQueue, State: state, BatchID: batchID}
}

// batchIn builds a hydrated Batch for testQueue in the given state.
func batchIn(id string, state entity.BatchState) entity.Batch {
	return entity.Batch{ID: id, Queue: testQueue, State: state, Version: 1}
}

func TestListByStates(t *testing.T) {
	storeErr := errors.New("storage failed")

	tests := map[string]struct {
		states  []entity.BatchState
		setup   func(*storagemock.MockBatchStore, *storagemock.MockQueueBatchStateStore)
		want    []entity.Batch
		wantErr error
	}{
		"empty states lists nothing": {
			states: nil,
			setup:  func(*storagemock.MockBatchStore, *storagemock.MockQueueBatchStateStore) {},
		},
		"hydrates every bucket and dedupes across them": {
			states: []entity.BatchState{entity.BatchStateCreated, entity.BatchStateSpeculating},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				// b2 appears in both buckets (mid-move duplicate): it must be hydrated
				// and returned exactly once.
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).
					Return([]entity.QueueBatchState{record(entity.BatchStateCreated, "b1"), record(entity.BatchStateCreated, "b2")}, nil)
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateSpeculating).
					Return([]entity.QueueBatchState{record(entity.BatchStateSpeculating, "b2"), record(entity.BatchStateSpeculating, "b3")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(batchIn("b1", entity.BatchStateCreated), nil)
				batchStore.EXPECT().Get(gomock.Any(), "b2").Return(batchIn("b2", entity.BatchStateSpeculating), nil)
				batchStore.EXPECT().Get(gomock.Any(), "b3").Return(batchIn("b3", entity.BatchStateSpeculating), nil)
			},
			want: []entity.Batch{
				batchIn("b1", entity.BatchStateCreated),
				batchIn("b2", entity.BatchStateSpeculating),
				batchIn("b3", entity.BatchStateSpeculating),
			},
		},
		"classifies by hydrated state, not by bucket": {
			states: []entity.BatchState{entity.BatchStateCreated},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				// A stale record files b1 under created, but the batch has moved on to
				// speculating — a state outside the requested set, so it is dropped.
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).
					Return([]entity.QueueBatchState{record(entity.BatchStateCreated, "b1")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(batchIn("b1", entity.BatchStateSpeculating), nil)
			},
		},
		"stale bucket still surfaces a batch whose true state is requested": {
			states: []entity.BatchState{entity.BatchStateCreated, entity.BatchStateSpeculating},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				// Only a stale created record exists for b1, but its hydrated state is
				// speculating — requested, so the batch is returned under its true state.
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).
					Return([]entity.QueueBatchState{record(entity.BatchStateCreated, "b1")}, nil)
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateSpeculating).
					Return(nil, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(batchIn("b1", entity.BatchStateSpeculating), nil)
			},
			want: []entity.Batch{batchIn("b1", entity.BatchStateSpeculating)},
		},
		"duplicate input states are listed once": {
			states: []entity.BatchState{entity.BatchStateCreated, entity.BatchStateCreated},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).
					Return([]entity.QueueBatchState{record(entity.BatchStateCreated, "b1")}, nil).
					Times(1)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(batchIn("b1", entity.BatchStateCreated), nil)
			},
			want: []entity.Batch{batchIn("b1", entity.BatchStateCreated)},
		},
		"list failure surfaces": {
			states: []entity.BatchState{entity.BatchStateCreated},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).Return(nil, storeErr)
			},
			wantErr: storeErr,
		},
		"hydrate failure surfaces": {
			states: []entity.BatchState{entity.BatchStateCreated},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).
					Return([]entity.QueueBatchState{record(entity.BatchStateCreated, "b1")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(entity.Batch{}, storeErr)
			},
			wantErr: storeErr,
		},
		"dangling record is an error, not a skip": {
			states: []entity.BatchState{entity.BatchStateCreated},
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				recordStore.EXPECT().List(gomock.Any(), entity.BatchStateCreated).
					Return([]entity.QueueBatchState{record(entity.BatchStateCreated, "b1")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(entity.Batch{}, storage.WrapNotFound(errors.New("no rows")))
			},
			wantErr: storage.ErrNotFound,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStorage, mockBatchStore, mockRecordStore := testStores(t)
			tt.setup(mockBatchStore, mockRecordStore)

			got, err := ListByStates(context.Background(), mockStorage, tt.states)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
