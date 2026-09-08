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

// testStores wires a MockStorage whose batch and queue-batch-state accessors
// return the two mocks the tests set expectations on.
func testStores(t *testing.T) (*storagemock.MockStorage, *storagemock.MockBatchStore, *storagemock.MockQueueBatchStateStore) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockStorage := storagemock.NewMockStorage(ctrl)
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockRecordStore := storagemock.NewMockQueueBatchStateStore(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetQueueBatchStateStore().Return(mockRecordStore).AnyTimes()
	return mockStorage, mockBatchStore, mockRecordStore
}

func TestTransition(t *testing.T) {
	base := entity.Batch{
		ID:       "monorepo/batch/7",
		Queue:    "monorepo",
		Contains: []string{"monorepo/1"},
		State:    entity.BatchStateCreated,
		Version:  3,
	}
	casTarget := base
	casTarget.State = entity.BatchStateSpeculating
	storeErr := errors.New("storage failed")

	tests := map[string]struct {
		newState entity.BatchState
		setup    func(*storagemock.MockBatchStore, *storagemock.MockQueueBatchStateStore)
		want     entity.Batch
		wantErr  error
	}{
		"success moves the record to the new bucket": {
			newState: entity.BatchStateSpeculating,
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				batchStore.EXPECT().Update(gomock.Any(), casTarget, int32(3), int32(4)).Return(nil)
				recordStore.EXPECT().Put(gomock.Any(), entity.QueueBatchState{
					Queue: base.Queue, State: entity.BatchStateSpeculating, BatchID: base.ID,
				}).Return(nil)
				recordStore.EXPECT().Delete(gomock.Any(), entity.BatchStateCreated, base.ID).Return(nil)
			},
			want: func() entity.Batch {
				b := casTarget
				b.Version = 4
				return b
			}(),
		},
		"same state skips the delete": {
			newState: entity.BatchStateCreated,
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				batchStore.EXPECT().Update(gomock.Any(), base, int32(3), int32(4)).Return(nil)
				recordStore.EXPECT().Put(gomock.Any(), entity.QueueBatchState{
					Queue: base.Queue, State: entity.BatchStateCreated, BatchID: base.ID,
				}).Return(nil)
			},
			want: func() entity.Batch {
				b := base
				b.Version = 4
				return b
			}(),
		},
		"lost CAS returns version mismatch and writes no records": {
			newState: entity.BatchStateSpeculating,
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				batchStore.EXPECT().Update(gomock.Any(), casTarget, int32(3), int32(4)).Return(storage.ErrVersionMismatch)
			},
			want:    base,
			wantErr: storage.ErrVersionMismatch,
		},
		"put failure surfaces after a committed CAS": {
			newState: entity.BatchStateSpeculating,
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				batchStore.EXPECT().Update(gomock.Any(), casTarget, int32(3), int32(4)).Return(nil)
				recordStore.EXPECT().Put(gomock.Any(), gomock.Any()).Return(storeErr)
			},
			want: func() entity.Batch {
				b := casTarget
				b.Version = 4
				return b
			}(),
			wantErr: storeErr,
		},
		"delete failure surfaces after the put": {
			newState: entity.BatchStateSpeculating,
			setup: func(batchStore *storagemock.MockBatchStore, recordStore *storagemock.MockQueueBatchStateStore) {
				batchStore.EXPECT().Update(gomock.Any(), casTarget, int32(3), int32(4)).Return(nil)
				recordStore.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil)
				recordStore.EXPECT().Delete(gomock.Any(), entity.BatchStateCreated, base.ID).Return(storeErr)
			},
			want: func() entity.Batch {
				b := casTarget
				b.Version = 4
				return b
			}(),
			wantErr: storeErr,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStorage, mockBatchStore, mockRecordStore := testStores(t)
			tt.setup(mockBatchStore, mockRecordStore)

			got, err := Transition(context.Background(), mockStorage, base, tt.newState)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureRecord(t *testing.T) {
	batch := entity.Batch{
		ID:      "monorepo/batch/7",
		Queue:   "monorepo",
		State:   entity.BatchStateLanding,
		Version: 5,
	}
	storeErr := errors.New("storage failed")

	tests := map[string]struct {
		setup   func(*storagemock.MockQueueBatchStateStore)
		wantErr error
	}{
		"files the batch under its current state": {
			setup: func(recordStore *storagemock.MockQueueBatchStateStore) {
				recordStore.EXPECT().Put(gomock.Any(), entity.QueueBatchState{
					Queue: batch.Queue, State: batch.State, BatchID: batch.ID,
				}).Return(nil)
			},
		},
		"put failure surfaces": {
			setup: func(recordStore *storagemock.MockQueueBatchStateStore) {
				recordStore.EXPECT().Put(gomock.Any(), gomock.Any()).Return(storeErr)
			},
			wantErr: storeErr,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStorage, _, mockRecordStore := testStores(t)
			tt.setup(mockRecordStore)

			err := EnsureRecord(context.Background(), mockStorage, batch)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
