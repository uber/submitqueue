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

const testRequestID = "monorepo/4"

// association builds a RequestBatch linking testRequestID to a batch.
func association(batchID string) entity.RequestBatch {
	return entity.RequestBatch{RequestID: testRequestID, BatchID: batchID, Version: 1}
}

// findStores wires a MockStorage over a batch store and a request-batch store.
func findStores(t *testing.T) (*storagemock.MockStorage, *storagemock.MockBatchStore, *storagemock.MockRequestBatchStore) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockStorage := storagemock.NewMockStorage(ctrl)
	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockAssociationStore := storagemock.NewMockRequestBatchStore(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetRequestBatchStore().Return(mockAssociationStore).AnyTimes()
	return mockStorage, mockBatchStore, mockAssociationStore
}

func TestFindByRequestID(t *testing.T) {
	storeErr := errors.New("storage failed")

	tests := map[string]struct {
		setup     func(*storagemock.MockBatchStore, *storagemock.MockRequestBatchStore)
		want      []entity.Batch
		wantStale int
		wantErr   error
	}{
		"no associations": {
			setup: func(_ *storagemock.MockBatchStore, associations *storagemock.MockRequestBatchStore) {
				associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return(nil, nil)
			},
		},
		"hydrates every association in batch id order": {
			setup: func(batchStore *storagemock.MockBatchStore, associations *storagemock.MockRequestBatchStore) {
				associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).
					Return([]entity.RequestBatch{association("b3"), association("b1")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b3").Return(batchIn("b3", entity.BatchStateCreated), nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(batchIn("b1", entity.BatchStateSucceeded), nil)
			},
			want: []entity.Batch{
				batchIn("b1", entity.BatchStateSucceeded),
				batchIn("b3", entity.BatchStateCreated),
			},
		},
		"missing batch is skipped and counted": {
			setup: func(batchStore *storagemock.MockBatchStore, associations *storagemock.MockRequestBatchStore) {
				associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).
					Return([]entity.RequestBatch{association("b1"), association("b2")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(entity.Batch{}, storage.WrapNotFound(errors.New("no rows")))
				batchStore.EXPECT().Get(gomock.Any(), "b2").Return(batchIn("b2", entity.BatchStateCreating), nil)
			},
			want:      []entity.Batch{batchIn("b2", entity.BatchStateCreating)},
			wantStale: 1,
		},
		"association read failure surfaces": {
			setup: func(_ *storagemock.MockBatchStore, associations *storagemock.MockRequestBatchStore) {
				associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return(nil, storeErr)
			},
			wantErr: storeErr,
		},
		"batch read failure surfaces": {
			setup: func(batchStore *storagemock.MockBatchStore, associations *storagemock.MockRequestBatchStore) {
				associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).
					Return([]entity.RequestBatch{association("b1")}, nil)
				batchStore.EXPECT().Get(gomock.Any(), "b1").Return(entity.Batch{}, storeErr)
			},
			wantErr: storeErr,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			mockStorage, mockBatchStore, mockAssociationStore := findStores(t)
			tt.setup(mockBatchStore, mockAssociationStore)

			got, stale, err := FindByRequestID(context.Background(), mockStorage, testRequestID)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantStale, stale)
		})
	}
}
