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

package request

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestAdmissionWriter_Create(t *testing.T) {
	summary := testRequestSummary()
	tests := []struct {
		name    string
		setup   func(*gomock.Controller, *storagemock.MockStorage)
		wantErr error
	}{
		{
			name: "creates all projections",
			setup: func(ctrl *gomock.Controller, store *storagemock.MockStorage) {
				summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
				uriStore := storagemock.NewMockRequestURIStore(ctrl)
				queueStore := storagemock.NewMockRequestQueueSummaryStore(ctrl)
				store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
				store.EXPECT().GetRequestURIStore().Return(uriStore).AnyTimes()
				store.EXPECT().GetRequestQueueSummaryStore().Return(queueStore).AnyTimes()
				summaryStore.EXPECT().Create(gomock.Any(), summary).Return(nil)
				uriStore.EXPECT().Create(gomock.Any(), entity.RequestURI{ChangeURI: "uri/1", ReceivedAtMs: 10, RequestID: "q/1"}).Return(nil)
				uriStore.EXPECT().Create(gomock.Any(), entity.RequestURI{ChangeURI: "uri/2", ReceivedAtMs: 10, RequestID: "q/1"}).Return(nil)
				queueStore.EXPECT().Create(gomock.Any(), queueSummaryFromSummary(summary)).Return(nil)
			},
		},
		{
			name: "identical retry succeeds",
			setup: func(ctrl *gomock.Controller, store *storagemock.MockStorage) {
				summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
				uriStore := storagemock.NewMockRequestURIStore(ctrl)
				queueStore := storagemock.NewMockRequestQueueSummaryStore(ctrl)
				store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
				store.EXPECT().GetRequestURIStore().Return(uriStore).AnyTimes()
				store.EXPECT().GetRequestQueueSummaryStore().Return(queueStore).AnyTimes()
				summaryStore.EXPECT().Create(gomock.Any(), summary).Return(storage.ErrAlreadyExists)
				summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(summary, nil)
				uriStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists).Times(2)
				queueStore.EXPECT().Create(gomock.Any(), queueSummaryFromSummary(summary)).Return(storage.ErrAlreadyExists)
				queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueSummaryFromSummary(summary), nil)
			},
		},
		{
			name: "conflicting summary retry fails",
			setup: func(ctrl *gomock.Controller, store *storagemock.MockStorage) {
				summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
				store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
				summaryStore.EXPECT().Create(gomock.Any(), summary).Return(storage.ErrAlreadyExists)
				conflict := summary
				conflict.Queue = "other"
				summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(conflict, nil)
			},
			wantErr: storage.ErrAlreadyExists,
		},
		{
			name: "URI write failure stops queue projection",
			setup: func(ctrl *gomock.Controller, store *storagemock.MockStorage) {
				summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
				uriStore := storagemock.NewMockRequestURIStore(ctrl)
				store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
				store.EXPECT().GetRequestURIStore().Return(uriStore).AnyTimes()
				summaryStore.EXPECT().Create(gomock.Any(), summary).Return(nil)
				uriStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("URI down"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockStorage(ctrl)
			tt.setup(ctrl, store)
			err := NewAdmissionWriter(store).Create(context.Background(), summary)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else if tt.name == "URI write failure stops queue projection" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func testRequestSummary() entity.RequestSummary {
	return entity.RequestSummary{
		RequestID: "q/1", Queue: "q", ChangeURIs: []string{"uri/1", "uri/2"}, ReceivedAtMs: 10,
		Status: entity.RequestStatusAccepted, StatusTimestampMs: 10, Version: 1, Metadata: map[string]string{},
	}
}
