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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestCreateContext(t *testing.T) {
	requestContext := entity.RequestContext{
		RequestID:    "q/1",
		Queue:        "q",
		ChangeURIs:   []string{"github://uber/repo/pull/1/abc"},
		AdmittedAtMs: 100,
	}

	tests := []struct {
		name        string
		stored      entity.RequestContext
		wantErr     bool
		setupCreate func(*storagemock.MockRequestContextStore)
	}{
		{
			name: "new context",
			setupCreate: func(contextStore *storagemock.MockRequestContextStore) {
				contextStore.EXPECT().Create(gomock.Any(), requestContext).Return(nil)
			},
		},
		{
			name:   "identical retry",
			stored: requestContext,
			setupCreate: func(contextStore *storagemock.MockRequestContextStore) {
				contextStore.EXPECT().Create(gomock.Any(), requestContext).Return(storage.ErrAlreadyExists)
				contextStore.EXPECT().Get(gomock.Any(), requestContext.RequestID).Return(requestContext, nil)
			},
		},
		{
			name: "conflicting retry",
			stored: entity.RequestContext{
				RequestID: requestContext.RequestID,
				Queue:     "other",
			},
			wantErr: true,
			setupCreate: func(contextStore *storagemock.MockRequestContextStore) {
				contextStore.EXPECT().Create(gomock.Any(), requestContext).Return(storage.ErrAlreadyExists)
				contextStore.EXPECT().Get(gomock.Any(), requestContext.RequestID).Return(entity.RequestContext{RequestID: requestContext.RequestID, Queue: "other"}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			contextStore := storagemock.NewMockRequestContextStore(ctrl)
			tt.setupCreate(contextStore)
			summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
			if !tt.wantErr {
				summaryStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			}
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestContextStore().Return(contextStore).AnyTimes()
			if !tt.wantErr {
				store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
			}

			err := CreateContext(context.Background(), store, requestContext)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestEqualRequestContext_NormalizesEmptyChangeURIs(t *testing.T) {
	left := entity.RequestContext{RequestID: "q/1", Queue: "q", AdmittedAtMs: 100}
	right := left
	right.ChangeURIs = []string{}

	require.True(t, equalRequestContext(left, right))
}
