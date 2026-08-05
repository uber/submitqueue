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

package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestList_ReturnsPageAndCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockRequestQueueSummaryStore(ctrl)
	store.EXPECT().List(gomock.Any(), storage.RequestQueueSummaryQuery{
		Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: 3,
	}).Return([]entity.RequestQueueSummary{
		{RequestID: "q/3", Queue: "q", ChangeURIs: []string{}, ReceivedAtMs: 190, Status: entity.RequestStatusAccepted, Metadata: map[string]string{}},
		{RequestID: "q/2", Queue: "q", ChangeURIs: []string{}, ReceivedAtMs: 180, Status: entity.RequestStatusLanded, Metadata: map[string]string{}},
		{RequestID: "q/1", Queue: "q", ChangeURIs: []string{}, ReceivedAtMs: 170, Status: entity.RequestStatusError, Metadata: map[string]string{}},
	}, nil)
	controller := newConfiguredListController(ctrl, store)

	result, err := controller.List(context.Background(), entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, PageSize: 2})

	require.NoError(t, err)
	require.Len(t, result.Requests, 2)
	assert.Equal(t, []string{"q/3", "q/2"}, []string{result.Requests[0].RequestID, result.Requests[1].RequestID})
	assert.Equal(t, int64(190), result.Requests[0].ReceivedAtMs)
	require.NotEmpty(t, result.NextPageToken)
	token, err := decodeListPageToken(result.NextPageToken)
	require.NoError(t, err)
	assert.Equal(t, int64(180), token.LastReceivedAtMs)
	assert.Equal(t, "q/2", token.LastRequestID)
}

func TestList_UsesCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockRequestQueueSummaryStore(ctrl)
	token := encodeListPageToken(listPageToken{Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, LastReceivedAtMs: 180, LastRequestID: "q/2"})
	store.EXPECT().List(gomock.Any(), storage.RequestQueueSummaryQuery{
		Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: 51,
		HasCursor: true, Cursor: storage.RequestQueueSummaryCursor{ReceivedAtMs: 180, RequestID: "q/2"},
	}).Return([]entity.RequestQueueSummary{}, nil)
	controller := newConfiguredListController(ctrl, store)

	result, err := controller.List(context.Background(), entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, PageToken: token})

	require.NoError(t, err)
	assert.Empty(t, result.Requests)
	assert.Empty(t, result.NextPageToken)
}

func TestList_Errors(t *testing.T) {
	validToken := encodeListPageToken(listPageToken{Queue: "other", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, LastReceivedAtMs: 150, LastRequestID: "other/1"})
	invalidFieldsToken := base64.RawURLEncoding.EncodeToString([]byte("queue=q&received_at_or_after_ms=100&received_before_ms=200&last_received_at_ms=150"))
	invalidNumberToken := base64.RawURLEncoding.EncodeToString([]byte("queue=q&received_at_or_after_ms=x&received_before_ms=200&last_received_at_ms=150&last_request_id=q%2F1"))
	backendErr := fmt.Errorf("store down")
	tests := []struct {
		name        string
		request     entity.ListRequest
		setup       func(*storagemock.MockRequestQueueSummaryStore)
		wantInvalid bool
		wantUnknown bool
	}{
		{name: "empty queue", request: entity.ListRequest{ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2}, wantInvalid: true},
		{name: "unknown queue", request: entity.ListRequest{Queue: "missing", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2}, wantUnknown: true},
		{name: "invalid range", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 2, ReceivedBeforeMs: 2}, wantInvalid: true},
		{name: "negative page size", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2, PageSize: -1}, wantInvalid: true},
		{name: "page size above maximum", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2, PageSize: 201}, wantInvalid: true},
		{name: "malformed token", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2, PageToken: "%%%"}, wantInvalid: true},
		{name: "invalid token number", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, PageToken: invalidNumberToken}, wantInvalid: true},
		{name: "invalid token fields", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, PageToken: invalidFieldsToken}, wantInvalid: true},
		{name: "token query mismatch", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, PageToken: validToken}, wantInvalid: true},
		{
			name:    "store failure",
			request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2},
			setup: func(store *storagemock.MockRequestQueueSummaryStore) {
				store.EXPECT().List(gomock.Any(), storage.RequestQueueSummaryQuery{Queue: "q", ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2, Limit: 51}).Return(nil, backendErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockRequestQueueSummaryStore(ctrl)
			queueConfigs := qcmock.NewMockStore(ctrl)
			if tt.request.Queue != "" {
				if tt.wantUnknown {
					queueConfigs.EXPECT().Get(gomock.Any(), tt.request.Queue).Return(entity.QueueConfig{}, queueconfig.ErrNotFound)
				} else {
					queueConfigs.EXPECT().Get(gomock.Any(), tt.request.Queue).Return(entity.QueueConfig{}, nil)
				}
			}
			if tt.setup != nil {
				tt.setup(store)
			}
			controller := NewListController(zap.NewNop().Sugar(), tally.NoopScope, store, queueConfigs)
			_, err := controller.List(context.Background(), tt.request)
			require.Error(t, err)
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			assert.Equal(t, tt.wantUnknown, IsUnrecognizedQueue(err))
		})
	}
}

func newConfiguredListController(ctrl *gomock.Controller, store storage.RequestQueueSummaryStore) ListController {
	queueConfigs := qcmock.NewMockStore(ctrl)
	queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
	return NewListController(zap.NewNop().Sugar(), tally.NoopScope, store, queueConfigs)
}
