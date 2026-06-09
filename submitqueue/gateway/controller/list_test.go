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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestList_ReturnsSummaries(t *testing.T) {
	ctrl := gomock.NewController(t)

	qcs := qcmock.NewMockStore(ctrl)
	qcs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)

	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	summaryStore.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, opts storage.RequestSummaryListOptions) (storage.RequestSummaryListResult, error) {
			assert.Equal(t, "q", opts.Queue)
			assert.Equal(t, int64(100), opts.StartTimeMs)
			assert.Equal(t, int64(200), opts.EndTimeMs)
			assert.Equal(t, []entity.RequestStatus{entity.RequestStatusBuilding, entity.RequestStatusLanded}, opts.Statuses)
			assert.Equal(t, defaultListPageSize, opts.Limit)
			require.Nil(t, opts.Cursor)
			return storage.RequestSummaryListResult{
				Requests: []entity.RequestSummary{
					{
						RequestID:     "q/2",
						Queue:         "q",
						ChangeURIs:    []string{"github://uber/repo/pull/2/abcdef"},
						Status:        entity.RequestStatusBuilding,
						LastError:     "last",
						Metadata:      map[string]string{"k": "v"},
						StartedAtMs:   150,
						UpdatedAtMs:   175,
						CompletedAtMs: 0,
						Terminal:      false,
					},
				},
				NextCursor: &storage.RequestSummaryCursor{StartedAtMs: 150, RequestID: "q/2"},
			}, nil
		},
	)

	controller := NewListController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, qcs)
	resp, err := controller.List(context.Background(), &pb.ListRequest{
		Queue:       "q",
		StartTimeMs: 100,
		EndTimeMs:   200,
		Statuses:    []string{"landed", "building", "landed"},
	})

	require.NoError(t, err)
	require.Len(t, resp.Requests, 1)
	assert.Equal(t, "q/2", resp.Requests[0].Sqid)
	assert.Equal(t, []string{"github://uber/repo/pull/2/abcdef"}, resp.Requests[0].ChangeUris)
	assert.Equal(t, "building", resp.Requests[0].Status)
	assert.Equal(t, "last", resp.Requests[0].LastError)
	assert.Equal(t, map[string]string{"k": "v"}, resp.Requests[0].Metadata)
	assert.Equal(t, int64(150), resp.Requests[0].StartedAtMs)
	assert.Equal(t, int64(175), resp.Requests[0].UpdatedAtMs)
	assert.NotEmpty(t, resp.NextPageToken)
}

func TestList_UsesPageTokenCursor(t *testing.T) {
	ctrl := gomock.NewController(t)

	qcs := qcmock.NewMockStore(ctrl)
	qcs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)

	token, err := encodeListPageToken(listPageToken{
		Queue:       "q",
		StartTimeMs: 100,
		EndTimeMs:   200,
		Statuses:    []string{"building"},
		StartedAtMs: 150,
		RequestID:   "q/2",
	})
	require.NoError(t, err)

	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	summaryStore.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, opts storage.RequestSummaryListOptions) (storage.RequestSummaryListResult, error) {
			require.NotNil(t, opts.Cursor)
			assert.Equal(t, int64(150), opts.Cursor.StartedAtMs)
			assert.Equal(t, "q/2", opts.Cursor.RequestID)
			assert.Equal(t, 10, opts.Limit)
			return storage.RequestSummaryListResult{}, nil
		},
	)

	controller := NewListController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, qcs)
	_, err = controller.List(context.Background(), &pb.ListRequest{
		Queue:       "q",
		StartTimeMs: 100,
		EndTimeMs:   200,
		Statuses:    []string{"building"},
		PageSize:    10,
		PageToken:   token,
	})
	require.NoError(t, err)
}

func TestList_Errors(t *testing.T) {
	tests := []struct {
		name             string
		req              *pb.ListRequest
		setupQueueConfig func(*qcmock.MockStore)
		wantInvalid      bool
		wantUnrecognized bool
	}{
		{
			name:        "empty queue",
			req:         &pb.ListRequest{StartTimeMs: 1, EndTimeMs: 2},
			wantInvalid: true,
		},
		{
			name:        "invalid window",
			req:         &pb.ListRequest{Queue: "q", StartTimeMs: 2, EndTimeMs: 1},
			wantInvalid: true,
		},
		{
			name:        "invalid page size",
			req:         &pb.ListRequest{Queue: "q", StartTimeMs: 1, EndTimeMs: 2, PageSize: -1},
			wantInvalid: true,
		},
		{
			name:        "unknown status",
			req:         &pb.ListRequest{Queue: "q", StartTimeMs: 1, EndTimeMs: 2, Statuses: []string{"wat"}},
			wantInvalid: true,
		},
		{
			name: "unrecognized queue",
			req:  &pb.ListRequest{Queue: "missing", StartTimeMs: 1, EndTimeMs: 2},
			setupQueueConfig: func(qcs *qcmock.MockStore) {
				qcs.EXPECT().Get(gomock.Any(), "missing").Return(entity.QueueConfig{}, queueconfig.ErrNotFound)
			},
			wantUnrecognized: true,
		},
		{
			name: "queue store failure",
			req:  &pb.ListRequest{Queue: "q", StartTimeMs: 1, EndTimeMs: 2},
			setupQueueConfig: func(qcs *qcmock.MockStore) {
				qcs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, fmt.Errorf("backend down"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			qcs := qcmock.NewMockStore(ctrl)
			if tt.setupQueueConfig != nil {
				tt.setupQueueConfig(qcs)
			}
			summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
			controller := NewListController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, qcs)

			_, err := controller.List(context.Background(), tt.req)

			require.Error(t, err)
			assert.Equal(t, tt.wantUnrecognized, IsUnrecognizedQueue(err))
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			if tt.wantUnrecognized {
				assert.True(t, errs.IsUserError(err))
			}
		})
	}
}

func TestList_RejectsMismatchedPageToken(t *testing.T) {
	ctrl := gomock.NewController(t)

	qcs := qcmock.NewMockStore(ctrl)
	qcs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)

	token, err := encodeListPageToken(listPageToken{
		Queue:       "other",
		StartTimeMs: 1,
		EndTimeMs:   2,
	})
	require.NoError(t, err)

	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	controller := NewListController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, qcs)

	_, err = controller.List(context.Background(), &pb.ListRequest{
		Queue:       "q",
		StartTimeMs: 1,
		EndTimeMs:   2,
		PageToken:   token,
	})

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}
