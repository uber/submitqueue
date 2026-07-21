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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestGetRequestSummaryByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	uriStore := storagemock.NewMockRequestURIStore(ctrl)
	summaryStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.RequestSummary{
		RequestID:    "test-queue/1",
		Queue:        "test-queue",
		ChangeURIs:   []string{"github://uber/repo/pull/1/abc"},
		ReceivedAtMs: 100,
		Status:       entity.RequestStatusValidating,
		LastError:    "boom",
		Metadata:     map[string]string{"k": "v"},
	}, nil)

	controller := NewRequestSummaryController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, uriStore)
	summary, err := controller.GetRequestSummaryByID(context.Background(), entity.GetRequestSummaryByIDRequest{ID: "test-queue/1"})

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", summary.RequestID)
	assert.Equal(t, "test-queue", summary.Queue)
	assert.Equal(t, []string{"github://uber/repo/pull/1/abc"}, summary.ChangeURIs)
	assert.Equal(t, int64(100), summary.ReceivedAtMs)
	assert.Equal(t, entity.RequestStatusValidating, summary.Status)
	assert.Equal(t, "boom", summary.LastError)
	assert.Equal(t, map[string]string{"k": "v"}, summary.Metadata)
}

func TestGetRequestSummaryByChangeURI(t *testing.T) {
	ctrl := gomock.NewController(t)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	uriStore := storagemock.NewMockRequestURIStore(ctrl)
	uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return([]entity.RequestURI{
		{ChangeURI: "uri", ReceivedAtMs: 200, RequestID: "queue/2"},
		{ChangeURI: "uri", ReceivedAtMs: 100, RequestID: "queue/1"},
	}, nil)
	summaryStore.EXPECT().Get(gomock.Any(), "queue/2").Return(entity.RequestSummary{RequestID: "queue/2", ReceivedAtMs: 200, Status: entity.RequestStatusLanded, ChangeURIs: []string{}}, nil)
	summaryStore.EXPECT().Get(gomock.Any(), "queue/1").Return(entity.RequestSummary{RequestID: "queue/1", ReceivedAtMs: 100, Status: entity.RequestStatusError, ChangeURIs: []string{}}, nil)

	controller := NewRequestSummaryController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, uriStore)
	summaries, err := controller.GetRequestSummaryByChangeURI(context.Background(), entity.GetRequestSummaryByChangeURIRequest{ChangeURI: "uri"})

	require.NoError(t, err)
	require.Len(t, summaries, 2)
	assert.Equal(t, []string{"queue/2", "queue/1"}, []string{summaries[0].RequestID, summaries[1].RequestID})
}

func TestStatusErrors(t *testing.T) {
	backendErr := fmt.Errorf("backend down")
	tests := []struct {
		name         string
		call         func(*RequestSummaryController) error
		setup        func(*storagemock.MockRequestSummaryStore, *storagemock.MockRequestURIStore)
		wantInvalid  bool
		wantNotFound bool
		wantTooMany  bool
		wantInternal bool
		wantUser     bool
	}{
		{
			name: "empty sqid",
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByID(context.Background(), entity.GetRequestSummaryByIDRequest{})
				return err
			},
			wantInvalid: true,
			wantUser:    true,
		},
		{
			name: "empty change URI",
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByChangeURI(context.Background(), entity.GetRequestSummaryByChangeURIRequest{})
				return err
			},
			wantInvalid: true,
			wantUser:    true,
		},
		{
			name: "sqid not found",
			setup: func(summaryStore *storagemock.MockRequestSummaryStore, _ *storagemock.MockRequestURIStore) {
				summaryStore.EXPECT().Get(gomock.Any(), "missing/1").Return(entity.RequestSummary{}, errs.ErrNotFound)
			},
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByID(context.Background(), entity.GetRequestSummaryByIDRequest{ID: "missing/1"})
				return err
			},
			wantNotFound: true,
			wantUser:     true,
		},
		{
			name: "sqid still accepting",
			setup: func(summaryStore *storagemock.MockRequestSummaryStore, _ *storagemock.MockRequestURIStore) {
				summaryStore.EXPECT().Get(gomock.Any(), "queue/1").Return(entity.RequestSummary{
					RequestID: "queue/1",
					Status:    entity.RequestStatusAccepting,
				}, nil)
			},
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByID(context.Background(), entity.GetRequestSummaryByIDRequest{ID: "queue/1"})
				return err
			},
			wantNotFound: true,
			wantUser:     true,
		},
		{
			name: "sqid storage failure",
			setup: func(summaryStore *storagemock.MockRequestSummaryStore, _ *storagemock.MockRequestURIStore) {
				summaryStore.EXPECT().Get(gomock.Any(), "queue/1").Return(entity.RequestSummary{}, backendErr)
			},
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByID(context.Background(), entity.GetRequestSummaryByIDRequest{ID: "queue/1"})
				return err
			},
		},
		{
			name: "change URI not found",
			setup: func(_ *storagemock.MockRequestSummaryStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return([]entity.RequestURI{}, nil)
			},
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByChangeURI(context.Background(), entity.GetRequestSummaryByChangeURIRequest{ChangeURI: "uri"})
				return err
			},
			wantNotFound: true,
			wantUser:     true,
		},
		{
			name: "too many change matches",
			setup: func(_ *storagemock.MockRequestSummaryStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return(make([]entity.RequestURI, 101), nil)
			},
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByChangeURI(context.Background(), entity.GetRequestSummaryByChangeURIRequest{ChangeURI: "uri"})
				return err
			},
			wantTooMany: true,
			wantUser:    true,
		},
		{
			name: "mapped summary missing",
			setup: func(summaryStore *storagemock.MockRequestSummaryStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return([]entity.RequestURI{{RequestID: "missing/1"}}, nil)
				summaryStore.EXPECT().Get(gomock.Any(), "missing/1").Return(entity.RequestSummary{}, errs.ErrNotFound)
			},
			call: func(c *RequestSummaryController) error {
				_, err := c.GetRequestSummaryByChangeURI(context.Background(), entity.GetRequestSummaryByChangeURIRequest{ChangeURI: "uri"})
				return err
			},
			wantInternal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
			uriStore := storagemock.NewMockRequestURIStore(ctrl)
			if tt.setup != nil {
				tt.setup(summaryStore, uriStore)
			}
			controller := NewRequestSummaryController(zap.NewNop().Sugar(), tally.NoopScope, summaryStore, uriStore)

			err := tt.call(controller)

			require.Error(t, err)
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			assert.Equal(t, tt.wantNotFound, IsRequestNotFound(err))
			assert.Equal(t, tt.wantTooMany, IsTooManyChangeRequests(err))
			assert.Equal(t, tt.wantInternal, IsInternalConsistency(err))
			assert.Equal(t, tt.wantUser, errs.IsUserError(err))
		})
	}
}
