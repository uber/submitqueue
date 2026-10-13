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
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestGetRequestHistoryByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	uriStore := storagemock.NewMockRequestURIStore(ctrl)
	logStore.EXPECT().List(gomock.Any(), "1").Return([]entity.RequestLog{
		{RequestID: "1", TimestampMs: 10, Status: entity.RequestStatusAccepted, Metadata: map[string]string{}},
		{RequestID: "1", TimestampMs: 20, Status: entity.RequestStatusStarted, LastError: "retry", Metadata: map[string]string{"attempt": "1"}},
		{RequestID: "1", TimestampMs: 20, Status: entity.RequestStatusStarted, LastError: "retry", Metadata: map[string]string{"attempt": "1"}},
	}, nil)

	controller := NewRequestHistoryController(zap.NewNop().Sugar(), tally.NoopScope, readModelFactory(ctrl, nil, logStore, uriStore))
	events, err := controller.GetRequestHistoryByID(context.Background(), entity.GetRequestHistoryByIDRequest{ID: "1", Queue: "queue"})

	require.NoError(t, err)
	require.Len(t, events, 3)
	assert.Equal(t, []entity.RequestStatus{entity.RequestStatusAccepted, entity.RequestStatusStarted, entity.RequestStatusStarted}, []entity.RequestStatus{events[0].Status, events[1].Status, events[2].Status})
	assert.Equal(t, int64(20), events[1].TimestampMs)
	assert.Equal(t, "retry", events[1].LastError)
	assert.Equal(t, map[string]string{"attempt": "1"}, events[1].Metadata)
}

func TestGetRequestHistoryByChangeURI(t *testing.T) {
	ctrl := gomock.NewController(t)
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	uriStore := storagemock.NewMockRequestURIStore(ctrl)
	uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return([]entity.RequestURI{
		{ChangeURI: "uri", RequestID: "10"},
		{ChangeURI: "uri", RequestID: "2"},
		{ChangeURI: "uri", RequestID: "3"},
		{ChangeURI: "uri", RequestID: "1"},
		{ChangeURI: "uri", RequestID: "4"},
	}, nil)
	logStore.EXPECT().List(gomock.Any(), "10").Return([]entity.RequestLog{{RequestID: "10", TimestampMs: 10, Status: entity.RequestStatusLanded}}, nil)
	logStore.EXPECT().List(gomock.Any(), "2").Return([]entity.RequestLog{{RequestID: "2", TimestampMs: 2, Status: entity.RequestStatusStarted}}, nil)
	logStore.EXPECT().List(gomock.Any(), "3").Return(nil, basestorage.ErrNotFound)
	logStore.EXPECT().List(gomock.Any(), "1").Return([]entity.RequestLog{{RequestID: "1", TimestampMs: 1, Status: entity.RequestStatusAccepted}}, nil)
	logStore.EXPECT().List(gomock.Any(), "4").Return([]entity.RequestLog{{RequestID: "4", TimestampMs: 4, Status: entity.RequestStatusError}}, nil)

	controller := NewRequestHistoryController(zap.NewNop().Sugar(), tally.NoopScope, readModelFactory(ctrl, nil, logStore, uriStore))
	histories, err := controller.GetRequestHistoryByChangeURI(context.Background(), entity.GetRequestHistoryByChangeURIRequest{ChangeURI: "uri", Queue: "queue"})

	require.NoError(t, err)
	require.Len(t, histories, 4)
	assert.Equal(t, []string{"1", "2", "4", "10"}, []string{
		histories[0].RequestID,
		histories[1].RequestID,
		histories[2].RequestID,
		histories[3].RequestID,
	})
}

func TestHistoryErrors(t *testing.T) {
	backendErr := fmt.Errorf("backend down")
	tests := []struct {
		name         string
		call         func(RequestHistoryController) error
		setup        func(*storagemock.MockRequestLogStore, *storagemock.MockRequestURIStore)
		wantInvalid  bool
		wantNotFound bool
		wantTooMany  bool
		wantInternal bool
		wantUser     bool
	}{
		{
			name: "empty sqid",
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByID(context.Background(), entity.GetRequestHistoryByIDRequest{})
				return err
			},
			wantInvalid: true,
			wantUser:    true,
		},
		{
			name: "composite sqid",
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByID(context.Background(), entity.GetRequestHistoryByIDRequest{ID: "queue/1", Queue: "queue"})
				return err
			},
			wantInvalid: true,
			wantUser:    true,
		},
		{
			name: "empty change URI",
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByChangeURI(context.Background(), entity.GetRequestHistoryByChangeURIRequest{})
				return err
			},
			wantInvalid: true,
			wantUser:    true,
		},
		{
			name: "sqid not found",
			setup: func(logStore *storagemock.MockRequestLogStore, _ *storagemock.MockRequestURIStore) {
				logStore.EXPECT().List(gomock.Any(), "1").Return(nil, basestorage.ErrNotFound)
			},
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByID(context.Background(), entity.GetRequestHistoryByIDRequest{ID: "1", Queue: "missing"})
				return err
			},
			wantNotFound: true,
			wantUser:     true,
		},
		{
			name: "sqid storage failure",
			setup: func(logStore *storagemock.MockRequestLogStore, _ *storagemock.MockRequestURIStore) {
				logStore.EXPECT().List(gomock.Any(), "1").Return(nil, backendErr)
			},
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByID(context.Background(), entity.GetRequestHistoryByIDRequest{ID: "1", Queue: "queue"})
				return err
			},
		},
		{
			name: "change URI not found",
			setup: func(_ *storagemock.MockRequestLogStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return(nil, nil)
			},
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByChangeURI(context.Background(), entity.GetRequestHistoryByChangeURIRequest{ChangeURI: "uri", Queue: "queue"})
				return err
			},
			wantNotFound: true,
			wantUser:     true,
		},
		{
			name: "too many change matches",
			setup: func(_ *storagemock.MockRequestLogStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return(make([]entity.RequestURI, 101), nil)
			},
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByChangeURI(context.Background(), entity.GetRequestHistoryByChangeURIRequest{ChangeURI: "uri", Queue: "queue"})
				return err
			},
			wantTooMany: true,
			wantUser:    true,
		},
		{
			name: "all mapped logs absent",
			setup: func(logStore *storagemock.MockRequestLogStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return([]entity.RequestURI{{RequestID: "1"}}, nil)
				logStore.EXPECT().List(gomock.Any(), "1").Return(nil, basestorage.ErrNotFound)
			},
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByChangeURI(context.Background(), entity.GetRequestHistoryByChangeURIRequest{ChangeURI: "uri", Queue: "queue"})
				return err
			},
			wantNotFound: true,
			wantUser:     true,
		},
		{
			name: "mapped sqid malformed",
			setup: func(_ *storagemock.MockRequestLogStore, uriStore *storagemock.MockRequestURIStore) {
				uriStore.EXPECT().ListByURI(gomock.Any(), "uri", 101).Return([]entity.RequestURI{{RequestID: "malformed"}}, nil)
			},
			call: func(c RequestHistoryController) error {
				_, err := c.GetRequestHistoryByChangeURI(context.Background(), entity.GetRequestHistoryByChangeURIRequest{ChangeURI: "uri", Queue: "queue"})
				return err
			},
			wantInternal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			logStore := storagemock.NewMockRequestLogStore(ctrl)
			uriStore := storagemock.NewMockRequestURIStore(ctrl)
			if tt.setup != nil {
				tt.setup(logStore, uriStore)
			}
			controller := NewRequestHistoryController(zap.NewNop().Sugar(), tally.NoopScope, readModelFactory(ctrl, nil, logStore, uriStore))

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
