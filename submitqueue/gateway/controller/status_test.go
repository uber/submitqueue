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
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestStatus_ReturnsCurrentState(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockRequestLogStore(ctrl)
	store.EXPECT().List(gomock.Any(), "test-queue/1").Return([]entity.RequestLog{
		{RequestID: "test-queue/1", TimestampMs: 100, Status: entity.RequestStatusAccepted},
		{RequestID: "test-queue/1", TimestampMs: 200, Status: entity.RequestStatusValidating, LastError: "boom", Metadata: map[string]string{"k": "v"}},
	}, nil)

	controller := NewStatusController(zap.NewNop().Sugar(), tally.NoopScope, store)

	resp, err := controller.Status(context.Background(), &pb.StatusRequest{Sqid: "test-queue/1"})

	require.NoError(t, err)
	assert.Equal(t, string(entity.RequestStatusValidating), resp.Status)
	assert.Equal(t, "boom", resp.LastError)
	assert.Equal(t, map[string]string{"k": "v"}, resp.Metadata)
}

func TestStatus_Errors(t *testing.T) {
	tests := []struct {
		name          string
		sqid          string
		setupStore    func(*storagemock.MockRequestLogStore)
		wantInvalid   bool
		wantNotFound  bool
		wantUserError bool
	}{
		{
			name:          "empty sqid is an invalid request",
			sqid:          "",
			wantInvalid:   true,
			wantUserError: true,
		},
		{
			name: "unknown sqid maps to not found",
			sqid: "missing/1",
			setupStore: func(s *storagemock.MockRequestLogStore) {
				s.EXPECT().List(gomock.Any(), "missing/1").Return(nil, storage.ErrNotFound)
			},
			wantNotFound:  true,
			wantUserError: true,
		},
		{
			name: "store failure propagates as infra error",
			sqid: "test-queue/1",
			setupStore: func(s *storagemock.MockRequestLogStore) {
				s.EXPECT().List(gomock.Any(), "test-queue/1").Return(nil, fmt.Errorf("log backend down"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockRequestLogStore(ctrl)
			if tt.setupStore != nil {
				tt.setupStore(store)
			}

			controller := NewStatusController(zap.NewNop().Sugar(), tally.NoopScope, store)

			_, err := controller.Status(context.Background(), &pb.StatusRequest{Sqid: tt.sqid})

			require.Error(t, err)
			// IsRequestNotFound is a precise type check; IsInvalidRequest matches any
			// user error (see errs.userError.Is), so only assert it where it is the
			// defining classification.
			assert.Equal(t, tt.wantNotFound, IsRequestNotFound(err))
			assert.Equal(t, tt.wantUserError, errs.IsUserError(err))
			assert.False(t, errs.IsRetryable(err))
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}

			if tt.wantNotFound {
				var typed *RequestNotFoundError
				require.ErrorAs(t, err, &typed)
				assert.Equal(t, tt.sqid, typed.Sqid)
			}
		})
	}
}
