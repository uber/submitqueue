// Copyright (c) 2026 Uber Technologies, Inc.
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

package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/controller"
	controllermock "github.com/uber/submitqueue/stovepipe/controller/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListIssueHistory(t *testing.T) {
	request := entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", UnitID: "unit", PageSize: 2, PageToken: "token"}
	result := entity.ListIssueHistoryResult{
		Events: []entity.IssueHistoryEntry{{
			Log:       entity.IssueLog{Event: entity.IssueEventReported, BuildID: "build/2", TimestampMs: 100},
			RequestID: "12",
		}},
		NextPageToken:       "next",
		AcceptedAtOrAfterMs: 100,
		AcceptedBeforeMs:    200,
	}

	for _, tt := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "success", code: codes.OK},
		{name: "invalid request", err: controller.ErrInvalidRequest, code: codes.InvalidArgument},
		{name: "canceled", err: context.Canceled, code: codes.Canceled},
		{name: "deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
		{name: "dependency failure", err: assert.AnError, code: codes.Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := controllermock.NewMockListIssueHistoryController(gomock.NewController(t))
			c.EXPECT().ListIssueHistory(ctx, request).Return(result, tt.err)
			server := NewStovepipeServer(nil, nil, nil, nil, nil, nil, nil, nil, nil, c)

			response, err := server.ListIssueHistory(ctx, &pb.ListIssueHistoryRequest{Queue: "queue", TaskId: "task", UnitId: "unit", PageSize: 2, PageToken: "token"})
			require.Equal(t, tt.code, status.Code(err))
			if tt.err != nil {
				require.Nil(t, response)
				return
			}

			require.Equal(t, &pb.ListIssueHistoryResponse{
				Events:              []*pb.IssueHistoryEvent{{Event: "reported", BuildId: "build/2", TimestampMs: 100, RequestId: "12"}},
				NextPageToken:       "next",
				AcceptedAtOrAfterMs: 100,
				AcceptedBeforeMs:    200,
			}, response)
		})
	}
}
