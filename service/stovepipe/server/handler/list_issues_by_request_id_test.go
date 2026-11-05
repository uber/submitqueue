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

func TestListIssuesByRequestID(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "success"},
		{name: "invalid", err: controller.ErrInvalidRequest, code: codes.InvalidArgument},
		{name: "missing", err: &controller.RequestHistoryByIDNotFoundError{RequestID: "7"}, code: codes.NotFound},
		{name: "cancelled", err: context.Canceled, code: codes.Canceled},
		{name: "deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
		{name: "storage failure", err: assert.AnError, code: codes.Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := controllermock.NewMockListIssuesByRequestIDController(gomock.NewController(t))
			result := entity.ListIssuesByRequestIDResult{Queue: "queue", RequestID: "7", WinningBuildID: "build", Issues: []entity.RequestIssue{{BuildID: "build", TaskID: "task", UnitID: "unit", FixID: "repair/1"}}, Builds: []entity.IssueBuild{{BuildID: "build", AnalysisStatus: entity.IssueAnalysisCompleted}}, NextPageToken: "next"}
			c.EXPECT().ListIssuesByRequestID(gomock.Any(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageSize: 10, PageToken: "token"}).Return(result, tt.err)
			server := &StovepipeServer{requestIssuesController: c}
			got, err := server.ListIssuesByRequestID(context.Background(), &pb.ListIssuesByRequestIDRequest{Queue: "queue", RequestId: "7", PageSize: 10, PageToken: "token"})
			if tt.err != nil {
				require.Nil(t, got)
				require.Equal(t, tt.code, status.Code(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, &pb.ListIssuesByRequestIDResponse{Queue: "queue", RequestId: "7", WinningBuildId: "build", Issues: []*pb.RequestIssue{{BuildId: "build", TaskId: "task", UnitId: "unit", FixId: "repair/1"}}, Builds: []*pb.IssueBuild{{BuildId: "build", AnalysisStatus: "completed"}}, NextPageToken: "next"}, got)
		})
	}
}
