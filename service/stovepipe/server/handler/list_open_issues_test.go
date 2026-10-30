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

func TestListOpenIssues(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "success"},
		{name: "invalid input", err: controller.ErrInvalidRequest, code: codes.InvalidArgument},
		{name: "canceled", err: context.Canceled, code: codes.Canceled},
		{name: "deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
		{name: "storage failure", err: assert.AnError, code: codes.Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := controllermock.NewMockListOpenIssuesController(gomock.NewController(t))
			result := entity.ListOpenIssuesResult{Issues: []entity.IssueSummary{{TaskID: "task", Status: entity.IssueStatusOpen, FirstSeenBuildID: "opening"}}, NextPageToken: "next"}
			c.EXPECT().ListOpenIssues(gomock.Any(), entity.ListOpenIssuesRequest{Queue: "queue", PageSize: 10, PageToken: "token"}).Return(result, tt.err)
			server := &StovepipeServer{openIssuesController: c}
			got, err := server.ListOpenIssues(context.Background(), &pb.ListOpenIssuesRequest{Queue: "queue", PageSize: 10, PageToken: "token"})
			if tt.err != nil {
				require.Equal(t, tt.code, status.Code(err))
				require.Nil(t, got)
				if tt.err == assert.AnError {
					require.ErrorIs(t, err, tt.err)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, "task", got.GetIssues()[0].GetTaskId())
				require.Equal(t, "open", got.GetIssues()[0].GetStatus())
				require.Equal(t, "next", got.GetNextPageToken())
			}
		})
	}
}
