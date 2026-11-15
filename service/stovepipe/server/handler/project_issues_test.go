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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/controller"
	controllermock "github.com/uber/submitqueue/stovepipe/controller/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProjectIssues(t *testing.T) {
	for _, byProject := range []bool{false, true} {
		for _, tt := range []struct {
			name string
			err  error
			code codes.Code
		}{
			{name: "success"},
			{name: "invalid", err: fmt.Errorf("bad lookup: %w", controller.ErrInvalidRequest), code: codes.InvalidArgument},
			{name: "not found", err: fmt.Errorf("missing history: %w", storage.ErrNotFound), code: codes.NotFound},
			{name: "cancelled", err: context.Canceled, code: codes.Canceled},
			{name: "deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
			{name: "failure", err: assert.AnError, code: codes.Unknown},
		} {
			t.Run(fmt.Sprintf("byProject=%t/%s", byProject, tt.name), func(t *testing.T) {
				c := controllermock.NewMockProjectIssuesController(gomock.NewController(t))
				result := entity.ListRequestProjectIssuesResult{
					Attributions:        []entity.ProjectIssueAttribution{{TaskID: "test", UnitID: "unit", Project: "app"}},
					AttributionComplete: true, NextPageToken: "next",
				}
				server := NewStovepipeServer(nil, nil, nil, nil, nil, nil, nil, nil, c)
				var got *pb.ListRequestProjectIssuesResponse
				var err error
				if byProject {
					c.EXPECT().ListRequestIssuesByProject(gomock.Any(), entity.ListRequestIssuesByProjectRequest{Queue: "queue", RequestID: "7", Project: "app", PageSize: 2, PageToken: "token"}).Return(result, tt.err)
					got, err = server.ListRequestIssuesByProject(context.Background(), &pb.ListRequestIssuesByProjectRequest{Queue: "queue", RequestId: "7", Project: "app", PageSize: 2, PageToken: "token"})
				} else {
					c.EXPECT().ListRequestProjectsByIssue(gomock.Any(), entity.ListRequestProjectsByIssueRequest{Queue: "queue", RequestID: "7", TaskID: "test", UnitID: "unit", PageSize: 2, PageToken: "token"}).Return(result, tt.err)
					got, err = server.ListRequestProjectsByIssue(context.Background(), &pb.ListRequestProjectsByIssueRequest{Queue: "queue", RequestId: "7", TaskId: "test", UnitId: "unit", PageSize: 2, PageToken: "token"})
				}
				if tt.err != nil {
					require.Equal(t, tt.code, status.Code(err))
					require.Nil(t, got)
					if tt.err == assert.AnError {
						require.ErrorIs(t, err, tt.err)
					}
				} else {
					require.NoError(t, err)
					require.True(t, got.GetAttributionComplete())
					require.Equal(t, "next", got.GetNextPageToken())
					require.Len(t, got.GetAttributions(), 1)
					require.Equal(t, "test", got.Attributions[0].GetTaskId())
					require.Equal(t, "unit", got.Attributions[0].GetUnitId())
					require.Equal(t, "app", got.Attributions[0].GetProject())
				}
			})
		}
	}
}
