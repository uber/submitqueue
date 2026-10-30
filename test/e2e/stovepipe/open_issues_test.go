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

package e2e_test

import (
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const openIssuesQueue = "e2e-stovepipe/open-issues"

func (s *StovepipeE2ESuite) TestListOpenIssues() {
	t := s.T()
	stores, err := s.appStorage.For(openIssuesQueue)
	require.NoError(t, err)
	for _, summary := range []entity.IssueSummary{
		{TaskID: "a", Status: entity.IssueStatusClosed, FirstSeenBuildID: "build/a", RequestID: "100", BuildID: "build/a-closed", ChangedAtMs: 1000, Version: 1},
		{TaskID: "b", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/b-new", RequestID: "100", BuildID: "build/b-new", ChangedAtMs: 1000, Version: 1},
		{TaskID: "c", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/c", RequestID: "100", BuildID: "build/c", ChangedAtMs: 1000, Version: 1},
		{TaskID: "d", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/d", RequestID: "100", BuildID: "build/d", ChangedAtMs: 1000, Version: 1},
		{TaskID: "e", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/e", RequestID: "100", BuildID: "build/e", ChangedAtMs: 1000, Version: 1},
		{TaskID: "f", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/f", RequestID: "100", BuildID: "build/f", ChangedAtMs: 1000, Version: 1},
	} {
		require.NoError(t, stores.GetIssueSummaryStore().Create(s.ctx, summary))
		opening := summary.FirstSeenBuildID
		if summary.TaskID == "b" {
			opening = "build/b-old"
		}
		require.NoError(t, stores.GetOpenIssueStore().Create(s.ctx, entity.OpenIssue{TaskID: summary.TaskID, UnitID: summary.UnitID, FirstSeenBuildID: opening}))
	}

	first, err := s.client.ListOpenIssues(s.ctx, &pb.ListOpenIssuesRequest{Queue: openIssuesQueue, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, first.GetIssues(), 2)
	require.Equal(t, "c", first.GetIssues()[0].GetTaskId())
	require.Equal(t, "d", first.GetIssues()[1].GetTaskId())
	require.NotEmpty(t, first.GetNextPageToken())
	_, err = s.client.ListOpenIssues(s.ctx, &pb.ListOpenIssuesRequest{Queue: listOtherQueue, PageToken: first.GetNextPageToken()})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	second, err := s.client.ListOpenIssues(s.ctx, &pb.ListOpenIssuesRequest{Queue: openIssuesQueue, PageSize: 1, PageToken: first.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, second.GetIssues(), 1)
	require.Equal(t, "e", second.GetIssues()[0].GetTaskId())
	require.Empty(t, second.GetIssues()[0].GetUnitId())
	require.NotEmpty(t, second.GetNextPageToken())

	third, err := s.client.ListOpenIssues(s.ctx, &pb.ListOpenIssuesRequest{Queue: openIssuesQueue, PageSize: 200, PageToken: second.GetNextPageToken()})
	require.NoError(t, err)
	require.Len(t, third.GetIssues(), 1)
	require.True(t, proto.Equal(&pb.IssueSummary{TaskId: "f", UnitId: "unit", Status: "open", FirstSeenBuildId: "build/f", RequestId: "100", BuildId: "build/f", ChangedAtMs: 1000}, third.GetIssues()[0]))
	require.Empty(t, third.GetNextPageToken())

	closed, err := stores.GetIssueSummaryStore().Get(s.ctx, "d", "unit")
	require.NoError(t, err)
	closed.Status, closed.BuildID, closed.RequestID, closed.ChangedAtMs = entity.IssueStatusClosed, "build/d-closed", "101", 2000
	require.NoError(t, stores.GetIssueSummaryStore().Update(s.ctx, closed, 1, 2))
	current, err := s.client.ListOpenIssues(s.ctx, &pb.ListOpenIssuesRequest{Queue: openIssuesQueue, PageSize: 200})
	require.NoError(t, err)
	require.Len(t, current.GetIssues(), 3)
	require.Equal(t, "c", current.GetIssues()[0].GetTaskId())
	require.Equal(t, "e", current.GetIssues()[1].GetTaskId())
	require.Equal(t, "f", current.GetIssues()[2].GetTaskId())
	require.Empty(t, current.GetNextPageToken())
}
