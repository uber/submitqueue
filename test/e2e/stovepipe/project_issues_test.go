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
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	projectissuescore "github.com/uber/submitqueue/stovepipe/core/projectissues"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const projectIssuesQueue = "e2e-stovepipe/project-issues"

func (s *StovepipeE2ESuite) TestProjectIssues() {
	t := s.T()
	stores, err := s.appStorage.For(projectIssuesQueue)
	require.NoError(t, err)
	request := entity.Request{ID: "900", Queue: projectIssuesQueue, TerminalBuildID: "build/900"}
	history := requestlog.NewMaterializer(tally.NoopScope)
	metadata := map[string]string{requestlog.MetadataKeyBuildID: request.TerminalBuildID}
	require.NoError(t, history.PersistLog(s.ctx, stores, requestlog.NewRequestEventLog(request, entity.RequestEventBuildAnalysisCompleted, request.TerminalBuildID, metadata)))
	materializer := projectissuescore.NewMaterializer(tally.NoopScope)
	for _, pair := range []entity.ProjectIssueAttribution{
		{TaskID: "test", UnitID: "unit", Project: "a"},
		{TaskID: "test", UnitID: "unit", Project: "b"},
		{TaskID: "lint", UnitID: "unit", Project: "a"},
	} {
		require.NoError(t, materializer.PersistAttribution(s.ctx, stores, request.ID, pair))
	}
	issueQuery := &pb.ListRequestProjectsByIssueRequest{Queue: request.Queue, RequestId: request.ID, TaskId: "test", UnitId: "unit", PageSize: 1}
	projectQuery := &pb.ListRequestIssuesByProjectRequest{Queue: request.Queue, RequestId: request.ID, Project: "a", PageSize: 1}
	pending, err := s.client.ListRequestProjectsByIssue(s.ctx, issueQuery)
	require.NoError(t, err)
	require.False(t, pending.GetAttributionComplete())
	require.Empty(t, pending.GetAttributions())
	pending, err = s.client.ListRequestIssuesByProject(s.ctx, projectQuery)
	require.NoError(t, err)
	require.False(t, pending.GetAttributionComplete())
	require.Empty(t, pending.GetAttributions())
	require.NoError(t, history.PersistLog(s.ctx, stores, requestlog.NewRequestEventLog(request, entity.RequestEventProjectIssuesRecorded, request.TerminalBuildID, metadata)))
	first, err := s.client.ListRequestProjectsByIssue(s.ctx, issueQuery)
	require.NoError(t, err)
	require.True(t, first.GetAttributionComplete())
	require.Len(t, first.GetAttributions(), 1)
	require.Equal(t, "a", first.Attributions[0].GetProject())
	require.NotEmpty(t, first.GetNextPageToken())
	issueQuery.PageToken = first.GetNextPageToken()
	issueQuery.PageSize = 10
	last, err := s.client.ListRequestProjectsByIssue(s.ctx, issueQuery)
	require.NoError(t, err)
	require.Len(t, last.GetAttributions(), 1)
	require.Equal(t, "b", last.Attributions[0].GetProject())
	require.Empty(t, last.GetNextPageToken())
	first, err = s.client.ListRequestIssuesByProject(s.ctx, projectQuery)
	require.NoError(t, err)
	require.True(t, first.GetAttributionComplete())
	require.Len(t, first.GetAttributions(), 1)
	require.Equal(t, "lint", first.Attributions[0].GetTaskId())
	require.NotEmpty(t, first.GetNextPageToken())
	projectQuery.PageToken = first.GetNextPageToken()
	last, err = s.client.ListRequestIssuesByProject(s.ctx, projectQuery)
	require.NoError(t, err)
	require.Len(t, last.GetAttributions(), 1)
	require.Equal(t, "test", last.Attributions[0].GetTaskId())
	require.Empty(t, last.GetNextPageToken())
	projectQuery.Project = "b"
	_, err = s.client.ListRequestIssuesByProject(s.ctx, projectQuery)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	issueQuery.PageToken = ""
	issueQuery.UnitId = "missing"
	empty, err := s.client.ListRequestProjectsByIssue(s.ctx, issueQuery)
	require.NoError(t, err)
	require.True(t, empty.GetAttributionComplete())
	require.Empty(t, empty.GetAttributions())
	issueQuery.RequestId = "901"
	_, err = s.client.ListRequestProjectsByIssue(s.ctx, issueQuery)
	require.Equal(t, codes.NotFound, status.Code(err))
}
