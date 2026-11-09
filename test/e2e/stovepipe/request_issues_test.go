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
	"github.com/uber/submitqueue/stovepipe/core/issuelog"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const requestIssuesQueue = "e2e-stovepipe/request-issues"

func (s *StovepipeE2ESuite) TestGetIssuesByRequestID() {
	t := s.T()
	const queue, requestID = requestIssuesQueue, "900"
	stores, err := s.appStorage.For(queue)
	require.NoError(t, err)
	materializer := issuelog.NewMaterializer(tally.NoopScope)
	require.NoError(t, stores.GetBuildStore().Create(s.ctx, entity.Build{ID: "b/900", RequestID: requestID, Status: entity.BuildStatusFailed, Version: 1}))
	for _, log := range []entity.IssueLog{
		{TaskID: "a", Event: entity.IssueEventOpened, BuildID: "b/900", FirstSeenBuildID: "b/900", TimestampMs: 1000},
		{TaskID: "a", UnitID: "unit", Event: entity.IssueEventReported, BuildID: "b/900", FirstSeenBuildID: "b/899"},
		{TaskID: "b", UnitID: "unit", Event: entity.IssueEventClosed, BuildID: "b/900", FirstSeenBuildID: "b/899", TimestampMs: 1000},
	} {
		require.NoError(t, materializer.PersistLog(s.ctx, stores, log, log.FirstSeenBuildID))
	}
	request := entity.Request{ID: requestID, Queue: queue}
	requestMaterializer := requestlog.NewMaterializer(tally.NoopScope)
	require.NoError(t, requestMaterializer.PersistLog(s.ctx, stores, requestlog.NewRequestEventLog(request, entity.RequestEventIssueTrackingStarted, "", nil)))
	first, err := s.client.GetIssuesByRequestID(s.ctx, &pb.GetIssuesByRequestIDRequest{Queue: queue, RequestId: requestID, PageSize: 2})
	require.NoError(t, err)
	require.Equal(t, "in_progress", first.GetStatus())
	require.Len(t, first.GetIssues(), 2)
	require.Equal(t, "new", first.GetIssues()[0].GetKind())
	require.Equal(t, "existing", first.GetIssues()[1].GetKind())
	require.NotEmpty(t, first.GetNextPageToken())
	require.NoError(t, requestMaterializer.PersistLog(s.ctx, stores, requestlog.NewRequestEventLog(request, entity.RequestEventIssuesReconciled, "b/900", nil)))
	last, err := s.client.GetIssuesByRequestID(s.ctx, &pb.GetIssuesByRequestIDRequest{Queue: queue, RequestId: requestID, PageSize: 2, PageToken: first.GetNextPageToken()})
	require.NoError(t, err)
	require.Equal(t, "complete", last.GetStatus())
	require.Len(t, last.GetIssues(), 1)
	require.Equal(t, "closed", last.GetIssues()[0].GetKind())
	require.Equal(t, "b/899", last.GetIssues()[0].GetFirstSeenBuildId())
	require.Empty(t, last.GetNextPageToken())
	_, err = s.client.GetIssuesByRequestID(s.ctx, &pb.GetIssuesByRequestIDRequest{Queue: queue, RequestId: "901", PageToken: first.GetNextPageToken()})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
