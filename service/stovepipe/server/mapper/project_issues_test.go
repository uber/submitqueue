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

package mapper

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/protobuf/proto"
)

func TestProjectIssuesRequests(t *testing.T) {
	require.Empty(t, ProtoToListRequestProjectsByIssueRequest(nil))
	require.Empty(t, ProtoToListRequestIssuesByProjectRequest(nil))
	require.Equal(t, entity.ListRequestProjectsByIssueRequest{Queue: "queue", RequestID: "7", TaskID: "test", UnitID: "unit", PageSize: 2, PageToken: "token"},
		ProtoToListRequestProjectsByIssueRequest(&pb.ListRequestProjectsByIssueRequest{Queue: "queue", RequestId: "7", TaskId: "test", UnitId: "unit", PageSize: 2, PageToken: "token"}))
	require.Equal(t, entity.ListRequestIssuesByProjectRequest{Queue: "queue", RequestID: "7", Project: "app", PageSize: 2, PageToken: "token"},
		ProtoToListRequestIssuesByProjectRequest(&pb.ListRequestIssuesByProjectRequest{Queue: "queue", RequestId: "7", Project: "app", PageSize: 2, PageToken: "token"}))
}

func TestProjectIssuesResult(t *testing.T) {
	result := entity.ListRequestProjectIssuesResult{Attributions: []entity.ProjectIssueAttribution{
		{TaskID: "test", UnitID: "unit", Project: "app"}, {TaskID: "test", UnitID: "unit", Project: "dependent"},
	}, AttributionComplete: true, NextPageToken: "next"}
	want := &pb.ListRequestProjectIssuesResponse{Attributions: []*pb.ProjectIssueAttribution{
		{TaskId: "test", UnitId: "unit", Project: "app"}, {TaskId: "test", UnitId: "unit", Project: "dependent"},
	}, AttributionComplete: true, NextPageToken: "next"}
	require.True(t, proto.Equal(want, ListRequestProjectIssuesResultToProto(result)))
	require.True(t, proto.Equal(&pb.ListRequestProjectIssuesResponse{}, ListRequestProjectIssuesResultToProto(entity.ListRequestProjectIssuesResult{})))
}
