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
)

func TestProtoToListOpenIssuesRequest(t *testing.T) {
	require.Empty(t, ProtoToListOpenIssuesRequest(nil))
	req := &pb.ListOpenIssuesRequest{Queue: "queue", PageSize: 10, PageToken: "token"}
	require.Equal(t, entity.ListOpenIssuesRequest{Queue: "queue", PageSize: 10, PageToken: "token"}, ProtoToListOpenIssuesRequest(req))
}

func TestListOpenIssuesResultToProto(t *testing.T) {
	result := entity.ListOpenIssuesResult{Issues: []entity.IssueSummary{
		{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "opening", RequestID: "100", BuildID: "source", ChangedAtMs: 1000, Version: 9},
		{TaskID: "task-wide", Status: entity.IssueStatusOpen, FirstSeenBuildID: "other", RequestID: "101", BuildID: "other", ChangedAtMs: 2000, Version: 1},
	}, NextPageToken: "next"}
	require.Equal(t, &pb.ListOpenIssuesResponse{Issues: []*pb.IssueSummary{
		{TaskId: "task", UnitId: "unit", Status: "open", FirstSeenBuildId: "opening", RequestId: "100", BuildId: "source", ChangedAtMs: 1000},
		{TaskId: "task-wide", Status: "open", FirstSeenBuildId: "other", RequestId: "101", BuildId: "other", ChangedAtMs: 2000},
	}, NextPageToken: "next"}, ListOpenIssuesResultToProto(result))
	empty := ListOpenIssuesResultToProto(entity.ListOpenIssuesResult{})
	require.Empty(t, empty.GetIssues())
	require.Empty(t, empty.GetNextPageToken())
}
