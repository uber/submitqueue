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

func TestIssueHistoryMapping(t *testing.T) {
	for _, req := range []*pb.ListIssueHistoryRequest{nil, {Queue: "q", TaskId: "t", UnitId: "u", PageSize: 2, PageToken: "p"}} {
		got := ProtoToListIssueHistoryRequest(req)
		require.Equal(t, req.GetQueue(), got.Queue)
		require.Equal(t, req.GetTaskId(), got.TaskID)
		require.Equal(t, req.GetUnitId(), got.UnitID)
		require.Equal(t, req.GetPageSize(), got.PageSize)
		require.Equal(t, req.GetPageToken(), got.PageToken)
	}
	log := entity.IssueLog{Event: entity.IssueEventReported, BuildID: "b", TimestampMs: 9007199254740993, FirstSeenBuildID: "a"}
	response := ListIssueHistoryResultToProto(entity.ListIssueHistoryResult{Events: []entity.IssueHistoryEntry{{Log: log, RequestID: "12"}, {Log: log}}, NextPageToken: "next"})
	require.Equal(t, "next", response.NextPageToken)
	require.Len(t, response.Events, 2)
	require.Equal(t, "reported", response.Events[0].Event)
	require.Equal(t, log.TimestampMs, response.Events[0].TimestampMs)
	require.Equal(t, "a", response.Events[0].FirstSeenBuildId)
	require.Equal(t, "b", response.Events[0].BuildId)
	require.Equal(t, "12", response.Events[0].RequestId)
	require.Empty(t, response.Events[1].RequestId)
}
