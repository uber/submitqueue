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
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

// ProtoToListIssueHistoryRequest leaves defaults and validation to the controller.
func ProtoToListIssueHistoryRequest(req *pb.ListIssueHistoryRequest) entity.ListIssueHistoryRequest {
	return entity.ListIssueHistoryRequest{Queue: req.GetQueue(), TaskID: req.GetTaskId(), UnitID: req.GetUnitId(), PageSize: req.GetPageSize(), PageToken: req.GetPageToken()}
}

// ListIssueHistoryResultToProto preserves original occurrence values and missing source requests.
func ListIssueHistoryResultToProto(result entity.ListIssueHistoryResult) *pb.ListIssueHistoryResponse {
	response := &pb.ListIssueHistoryResponse{Events: make([]*pb.IssueHistoryEvent, 0, len(result.Events)), NextPageToken: result.NextPageToken}
	for _, entry := range result.Events {
		response.Events = append(response.Events, &pb.IssueHistoryEvent{Event: string(entry.Log.Event), BuildId: entry.Log.BuildID,
			TimestampMs: entry.Log.TimestampMs, FirstSeenBuildId: entry.Log.FirstSeenBuildID, RequestId: entry.RequestID})
	}
	return response
}
