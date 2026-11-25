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

// ProtoToListIssueHistoryRequest preserves time-bound presence, leaving defaults and validation to the controller.
func ProtoToListIssueHistoryRequest(req *pb.ListIssueHistoryRequest) entity.ListIssueHistoryRequest {
	return entity.ListIssueHistoryRequest{
		Queue:                  req.GetQueue(),
		TaskID:                 req.GetTaskId(),
		UnitID:                 req.GetUnitId(),
		PageSize:               req.GetPageSize(),
		PageToken:              req.GetPageToken(),
		AcceptedAtOrAfterMs:    req.GetAcceptedAtOrAfterMs(),
		HasAcceptedAtOrAfterMs: req.GetAcceptedLowerBound() != nil,
		AcceptedBeforeMs:       req.GetAcceptedBeforeMs(),
		HasAcceptedBeforeMs:    req.GetAcceptedUpperBound() != nil,
	}
}

// ListIssueHistoryResultToProto preserves event order, original values, and the resolved request window.
func ListIssueHistoryResultToProto(result entity.ListIssueHistoryResult) *pb.ListIssueHistoryResponse {
	events := make([]*pb.IssueHistoryEvent, len(result.Events))
	for i, entry := range result.Events {
		events[i] = &pb.IssueHistoryEvent{
			TaskId:           entry.Log.TaskID,
			UnitId:           entry.Log.UnitID,
			Event:            string(entry.Log.Event),
			BuildId:          entry.Log.BuildID,
			TimestampMs:      entry.Log.TimestampMs,
			FirstSeenBuildId: entry.Log.FirstSeenBuildID,
			RequestId:        entry.RequestID,
		}
	}

	return &pb.ListIssueHistoryResponse{
		Events:              events,
		NextPageToken:       result.NextPageToken,
		AcceptedAtOrAfterMs: result.AcceptedAtOrAfterMs,
		AcceptedBeforeMs:    result.AcceptedBeforeMs,
	}
}
