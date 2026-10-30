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

// ProtoToListOpenIssuesRequest leaves defaults and validation to the controller.
func ProtoToListOpenIssuesRequest(req *pb.ListOpenIssuesRequest) entity.ListOpenIssuesRequest {
	return entity.ListOpenIssuesRequest{Queue: req.GetQueue(), PageSize: req.GetPageSize(), PageToken: req.GetPageToken()}
}

// ListOpenIssuesResultToProto preserves summary order and the continuation token.
func ListOpenIssuesResultToProto(result entity.ListOpenIssuesResult) *pb.ListOpenIssuesResponse {
	issues := make([]*pb.IssueSummary, len(result.Issues))
	for i, summary := range result.Issues {
		issues[i] = &pb.IssueSummary{
			TaskId:           summary.TaskID,
			UnitId:           summary.UnitID,
			Status:           string(summary.Status),
			FirstSeenBuildId: summary.FirstSeenBuildID,
			RequestId:        summary.RequestID,
			BuildId:          summary.BuildID,
			ChangedAtMs:      summary.ChangedAtMs,
		}
	}
	return &pb.ListOpenIssuesResponse{Issues: issues, NextPageToken: result.NextPageToken}
}
