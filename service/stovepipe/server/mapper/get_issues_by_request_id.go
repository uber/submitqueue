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

// ProtoToGetIssuesByRequestIDRequest leaves defaults and validation to the controller.
func ProtoToGetIssuesByRequestIDRequest(req *pb.GetIssuesByRequestIDRequest) entity.GetIssuesByRequestIDRequest {
	return entity.GetIssuesByRequestIDRequest{Queue: req.GetQueue(), RequestID: req.GetRequestId(), PageSize: req.GetPageSize(), PageToken: req.GetPageToken()}
}

// GetIssuesByRequestIDResultToProto preserves request classifications and completion status.
func GetIssuesByRequestIDResultToProto(result entity.GetIssuesByRequestIDResult) *pb.GetIssuesByRequestIDResponse {
	issues := make([]*pb.RequestIssueSummary, 0, len(result.Issues))
	for _, issue := range result.Issues {
		issues = append(issues, &pb.RequestIssueSummary{TaskId: issue.TaskID, UnitId: issue.UnitID, Kind: string(issue.Kind), BuildId: issue.BuildID, FirstSeenBuildId: issue.FirstSeenBuildID})
	}
	return &pb.GetIssuesByRequestIDResponse{Issues: issues, Status: string(result.Status), NextPageToken: result.NextPageToken}
}
