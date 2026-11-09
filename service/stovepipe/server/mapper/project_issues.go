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

func ProtoToListRequestProjectsByIssueRequest(req *pb.ListRequestProjectsByIssueRequest) entity.ListRequestProjectsByIssueRequest {
	return entity.ListRequestProjectsByIssueRequest{
		Queue: req.GetQueue(), RequestID: req.GetRequestId(), TaskID: req.GetTaskId(), UnitID: req.GetUnitId(),
		PageSize: req.GetPageSize(), PageToken: req.GetPageToken(),
	}
}

func ProtoToListRequestIssuesByProjectRequest(req *pb.ListRequestIssuesByProjectRequest) entity.ListRequestIssuesByProjectRequest {
	return entity.ListRequestIssuesByProjectRequest{
		Queue: req.GetQueue(), RequestID: req.GetRequestId(), Project: req.GetProject(),
		PageSize: req.GetPageSize(), PageToken: req.GetPageToken(),
	}
}

func ListRequestProjectIssuesResultToProto(result entity.ListRequestProjectIssuesResult) *pb.ListRequestProjectIssuesResponse {
	attributions := make([]*pb.ProjectIssueAttribution, len(result.Attributions))
	for i, entry := range result.Attributions {
		attributions[i] = &pb.ProjectIssueAttribution{TaskId: entry.TaskID, UnitId: entry.UnitID, Project: entry.Project}
	}
	return &pb.ListRequestProjectIssuesResponse{
		Attributions: attributions, AttributionComplete: result.AttributionComplete, NextPageToken: result.NextPageToken,
	}
}
