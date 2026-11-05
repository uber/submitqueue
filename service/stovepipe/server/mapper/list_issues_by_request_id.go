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

// ProtoToListIssuesByRequestIDRequest leaves validation and defaults to the controller.
func ProtoToListIssuesByRequestIDRequest(req *pb.ListIssuesByRequestIDRequest) entity.ListIssuesByRequestIDRequest {
	return entity.ListIssuesByRequestIDRequest{Queue: req.GetQueue(), RequestID: req.GetRequestId(), PageSize: req.GetPageSize(), PageToken: req.GetPageToken()}
}

// ListIssuesByRequestIDResultToProto preserves observations, provenance, and analysis outcomes.
func ListIssuesByRequestIDResultToProto(result entity.ListIssuesByRequestIDResult) *pb.ListIssuesByRequestIDResponse {
	issues := make([]*pb.RequestIssue, len(result.Issues))
	for i, issue := range result.Issues {
		issues[i] = &pb.RequestIssue{BuildId: issue.BuildID, TaskId: issue.TaskID, UnitId: issue.UnitID, FixId: issue.FixID}
	}
	builds := make([]*pb.IssueBuild, len(result.Builds))
	for i, build := range result.Builds {
		builds[i] = &pb.IssueBuild{BuildId: build.BuildID, AnalysisStatus: string(build.AnalysisStatus)}
	}
	return &pb.ListIssuesByRequestIDResponse{Queue: result.Queue, RequestId: result.RequestID, Issues: issues, Builds: builds, WinningBuildId: result.WinningBuildID, NextPageToken: result.NextPageToken}
}
