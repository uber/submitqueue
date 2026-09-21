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

// ProtoToGetProjectStatusByURIRequest maps a wire selector to its domain form.
func ProtoToGetProjectStatusByURIRequest(req *pb.GetProjectStatusByURIRequest) entity.GetProjectStatusByURIRequest {
	result := entity.GetProjectStatusByURIRequest{
		Queue:     req.GetQueue(),
		ChangeURI: req.GetChangeUri(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	}
	result.Projects = req.GetProjects()
	return result
}

// GetProjectStatusByURIResultToProto maps a domain status projection to its wire response.
func GetProjectStatusByURIResultToProto(result entity.GetProjectStatusByURIResult) *pb.GetProjectStatusByURIResponse {
	response := &pb.GetProjectStatusByURIResponse{
		RequestId:              result.RequestSummary.RequestID,
		Queue:                  result.RequestSummary.Queue,
		ChangeUri:              result.RequestSummary.URI,
		BaseUri:                result.RequestSummary.BaseURI,
		RequestState:           string(result.RequestSummary.State),
		UpdatedAtMs:            result.UpdatedAtMs,
		ProjectResultsComplete: result.ProjectResultsComplete,
		NextPageToken:          result.NextPageToken,
	}
	if result.HasRepositoryValidationFact {
		response.RepositoryBreakageDegree = &result.RepositoryValidationFact.Degree
	}
	response.Projects = make([]*pb.ProjectValidation, 0, len(result.ProjectValidationFacts))
	for _, fact := range result.ProjectValidationFacts {
		degree := fact.Degree
		response.Projects = append(response.Projects, &pb.ProjectValidation{
			Project:        fact.Project,
			BreakageDegree: &degree,
		})
	}
	return response
}
