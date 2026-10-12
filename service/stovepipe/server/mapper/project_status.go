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

// ProtoToGetProjectStatusByIDRequest maps a wire selector to its domain form.
func ProtoToGetProjectStatusByIDRequest(req *pb.GetProjectStatusByIDRequest) entity.GetProjectStatusRequest {
	result := entity.GetProjectStatusRequest{
		Queue:     req.GetQueue(),
		RequestID: req.GetRequestId(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	}
	result.Projects = req.GetProjects()
	return result
}

// ProtoToGetProjectStatusByURIRequest maps a wire selector to its domain form.
func ProtoToGetProjectStatusByURIRequest(req *pb.GetProjectStatusByURIRequest) entity.GetProjectStatusRequest {
	result := entity.GetProjectStatusRequest{
		Queue:     req.GetQueue(),
		ChangeURI: req.GetChangeUri(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	}
	result.Projects = req.GetProjects()
	return result
}

// GetProjectStatusByIDResultToProto maps a domain status projection to its wire response.
func GetProjectStatusByIDResultToProto(result entity.GetProjectStatusResult) *pb.GetProjectStatusByIDResponse {
	response := &pb.GetProjectStatusByIDResponse{
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
		response.RepositoryResult = &pb.GetProjectStatusByIDResponse_RepositoryBreakageDegree{
			RepositoryBreakageDegree: result.RepositoryValidationFact.Degree,
		}
	}
	response.Projects = validationFactsToProto(result.ProjectValidationFacts)
	return response
}

// GetProjectStatusByURIResultToProto maps a domain status projection to its wire response.
func GetProjectStatusByURIResultToProto(result entity.GetProjectStatusResult) *pb.GetProjectStatusByURIResponse {
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
		response.RepositoryResult = &pb.GetProjectStatusByURIResponse_RepositoryBreakageDegree{
			RepositoryBreakageDegree: result.RepositoryValidationFact.Degree,
		}
	}
	response.Projects = validationFactsToProto(result.ProjectValidationFacts)
	return response
}

func validationFactsToProto(facts []entity.ValidationFact) []*pb.ProjectValidation {
	projects := make([]*pb.ProjectValidation, 0, len(facts))
	for _, fact := range facts {
		degree := fact.Degree
		projects = append(projects, &pb.ProjectValidation{
			Project: fact.Project,
			Result: &pb.ProjectValidation_BreakageDegree{
				BreakageDegree: degree,
			},
		})
	}
	return projects
}
