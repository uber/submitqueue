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

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestProjectStatusRequestAndResponseMapping(t *testing.T) {
	request := &pb.GetProjectStatusByURIRequest{
		Queue: "queue", ChangeUri: "uri", Projects: []string{"project-a", "project-b"}, PageSize: 25, PageToken: "token",
	}
	assert.Equal(t, entity.GetProjectStatusRequest{
		Queue: "queue", ChangeURI: "uri", Projects: []string{"project-a", "project-b"}, PageSize: 25, PageToken: "token",
	}, ProtoToGetProjectStatusByURIRequest(request))

	degree := entity.DegreeGreen
	response := GetProjectStatusByURIResultToProto(entity.GetProjectStatusResult{
		RequestSummary:              entity.RequestSummary{RequestID: "1", Queue: "queue", URI: "uri", BaseURI: "base", State: entity.RequestStateSucceeded},
		RepositoryValidationFact:    entity.ValidationFact{Degree: degree},
		HasRepositoryValidationFact: true,
		ProjectValidationFacts:      []entity.ValidationFact{{Project: "project-a", ProjectType: "service", Degree: entity.DegreeBroken}},
		NextPageToken:               "next-token",
	})
	assert.Equal(t, "1", response.GetRequestId())
	assert.Equal(t, "succeeded", response.GetRequestState())
	assert.Equal(t, degree, response.GetRepositoryBreakageDegree())
	assert.IsType(t, &pb.GetProjectStatusByURIResponse_RepositoryBreakageDegree{}, response.GetRepositoryResult())
	assert.False(t, response.GetProjectResultsComplete())
	if !assert.Len(t, response.GetProjects(), 1) {
		return
	}
	assert.Equal(t, "project-a", response.GetProjects()[0].GetProject())
	assert.Equal(t, "service", response.GetProjects()[0].GetProjectType())
	assert.Equal(t, entity.DegreeBroken, response.GetProjects()[0].GetBreakageDegree())
	assert.IsType(t, &pb.ProjectValidation_BreakageDegree{}, response.GetProjects()[0].GetResult())
	assert.Equal(t, "next-token", response.GetNextPageToken())
}

func TestProjectStatusMappingPreservesExplicitGreenResults(t *testing.T) {
	response := GetProjectStatusByURIResultToProto(entity.GetProjectStatusResult{
		HasRepositoryValidationFact: true,
		RepositoryValidationFact: entity.ValidationFact{
			Degree: entity.DegreeGreen,
		},
		ProjectValidationFacts: []entity.ValidationFact{{
			Project:     "project-a",
			ProjectType: "service",
			Degree:      entity.DegreeGreen,
		}},
	})

	assert.IsType(t, &pb.GetProjectStatusByURIResponse_RepositoryBreakageDegree{}, response.GetRepositoryResult())
	if assert.Len(t, response.GetProjects(), 1) {
		assert.IsType(t, &pb.ProjectValidation_BreakageDegree{}, response.GetProjects()[0].GetResult())
	}
}

func TestProjectStatusByIDRequestAndResponseMapping(t *testing.T) {
	assert.Equal(t, entity.GetProjectStatusRequest{
		Queue: "queue", RequestID: "1", Projects: []string{"project-a", "project-b"}, PageSize: 25, PageToken: "token",
	}, ProtoToGetProjectStatusByIDRequest(&pb.GetProjectStatusByIDRequest{
		Queue: "queue", RequestId: "1", Projects: []string{"project-a", "project-b"}, PageSize: 25, PageToken: "token",
	}))
	for _, hasFact := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent repository result", true: "explicit green repository result"}[hasFact], func(t *testing.T) {
			result := entity.GetProjectStatusResult{
				RequestSummary:              entity.RequestSummary{RequestID: "1", Queue: "queue", URI: "uri", BaseURI: "base", State: entity.RequestStateSucceeded},
				HasRepositoryValidationFact: hasFact,
				RepositoryValidationFact:    entity.ValidationFact{Degree: entity.DegreeGreen},
				ProjectResultsComplete:      true,
				ProjectValidationFacts:      []entity.ValidationFact{{Project: "project-a", ProjectType: "service", Degree: entity.DegreeGreen}, {Project: "project-b", Degree: entity.DegreeBroken}},
				NextPageToken:               "next-token",
				UpdatedAtMs:                 123,
			}
			response := GetProjectStatusByIDResultToProto(result)
			assert.Equal(t, "1", response.GetRequestId())
			assert.Equal(t, "queue", response.GetQueue())
			assert.Equal(t, "uri", response.GetChangeUri())
			assert.Equal(t, "base", response.GetBaseUri())
			assert.Equal(t, "succeeded", response.GetRequestState())
			assert.Equal(t, int64(123), response.GetUpdatedAtMs())
			assert.True(t, response.GetProjectResultsComplete())
			assert.Equal(t, "next-token", response.GetNextPageToken())
			if hasFact {
				assert.IsType(t, &pb.GetProjectStatusByIDResponse_RepositoryBreakageDegree{}, response.GetRepositoryResult())
				assert.Equal(t, entity.DegreeGreen, response.GetRepositoryBreakageDegree())
			} else {
				assert.Nil(t, response.GetRepositoryResult())
			}
			if assert.Len(t, response.Projects, 2) {
				assert.Equal(t, "project-a", response.Projects[0].GetProject())
				assert.Equal(t, "service", response.Projects[0].GetProjectType())
				assert.Empty(t, response.Projects[1].GetProjectType())
				assert.IsType(t, &pb.ProjectValidation_BreakageDegree{}, response.Projects[0].GetResult())
				assert.Equal(t, entity.DegreeGreen, response.Projects[0].GetBreakageDegree())
				assert.Equal(t, "project-b", response.Projects[1].GetProject())
				assert.Equal(t, entity.DegreeBroken, response.Projects[1].GetBreakageDegree())
			}
		})
	}
}
