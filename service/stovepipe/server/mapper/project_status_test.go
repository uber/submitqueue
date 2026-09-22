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
	assert.Equal(t, entity.GetProjectStatusByURIRequest{
		Queue: "queue", ChangeURI: "uri", Projects: []string{"project-a", "project-b"}, PageSize: 25, PageToken: "token",
	}, ProtoToGetProjectStatusByURIRequest(request))

	degree := entity.DegreeGreen
	response := GetProjectStatusByURIResultToProto(entity.GetProjectStatusByURIResult{
		RequestSummary:              entity.RequestSummary{RequestID: "request/1", Queue: "queue", URI: "uri", BaseURI: "base", State: entity.RequestStateSucceeded},
		RepositoryValidationFact:    entity.ValidationFact{Degree: degree},
		HasRepositoryValidationFact: true,
		ProjectValidationFacts:      []entity.ValidationFact{{Project: "project-a", Degree: entity.DegreeBroken}},
		NextPageToken:               "next-token",
	})
	assert.Equal(t, "request/1", response.GetRequestId())
	assert.Equal(t, "succeeded", response.GetRequestState())
	assert.Equal(t, degree, response.GetRepositoryBreakageDegree())
	assert.IsType(t, &pb.GetProjectStatusByURIResponse_RepositoryBreakageDegree{}, response.GetRepositoryResult())
	assert.False(t, response.GetProjectResultsComplete())
	if !assert.Len(t, response.GetProjects(), 1) {
		return
	}
	assert.Equal(t, "project-a", response.GetProjects()[0].GetProject())
	assert.Equal(t, entity.DegreeBroken, response.GetProjects()[0].GetBreakageDegree())
	assert.IsType(t, &pb.ProjectValidation_BreakageDegree{}, response.GetProjects()[0].GetResult())
	assert.Equal(t, "next-token", response.GetNextPageToken())
}

func TestProjectStatusMappingPreservesExplicitGreenResults(t *testing.T) {
	response := GetProjectStatusByURIResultToProto(entity.GetProjectStatusByURIResult{
		HasRepositoryValidationFact: true,
		RepositoryValidationFact: entity.ValidationFact{
			Degree: entity.DegreeGreen,
		},
		ProjectValidationFacts: []entity.ValidationFact{{
			Project: "project-a",
			Degree:  entity.DegreeGreen,
		}},
	})

	assert.IsType(t, &pb.GetProjectStatusByURIResponse_RepositoryBreakageDegree{}, response.GetRepositoryResult())
	if assert.Len(t, response.GetProjects(), 1) {
		assert.IsType(t, &pb.ProjectValidation_BreakageDegree{}, response.GetProjects()[0].GetResult())
	}
}
