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
	"fmt"

	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

// ProtoToGetProjectStatusByURIRequest maps the wire selector to its domain form.
func ProtoToGetProjectStatusByURIRequest(req *pb.GetProjectStatusByURIRequest) entity.GetProjectStatusByURIRequest {
	result := entity.GetProjectStatusByURIRequest{
		Queue:     req.GetQueue(),
		ChangeURI: req.GetChangeUri(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	}
	if req.Project != nil {
		result.Project = req.GetProject()
		result.HasProject = true
	}
	return result
}

// GetProjectStatusByURIResultToProto maps a domain projection to the wire response.
func GetProjectStatusByURIResultToProto(result entity.GetProjectStatusByURIResult) (*pb.GetProjectStatusByURIResponse, error) {
	requestState, err := projectStatusRequestStateToProto(result.Request.State)
	if err != nil {
		return nil, err
	}
	response := &pb.GetProjectStatusByURIResponse{
		RequestId:    result.Request.ID,
		Queue:        result.Request.Queue,
		ChangeUri:    result.Request.URI,
		BaseUri:      result.Request.BaseURI,
		RequestState: requestState,
	}
	if result.HasRepositoryValidationFact {
		response.RepositoryBreakageDegree = &result.RepositoryValidationFact.Degree
	}
	return response, nil
}

func projectStatusRequestStateToProto(state entity.RequestState) (pb.RequestState, error) {
	switch state {
	case entity.RequestStateAccepted:
		return pb.RequestState_REQUEST_STATE_ACCEPTED, nil
	case entity.RequestStateProcessing:
		return pb.RequestState_REQUEST_STATE_PROCESSING, nil
	case entity.RequestStateSucceeded:
		return pb.RequestState_REQUEST_STATE_SUCCEEDED, nil
	case entity.RequestStateFailed:
		return pb.RequestState_REQUEST_STATE_FAILED, nil
	case entity.RequestStateCancelled:
		return pb.RequestState_REQUEST_STATE_CANCELLED, nil
	case entity.RequestStateSuperseded:
		return pb.RequestState_REQUEST_STATE_SUPERSEDED, nil
	default:
		return pb.RequestState_REQUEST_STATE_UNSPECIFIED, fmt.Errorf("request state %q cannot be represented by GetProjectStatusByURI", state)
	}
}
