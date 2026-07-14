// Copyright (c) 2025 Uber Technologies, Inc.
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
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// ProtoToStatusRequest maps the wire StatusRequest to the entity.StatusRequest
// the controller operates on.
func ProtoToStatusRequest(req *pb.StatusRequest) entity.StatusRequest {
	return entity.StatusRequest{
		ID: req.GetSqid(),
	}
}

// CurrentStateToProto maps the domain read model to the wire StatusResponse.
func CurrentStateToProto(state request.CurrentState) *pb.StatusResponse {
	return &pb.StatusResponse{
		Status:    string(state.Status),
		LastError: state.LastError,
		Metadata:  state.Metadata,
	}
}
