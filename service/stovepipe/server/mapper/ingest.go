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

// Package mapper translates stovepipe wire (proto) types to and from the domain
// entities the controllers operate on. Each RPC gets its own file; translation
// lives here so controllers stay proto-free.
package mapper

import (
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

// ProtoToIngestRequest maps the wire IngestRequest to the entity.IngestRequest
// the controller operates on.
func ProtoToIngestRequest(req *pb.IngestRequest) entity.IngestRequest {
	return entity.IngestRequest{
		Queue: req.GetQueue(),
	}
}

// IngestResultToProto maps the domain result to the wire IngestResponse.
func IngestResultToProto(result entity.IngestResult) *pb.IngestResponse {
	return &pb.IngestResponse{
		Id: result.ID,
	}
}
