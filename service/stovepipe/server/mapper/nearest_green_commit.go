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

// ProtoToNearestGreenCommitRequest maps the wire selectors to the domain.
func ProtoToNearestGreenCommitRequest(req *pb.NearestGreenCommitThatIncludesRequest) entity.NearestGreenCommitRequest {
	return entity.NearestGreenCommitRequest{Queue: req.GetQueue(), DesiredCommitURI: req.GetDesiredCommitUri(), Project: req.GetProject()}
}

// NearestGreenCommitResultToProto maps the optional match to the wire response.
func NearestGreenCommitResultToProto(result entity.NearestGreenCommitResult) *pb.NearestGreenCommitThatIncludesResponse {
	return &pb.NearestGreenCommitThatIncludesResponse{ClosestContainingCommitUri: result.CommitURI, RequestId: result.RequestID}
}
