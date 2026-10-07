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

// ProtoToListRequest preserves time-bound presence, leaving defaults and validation to the controller.
func ProtoToListRequest(req *pb.ListRequest) entity.ListRequest {
	return entity.ListRequest{
		Queue:                  req.GetQueue(),
		AcceptedAtOrAfterMs:    req.GetAcceptedAtOrAfterMs(),
		HasAcceptedAtOrAfterMs: req.GetAcceptedLowerBound() != nil,
		AcceptedBeforeMs:       req.GetAcceptedBeforeMs(),
		HasAcceptedBeforeMs:    req.GetAcceptedUpperBound() != nil,
		PageSize:               req.GetPageSize(),
		PageToken:              req.GetPageToken(),
	}
}

// ListResultToProto preserves request order and the continuation token.
func ListResultToProto(result entity.ListResult) *pb.ListResponse {
	requests := make([]*pb.RequestSummary, len(result.Requests))
	for i, summary := range result.Requests {
		requests[i] = RequestSummaryToProto(summary)
	}
	return &pb.ListResponse{Requests: requests, NextPageToken: result.NextPageToken}
}

// RequestSummaryToProto exposes public summary fields, omitting internal storage versions.
func RequestSummaryToProto(summary entity.RequestSummary) *pb.RequestSummary {
	return &pb.RequestSummary{
		RequestId:        summary.RequestID,
		Queue:            summary.Queue,
		ChangeUri:        summary.URI,
		BaseUri:          summary.BaseURI,
		RequestState:     string(summary.State),
		StateUpdatedAtMs: summary.StateTimestampMs,
		AcceptedAtMs:     summary.AcceptedAtMs,
		OutcomeReason:    string(summary.OutcomeReason),
	}
}
