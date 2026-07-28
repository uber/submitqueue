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
	"github.com/uber/submitqueue/submitqueue/entity"
)

// ProtoToListRequest maps the wire request to the entity request the controller operates on.
func ProtoToListRequest(req *pb.ListRequest) entity.ListRequest {
	return entity.ListRequest{
		Queue:               req.GetQueue(),
		ReceivedAtOrAfterMs: req.GetReceivedAtOrAfterMs(),
		ReceivedBeforeMs:    req.GetReceivedBeforeMs(),
		PageSize:            req.GetPageSize(),
		PageToken:           req.GetPageToken(),
	}
}

// ListResultToProto maps the domain result to the wire response.
func ListResultToProto(result entity.ListResult) *pb.ListResponse {
	requests := make([]*pb.RequestSummary, 0, len(result.Requests))
	for _, summary := range result.Requests {
		requests = append(requests, RequestQueueSummaryToProto(summary))
	}
	return &pb.ListResponse{
		Requests:      requests,
		NextPageToken: result.NextPageToken,
	}
}

// RequestQueueSummaryToProto maps a queue projection to the wire request summary.
func RequestQueueSummaryToProto(summary entity.RequestQueueSummary) *pb.RequestSummary {
	return &pb.RequestSummary{
		Sqid:         summary.RequestID,
		Queue:        summary.Queue,
		ChangeUris:   append([]string{}, summary.ChangeURIs...),
		ReceivedAtMs: summary.ReceivedAtMs,
		Status:       string(summary.Status),
		LastError:    summary.LastError,
		Metadata:     cloneStringMap(summary.Metadata),
	}
}
