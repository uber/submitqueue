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

// ProtoToGetRequestSummaryByIDRequest maps the wire request to the entity request the controller operates on.
func ProtoToGetRequestSummaryByIDRequest(req *pb.GetRequestSummaryByIDRequest) entity.GetRequestSummaryByIDRequest {
	return entity.GetRequestSummaryByIDRequest{
		ID: req.GetSqid(),
	}
}

// ProtoToGetRequestSummaryByChangeURIRequest maps the wire request to the entity request the controller operates on.
func ProtoToGetRequestSummaryByChangeURIRequest(req *pb.GetRequestSummaryByChangeURIRequest) entity.GetRequestSummaryByChangeURIRequest {
	return entity.GetRequestSummaryByChangeURIRequest{
		ChangeURI: req.GetChangeUri(),
	}
}

// RequestSummaryToProto maps a domain request summary to the wire request summary.
func RequestSummaryToProto(summary entity.RequestSummary) *pb.RequestSummary {
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

// RequestSummariesToProto maps domain request summaries to wire request summaries.
func RequestSummariesToProto(summaries []entity.RequestSummary) []*pb.RequestSummary {
	requests := make([]*pb.RequestSummary, 0, len(summaries))
	for _, summary := range summaries {
		requests = append(requests, RequestSummaryToProto(summary))
	}
	return requests
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return map[string]string{}
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
