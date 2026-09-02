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
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

// ProtoToGetRequestHistoryByIDRequest maps a wire request to its domain value.
func ProtoToGetRequestHistoryByIDRequest(req *pb.GetRequestHistoryByIDRequest) entity.GetRequestHistoryByIDRequest {
	return entity.GetRequestHistoryByIDRequest{ID: req.GetRequestId(), Queue: req.GetQueue()}
}

// ProtoToGetRequestHistoryByURIRequest maps a wire request to its domain value.
func ProtoToGetRequestHistoryByURIRequest(req *pb.GetRequestHistoryByURIRequest) entity.GetRequestHistoryByURIRequest {
	return entity.GetRequestHistoryByURIRequest{URI: req.GetUri(), Queue: req.GetQueue()}
}

// HistoryEventsToProto maps retained request-log events to wire history events.
func HistoryEventsToProto(logs []entity.RequestLog) []*pb.HistoryEvent {
	events := make([]*pb.HistoryEvent, len(logs))
	for i, log := range logs {
		event := &pb.HistoryEvent{
			EventId:       log.ID,
			TimestampMs:   log.TimestampMs,
			OutcomeReason: string(log.OutcomeReason),
		}
		if log.State != entity.RequestStateUnknown {
			event.Occurrence = &pb.HistoryEvent_RequestState{RequestState: string(log.State)}
		} else {
			event.Occurrence = &pb.HistoryEvent_Event{Event: string(log.Event)}
		}
		events[i] = event
	}
	return events
}

// RequestHistoriesToProto maps grouped retained histories to their wire representation.
func RequestHistoriesToProto(histories []entity.RequestHistory) []*pb.RequestHistory {
	result := make([]*pb.RequestHistory, len(histories))
	for i, history := range histories {
		result[i] = &pb.RequestHistory{
			RequestId: history.RequestID,
			Events:    HistoryEventsToProto(history.Events),
		}
	}
	return result
}
