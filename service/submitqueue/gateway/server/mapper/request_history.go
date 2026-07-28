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

// ProtoToGetRequestHistoryByIDRequest maps the wire request to the entity request the controller operates on.
func ProtoToGetRequestHistoryByIDRequest(req *pb.GetRequestHistoryByIDRequest) entity.GetRequestHistoryByIDRequest {
	return entity.GetRequestHistoryByIDRequest{ID: req.GetSqid()}
}

// ProtoToGetRequestHistoryByChangeURIRequest maps the wire request to the entity request the controller operates on.
func ProtoToGetRequestHistoryByChangeURIRequest(req *pb.GetRequestHistoryByChangeURIRequest) entity.GetRequestHistoryByChangeURIRequest {
	return entity.GetRequestHistoryByChangeURIRequest{ChangeURI: req.GetChangeUri()}
}

// HistoryEventsToProto maps retained request-log events to wire history events.
func HistoryEventsToProto(logs []entity.RequestLog) []*pb.HistoryEvent {
	events := make([]*pb.HistoryEvent, len(logs))
	for i, log := range logs {
		events[i] = &pb.HistoryEvent{
			TimestampMs: log.TimestampMs,
			Status:      string(log.Status),
			LastError:   log.LastError,
			Metadata:    cloneStringMap(log.Metadata),
		}
	}
	return events
}

// RequestHistoriesToProto maps grouped retained histories to the wire representation.
func RequestHistoriesToProto(histories []entity.RequestHistory) []*pb.RequestHistory {
	result := make([]*pb.RequestHistory, len(histories))
	for i, history := range histories {
		result[i] = &pb.RequestHistory{
			Sqid:   history.RequestID,
			Events: HistoryEventsToProto(history.Events),
		}
	}
	return result
}
