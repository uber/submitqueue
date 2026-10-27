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
	"math"
	"strconv"

	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
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

// RequestHistoryByIDToProto maps retained events and the current summary to their wire representation.
func RequestHistoryByIDToProto(history entity.RequestHistory) *pb.GetRequestHistoryByIDResponse {
	return &pb.GetRequestHistoryByIDResponse{
		Events:         HistoryEventsToProto(history.Events),
		RequestSummary: RequestSummaryToProto(history.RequestSummary),
	}
}

// HistoryEventsToProto maps retained request-log events to wire history events.
func HistoryEventsToProto(logs []entity.RequestLog) []*pb.HistoryEvent {
	events := make([]*pb.HistoryEvent, len(logs))
	for i, log := range logs {
		event := &pb.HistoryEvent{
			EventId:       log.ID,
			TimestampMs:   log.TimestampMs,
			OutcomeReason: string(log.OutcomeReason),
			Data:          historyEventDataToProto(log),
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
			RequestId:      history.RequestID,
			Events:         HistoryEventsToProto(history.Events),
			RequestSummary: RequestSummaryToProto(history.RequestSummary),
		}
	}
	return result
}

func historyEventDataToProto(log entity.RequestLog) *pb.HistoryEventData {
	if log.State != entity.RequestStateUnknown {
		buildID := log.Metadata[requestlog.MetadataKeyBuildID]
		if buildID == "" {
			return nil
		}
		return &pb.HistoryEventData{Payload: &pb.HistoryEventData_RequestState{
			RequestState: &pb.HistoryRequestStateData{BuildId: buildID},
		}}
	}

	switch log.Event {
	case entity.RequestEventBuildTriggered, entity.RequestEventBuildFinished, entity.RequestEventBuildAnalysisAbandoned:
		buildID := log.Metadata[requestlog.MetadataKeyBuildID]
		if buildID == "" {
			return nil
		}
		build := &pb.HistoryBuildData{BuildId: buildID}
		data := &pb.HistoryEventData{}
		switch log.Event {
		case entity.RequestEventBuildTriggered:
			data.Payload = &pb.HistoryEventData_BuildTriggered{BuildTriggered: build}
		case entity.RequestEventBuildFinished:
			data.Payload = &pb.HistoryEventData_BuildFinished{BuildFinished: build}
		case entity.RequestEventBuildAnalysisAbandoned:
			data.Payload = &pb.HistoryEventData_BuildAnalysisAbandoned{BuildAnalysisAbandoned: build}
		}
		return data
	case entity.RequestEventValidationFactRecorded:
		degree, err := strconv.ParseFloat(log.Metadata[requestlog.MetadataKeyFactDegree], 64)
		if err != nil || math.IsNaN(degree) || degree < entity.DegreeGreen || degree > entity.DegreeBroken {
			return nil
		}
		return &pb.HistoryEventData{Payload: &pb.HistoryEventData_ValidationFactRecorded{
			ValidationFactRecorded: &pb.HistoryValidationFactData{FactDegree: degree},
		}}
	case entity.RequestEventProjectFactsRecorded:
		count, err := strconv.ParseUint(log.Metadata[requestlog.MetadataKeyProjectFactCount], 10, 64)
		if err != nil {
			return nil
		}
		return &pb.HistoryEventData{Payload: &pb.HistoryEventData_ProjectFactsRecorded{
			ProjectFactsRecorded: &pb.HistoryProjectFactsData{ProjectFactCount: count},
		}}
	default:
		return nil
	}
}
