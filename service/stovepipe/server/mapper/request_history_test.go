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
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestProtoToGetRequestHistoryByIDRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *pb.GetRequestHistoryByIDRequest
		want entity.GetRequestHistoryByIDRequest
	}{
		{
			name: "maps selector",
			req:  &pb.GetRequestHistoryByIDRequest{Queue: "monorepo/main", RequestId: "request/monorepo/main/1"},
			want: entity.GetRequestHistoryByIDRequest{Queue: "monorepo/main", ID: "request/monorepo/main/1"},
		},
		{
			name: "empty request yields zero value",
			req:  &pb.GetRequestHistoryByIDRequest{},
			want: entity.GetRequestHistoryByIDRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ProtoToGetRequestHistoryByIDRequest(tt.req))
		})
	}
}

func TestProtoToGetRequestHistoryByURIRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *pb.GetRequestHistoryByURIRequest
		want entity.GetRequestHistoryByURIRequest
	}{
		{
			name: "maps selector",
			req:  &pb.GetRequestHistoryByURIRequest{Queue: "monorepo/main", Uri: "git://remote/monorepo/main/abc"},
			want: entity.GetRequestHistoryByURIRequest{Queue: "monorepo/main", URI: "git://remote/monorepo/main/abc"},
		},
		{
			name: "empty request yields zero value",
			req:  &pb.GetRequestHistoryByURIRequest{},
			want: entity.GetRequestHistoryByURIRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ProtoToGetRequestHistoryByURIRequest(tt.req))
		})
	}
}

func TestHistoryEventsToProto(t *testing.T) {
	state := entity.RequestLog{
		ID:             "state/2",
		TimestampMs:    20,
		State:          entity.RequestStateFailed,
		RequestVersion: 2,
		OutcomeReason:  entity.RequestOutcomeReasonProcessingFailed,
		Metadata:       map[string]string{"debug": "not public"},
	}
	occurrence := entity.RequestLog{
		ID:          "event/build-finished",
		TimestampMs: 30,
		Event:       entity.RequestEventBuildFinished,
	}
	futureState := entity.RequestLog{ID: "state/future", TimestampMs: 40, State: entity.RequestState("future_state")}
	futureEvent := entity.RequestLog{ID: "event/future", TimestampMs: 50, Event: entity.RequestEvent("future_event")}

	tests := []struct {
		name string
		logs []entity.RequestLog
		want []*pb.HistoryEvent
	}{
		{
			name: "maps state and event vocabulary without internal fields",
			logs: []entity.RequestLog{state, occurrence, occurrence, futureState, futureEvent},
			want: []*pb.HistoryEvent{
				{
					EventId:       "state/2",
					TimestampMs:   20,
					Occurrence:    &pb.HistoryEvent_RequestState{RequestState: "failed"},
					OutcomeReason: "processing_failed",
				},
				{
					EventId:     "event/build-finished",
					TimestampMs: 30,
					Occurrence:  &pb.HistoryEvent_Event{Event: "build_finished"},
				},
				{
					EventId:     "event/build-finished",
					TimestampMs: 30,
					Occurrence:  &pb.HistoryEvent_Event{Event: "build_finished"},
				},
				{
					EventId:     "state/future",
					TimestampMs: 40,
					Occurrence:  &pb.HistoryEvent_RequestState{RequestState: "future_state"},
				},
				{
					EventId:     "event/future",
					TimestampMs: 50,
					Occurrence:  &pb.HistoryEvent_Event{Event: "future_event"},
				},
			},
		},
		{name: "nil input", logs: nil, want: []*pb.HistoryEvent{}},
		{name: "empty input", logs: []entity.RequestLog{}, want: []*pb.HistoryEvent{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HistoryEventsToProto(tt.logs))
		})
	}
}

func TestRequestHistoriesToProto(t *testing.T) {
	logs := []entity.RequestLog{{ID: "state/1", TimestampMs: 10, State: entity.RequestStateAccepted}}
	tests := []struct {
		name      string
		histories []entity.RequestHistory
		want      []*pb.RequestHistory
	}{
		{
			name: "maps groups in order",
			histories: []entity.RequestHistory{
				{RequestID: "request/monorepo/main/1", Events: logs},
				{RequestID: "request/monorepo/main/2", Events: []entity.RequestLog{}},
			},
			want: []*pb.RequestHistory{
				{
					RequestId: "request/monorepo/main/1",
					Events: []*pb.HistoryEvent{{
						EventId:     "state/1",
						TimestampMs: 10,
						Occurrence:  &pb.HistoryEvent_RequestState{RequestState: "accepted"},
					}},
				},
				{RequestId: "request/monorepo/main/2", Events: []*pb.HistoryEvent{}},
			},
		},
		{name: "nil input", histories: nil, want: []*pb.RequestHistory{}},
		{name: "empty input", histories: []entity.RequestHistory{}, want: []*pb.RequestHistory{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RequestHistoriesToProto(tt.histories))
		})
	}
}
