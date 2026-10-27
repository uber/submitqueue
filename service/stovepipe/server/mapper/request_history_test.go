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
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

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
			req:  &pb.GetRequestHistoryByIDRequest{Queue: "monorepo/main", RequestId: "1"},
			want: entity.GetRequestHistoryByIDRequest{Queue: "monorepo/main", ID: "1"},
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
				{RequestID: "1", Events: logs, RequestSummary: entity.RequestSummary{RequestID: "1"}},
				{RequestID: "2", Events: []entity.RequestLog{}, RequestSummary: entity.RequestSummary{RequestID: "2"}},
			},
			want: []*pb.RequestHistory{
				{
					RequestId:      "1",
					RequestSummary: &pb.RequestSummary{RequestId: "1"},
					Events: []*pb.HistoryEvent{{
						EventId:     "state/1",
						TimestampMs: 10,
						Occurrence:  &pb.HistoryEvent_RequestState{RequestState: "accepted"},
					}},
				},
				{RequestId: "2", Events: []*pb.HistoryEvent{}, RequestSummary: &pb.RequestSummary{RequestId: "2"}},
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

func TestHistoryMetadataToProto(t *testing.T) {
	tests := []struct {
		name     string
		state    entity.RequestState
		event    entity.RequestEvent
		metadata map[string]string
		wantData string
	}{
		{
			name:     "state retains baseline and terminal build",
			state:    entity.RequestStateSucceeded,
			metadata: map[string]string{"base_uri": "git://repo/base", "build_id": "build/1", "private": "hidden"},
			wantData: `{"requestState":{"baseUri":"git://repo/base","buildId":"build/1"}}`,
		},
		{
			name:     "full build has an explicitly empty baseline",
			state:    entity.RequestStateProcessing,
			metadata: map[string]string{"base_uri": ""},
			wantData: `{"requestState":{"baseUri":""}}`,
		},
		{
			name:     "baseline without terminal build",
			state:    entity.RequestStateProcessing,
			metadata: map[string]string{"base_uri": "git://repo/base"},
			wantData: `{"requestState":{"baseUri":"git://repo/base"}}`,
		},
		{
			name:     "terminal build with unknown baseline",
			state:    entity.RequestStateFailed,
			metadata: map[string]string{"build_id": "build/1"},
			wantData: `{"requestState":{"buildId":"build/1"}}`,
		},
		{
			name:     "triggered build",
			event:    entity.RequestEventBuildTriggered,
			metadata: map[string]string{"build_id": "build/1", "base_uri": "not state context"},
			wantData: `{"buildTriggered":{"buildId":"build/1"}}`,
		},
		{
			name:     "finished build",
			event:    entity.RequestEventBuildFinished,
			metadata: map[string]string{"build_id": "build/1"},
			wantData: `{"buildFinished":{"buildId":"build/1"}}`,
		},
		{
			name:     "abandoned build analysis",
			event:    entity.RequestEventBuildAnalysisAbandoned,
			metadata: map[string]string{"build_id": "build/1"},
			wantData: `{"buildAnalysisAbandoned":{"buildId":"build/1"}}`,
		},
		{
			name:     "green validation degree is known zero",
			event:    entity.RequestEventValidationFactRecorded,
			metadata: map[string]string{"fact_degree": "0"},
			wantData: `{"validationFactRecorded":{}}`,
		},
		{
			name:     "partial validation degree",
			event:    entity.RequestEventValidationFactRecorded,
			metadata: map[string]string{"fact_degree": "0.5"},
			wantData: `{"validationFactRecorded":{"factDegree":0.5}}`,
		},
		{
			name:     "broken validation degree",
			event:    entity.RequestEventValidationFactRecorded,
			metadata: map[string]string{"fact_degree": "1"},
			wantData: `{"validationFactRecorded":{"factDegree":1}}`,
		},
		{
			name:     "empty project fact batch is known zero",
			event:    entity.RequestEventProjectFactsRecorded,
			metadata: map[string]string{"project_fact_count": "0"},
			wantData: `{"projectFactsRecorded":{}}`,
		},
		{
			name:     "project fact count",
			event:    entity.RequestEventProjectFactsRecorded,
			metadata: map[string]string{"project_fact_count": "42"},
			wantData: `{"projectFactsRecorded":{"projectFactCount":"42"}}`,
		},
		{
			name:     "large project fact count preserves precision",
			event:    entity.RequestEventProjectFactsRecorded,
			metadata: map[string]string{"project_fact_count": "9007199254740993"},
			wantData: `{"projectFactsRecorded":{"projectFactCount":"9007199254740993"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadataBefore := maps.Clone(tt.metadata)
			history := entity.RequestHistory{RequestID: "1", Events: []entity.RequestLog{{
				ID: "occurrence", State: tt.state, Event: tt.event, Metadata: tt.metadata,
			}}}
			byID := RequestHistoryByIDToProto(history).Events[0]
			byURI := RequestHistoriesToProto([]entity.RequestHistory{history})[0].Events[0]
			require.NotNil(t, byID.Data)
			assert.True(t, proto.Equal(byID, byURI))
			assert.Equal(t, metadataBefore, tt.metadata)

			jsonData, err := protojson.Marshal(byID.Data)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wantData, string(jsonData))

			encoded, err := proto.Marshal(byID)
			require.NoError(t, err)
			var decoded pb.HistoryEvent
			require.NoError(t, proto.Unmarshal(encoded, &decoded))
			assert.True(t, proto.Equal(byID, &decoded))

			jsonEvent, err := protojson.Marshal(byID)
			require.NoError(t, err)
			var decodedJSON pb.HistoryEvent
			require.NoError(t, protojson.Unmarshal(jsonEvent, &decodedJSON))
			assert.True(t, proto.Equal(byID, &decodedJSON))
		})
	}
}

func TestHistoryMetadataUnavailable(t *testing.T) {
	type testCase struct {
		name string
		log  entity.RequestLog
	}
	tests := []testCase{
		{name: "state without metadata", log: entity.RequestLog{State: entity.RequestStateAccepted}},
		{name: "state without recognized metadata", log: entity.RequestLog{State: entity.RequestStateFailed, Metadata: map[string]string{"private": "hidden", "build_id": ""}}},
		{name: "triggered build without metadata", log: entity.RequestLog{Event: entity.RequestEventBuildTriggered}},
		{name: "finished build with empty ID", log: entity.RequestLog{Event: entity.RequestEventBuildFinished, Metadata: map[string]string{"build_id": ""}}},
		{name: "abandoned analysis without metadata", log: entity.RequestLog{Event: entity.RequestEventBuildAnalysisAbandoned}},
		{name: "validation without degree", log: entity.RequestLog{Event: entity.RequestEventValidationFactRecorded}},
		{name: "project facts without count", log: entity.RequestLog{Event: entity.RequestEventProjectFactsRecorded}},
		{name: "metadata for another event", log: entity.RequestLog{Event: entity.RequestEventRecordAbandoned, Metadata: map[string]string{"build_id": "build/1", "fact_degree": "1", "project_fact_count": "2"}}},
		{name: "unknown event metadata", log: entity.RequestLog{Event: entity.RequestEvent("future_event"), Metadata: map[string]string{"build_id": "build/1"}}},
	}
	for _, degree := range []string{"invalid", "NaN", "+Inf", "-Inf", "-0.1", "1.1"} {
		tests = append(tests, testCase{name: "invalid degree " + degree, log: entity.RequestLog{
			Event: entity.RequestEventValidationFactRecorded, Metadata: map[string]string{"fact_degree": degree},
		}})
	}
	for _, count := range []string{"invalid", "-1", "1.5", "18446744073709551616"} {
		tests = append(tests, testCase{name: "invalid count " + count, log: entity.RequestLog{
			Event: entity.RequestEventProjectFactsRecorded, Metadata: map[string]string{"project_fact_count": count},
		}})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := HistoryEventsToProto([]entity.RequestLog{tt.log})[0]
			assert.Nil(t, event.Data)
			assert.Equal(t, string(tt.log.State), event.GetRequestState())
			assert.Equal(t, string(tt.log.Event), event.GetEvent())
		})
	}
}

func TestRequestHistorySummaryMapping(t *testing.T) {
	history := entity.RequestHistory{
		RequestID: "7", Events: []entity.RequestLog{{ID: "accepted", State: entity.RequestStateAccepted}},
		RequestSummary: entity.RequestSummary{
			RequestID: "7", Queue: "queue", URI: "uri", BaseURI: "base", State: "future-state",
			StateTimestampMs: 200, AcceptedAtMs: 100, OutcomeReason: "future-reason",
		},
	}
	byID := RequestHistoryByIDToProto(history)
	byURI := RequestHistoriesToProto([]entity.RequestHistory{history})[0]
	assert.Equal(t, byID.Events, byURI.Events)
	want := &pb.RequestSummary{
		RequestId: "7", Queue: "queue", ChangeUri: "uri", BaseUri: "base", RequestState: "future-state",
		StateUpdatedAtMs: 200, AcceptedAtMs: 100, OutcomeReason: "future-reason",
	}
	assert.Equal(t, want, byID.RequestSummary)
	assert.Equal(t, want, byURI.RequestSummary)
}
