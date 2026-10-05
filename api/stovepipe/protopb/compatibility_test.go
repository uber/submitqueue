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

package protopb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestRequestSummaryCompatibilityWithProjectStatus(t *testing.T) {
	sharedSummary := &RequestSummary{
		RequestId: "request/queue/42", Queue: "queue", ChangeUri: "uri", BaseUri: "base", RequestState: "failed",
	}
	sharedStatus := &GetProjectStatusByURIResponse{
		RequestId: "request/queue/42", Queue: "queue", ChangeUri: "uri", BaseUri: "base", RequestState: "failed",
	}
	summary := proto.Clone(sharedSummary).(*RequestSummary)
	summary.StateUpdatedAtMs = 100
	summary.AcceptanceTime = &RequestSummary_AcceptedAtMs{AcceptedAtMs: 50}
	summary.OutcomeReason = "build_failed"
	zeroAcceptanceSummary := proto.Clone(summary).(*RequestSummary)
	zeroAcceptanceSummary.AcceptanceTime = &RequestSummary_AcceptedAtMs{AcceptedAtMs: 0}
	status := proto.Clone(sharedStatus).(*GetProjectStatusByURIResponse)
	status.UpdatedAtMs = 200
	status.RepositoryResult = &GetProjectStatusByURIResponse_RepositoryBreakageDegree{RepositoryBreakageDegree: 0}
	status.ProjectResultsComplete = true
	status.Projects = []*ProjectValidation{{Project: "project", Result: &ProjectValidation_BreakageDegree{BreakageDegree: 1}}}
	status.NextPageToken = "next"

	codecs := []struct {
		name      string
		marshal   func(proto.Message) ([]byte, error)
		unmarshal func([]byte, proto.Message) error
	}{
		{"protobuf", proto.Marshal, proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal},
		{"unknown_tolerant_json", protojson.Marshal, protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal},
	}
	tests := []struct {
		name        string
		source      proto.Message
		destination func() proto.Message
		want        proto.Message
	}{
		{"summary_to_legacy_status", summary, func() proto.Message { return &GetProjectStatusByURIResponse{} }, sharedStatus},
		{"legacy_status_to_summary", status, func() proto.Message { return &RequestSummary{} }, sharedSummary},
		{"summary_round_trip", summary, func() proto.Message { return &RequestSummary{} }, summary},
		{"explicit_zero_acceptance_round_trip", zeroAcceptanceSummary, func() proto.Message { return &RequestSummary{} }, zeroAcceptanceSummary},
	}
	for _, codec := range codecs {
		t.Run(codec.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					encoded, err := codec.marshal(tt.source)
					require.NoError(t, err)
					decoded := tt.destination()
					require.NoError(t, codec.unmarshal(encoded, decoded))
					require.True(t, proto.Equal(tt.want, decoded), "decoded: %v", decoded)
				})
			}
		})
	}
}
