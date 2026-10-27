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

func TestProtoToIngestRequest(t *testing.T) {
	tests := []struct {
		name     string
		req      *pb.IngestRequest
		expected entity.IngestRequest
	}{
		{
			name:     "maps queue",
			req:      &pb.IngestRequest{Queue: "monorepo/main"},
			expected: entity.IngestRequest{Queue: "monorepo/main"},
		},
		{
			name:     "empty request yields zero value",
			req:      &pb.IngestRequest{},
			expected: entity.IngestRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ProtoToIngestRequest(tt.req)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIngestResultToProto(t *testing.T) {
	tests := []struct {
		name     string
		result   entity.IngestResult
		expected *pb.IngestResponse
	}{
		{
			name:     "maps ID",
			result:   entity.IngestResult{ID: "7"},
			expected: &pb.IngestResponse{Id: "7", Enablement: &pb.QueueEnablementStatus{}},
		},
		{
			name:     "zero value result yields empty response",
			result:   entity.IngestResult{},
			expected: &pb.IngestResponse{Enablement: &pb.QueueEnablementStatus{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IngestResultToProto(tt.result)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIngestDecisionMapping(t *testing.T) {
	for _, tc := range []struct {
		name  string
		wire  pb.QueueEnablement
		state entity.QueueEnablement
	}{
		{"enable", pb.QueueEnablement_QUEUE_ENABLEMENT_ENABLED, entity.QueueEnablementEnabled},
		{"disable", pb.QueueEnablement_QUEUE_ENABLEMENT_DISABLED, entity.QueueEnablementDisabled},
		{"unknown", pb.QueueEnablement_QUEUE_ENABLEMENT_UNKNOWN, entity.QueueEnablementUnknown},
		{"future enum", pb.QueueEnablement(99), entity.QueueEnablementUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ProtoToIngestRequest(&pb.IngestRequest{Queue: "repo/main", Enablement: tc.wire})
			assert.Equal(t, tc.wire != pb.QueueEnablement_QUEUE_ENABLEMENT_UNKNOWN, got.HasEnablement)
			assert.Equal(t, tc.state, got.Enablement)
		})
	}
	assert.Equal(t, entity.IngestRequest{}, ProtoToIngestRequest(nil))
	assert.False(t, ProtoToIngestRequest(&pb.IngestRequest{Enablement: pb.QueueEnablement_QUEUE_ENABLEMENT_UNKNOWN}).HasEnablement)
	state := entity.QueueState{State: entity.QueueEnablementDisabled, Version: 3, EffectiveFromCommitURI: "D", ChangedAtMs: 1000}
	assert.Equal(t, &pb.QueueEnablementStatus{State: pb.QueueEnablement_QUEUE_ENABLEMENT_DISABLED, Version: 3, EffectiveFromCommitUri: "D", ChangedAtMs: 1000}, IngestResultToProto(entity.IngestResult{Enablement: state}).Enablement)
}
