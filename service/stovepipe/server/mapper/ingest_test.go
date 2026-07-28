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
			result:   entity.IngestResult{ID: "request/monorepo/main/7"},
			expected: &pb.IngestResponse{Id: "request/monorepo/main/7"},
		},
		{
			name:     "zero value result yields empty response",
			result:   entity.IngestResult{},
			expected: &pb.IngestResponse{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IngestResultToProto(tt.result)
			assert.Equal(t, tt.expected, got)
		})
	}
}
