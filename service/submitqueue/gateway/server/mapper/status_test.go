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
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestProtoToStatusRequest(t *testing.T) {
	tests := []struct {
		name     string
		req      *pb.StatusRequest
		expected entity.StatusRequest
	}{
		{
			name:     "maps sqid to ID",
			req:      &pb.StatusRequest{Sqid: "test-queue/42"},
			expected: entity.StatusRequest{ID: "test-queue/42"},
		},
		{
			name:     "empty request yields zero value",
			req:      &pb.StatusRequest{},
			expected: entity.StatusRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ProtoToStatusRequest(tt.req)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestCurrentStateToProto(t *testing.T) {
	tests := []struct {
		name     string
		state    request.CurrentState
		expected *pb.StatusResponse
	}{
		{
			name: "maps all fields",
			state: request.CurrentState{
				Status:    entity.RequestStatusValidating,
				LastError: "validation failed",
				Metadata:  map[string]string{"step": "lint"},
			},
			expected: &pb.StatusResponse{
				Status:    string(entity.RequestStatusValidating),
				LastError: "validation failed",
				Metadata:  map[string]string{"step": "lint"},
			},
		},
		{
			name:  "zero value state maps to empty response",
			state: request.CurrentState{},
			expected: &pb.StatusResponse{
				Status: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CurrentStateToProto(tt.state)
			assert.Equal(t, tt.expected, got)
		})
	}
}
