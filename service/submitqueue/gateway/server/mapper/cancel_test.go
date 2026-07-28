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
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestProtoToCancelRequest(t *testing.T) {
	tests := []struct {
		name     string
		req      *pb.CancelRequest
		expected entity.CancelRequest
	}{
		{
			name:     "maps sqid and reason",
			req:      &pb.CancelRequest{Sqid: "test-queue/42", Reason: "obsolete change"},
			expected: entity.CancelRequest{ID: "test-queue/42", Reason: "obsolete change"},
		},
		{
			name:     "maps sqid without reason",
			req:      &pb.CancelRequest{Sqid: "test-queue/1"},
			expected: entity.CancelRequest{ID: "test-queue/1"},
		},
		{
			name:     "empty request yields zero value",
			req:      &pb.CancelRequest{},
			expected: entity.CancelRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ProtoToCancelRequest(tt.req)
			assert.Equal(t, tt.expected, got)
		})
	}
}
