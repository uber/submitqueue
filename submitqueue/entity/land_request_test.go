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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/entity/change"
)

func TestLandRequest_ToBytes(t *testing.T) {
	req := LandRequest{
		ID:    "test-queue/123",
		Queue: "test-queue",
		Change: change.Change{URIs: []string{
			"github://uber/submitqueue/pull/456/abcdef0123456789abcdef0123456789abcdef01",
			"github://uber/submitqueue/pull/789/0123456789abcdef0123456789abcdef01234567",
		}},
		LandStrategy: RequestLandStrategyRebase,
	}

	data, err := req.ToBytes()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify JSON contains expected fields
	jsonStr := string(data)
	assert.Contains(t, jsonStr, "test-queue/123")
	assert.Contains(t, jsonStr, "github://uber/submitqueue/pull/456/abcdef0123456789abcdef0123456789abcdef01")
	assert.Contains(t, jsonStr, "rebase")
}

func TestLandRequestFromBytes(t *testing.T) {
	original := LandRequest{
		ID:           "my-queue/999",
		Queue:        "my-queue",
		Change:       change.Change{URIs: []string{"code.uber.internal.com/D111"}},
		LandStrategy: RequestLandStrategyMerge,
	}

	// Serialize
	data, err := original.ToBytes()
	require.NoError(t, err)

	// Deserialize
	deserialized, err := LandRequestFromBytes(data)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Queue, deserialized.Queue)
	assert.Equal(t, original.Change.URIs, deserialized.Change.URIs)
	assert.Equal(t, original.LandStrategy, deserialized.LandStrategy)
}

func TestLandRequestFromBytes_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"invalid": json"}`)

	_, err := LandRequestFromBytes(invalidJSON)
	assert.Error(t, err)
}

func TestLandRequestFromBytes_EmptyData(t *testing.T) {
	emptyJSON := []byte(`{}`)

	req, err := LandRequestFromBytes(emptyJSON)
	require.NoError(t, err)

	// Empty JSON should deserialize with zero values
	assert.Empty(t, req.ID)
	assert.Empty(t, req.Queue)
	assert.Equal(t, RequestLandStrategyUnknown, req.LandStrategy)
}

func TestLandRequest_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  LandRequest
	}{
		{
			name: "github stacked diff",
			req: LandRequest{
				ID:    "queue1/100",
				Queue: "queue1",
				Change: change.Change{URIs: []string{
					"github://uber/repo-a/pull/101/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"github://uber/repo-a/pull/102/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"github://uber/repo-a/pull/103/cccccccccccccccccccccccccccccccccccccccc",
				}},
				LandStrategy: RequestLandStrategySquashRebase,
			},
		},
		{
			name: "phabricator revision",
			req: LandRequest{
				ID:           "queue2/200",
				Queue:        "queue2",
				Change:       change.Change{URIs: []string{"code.uber.internal.com/D12345"}},
				LandStrategy: RequestLandStrategyRebase,
			},
		},
		{
			name: "github enterprise request",
			req: LandRequest{
				ID:           "queue3/300",
				Queue:        "queue3",
				Change:       change.Change{URIs: []string{"github.uber.com/internal/service/999/deadbeef12"}},
				LandStrategy: RequestLandStrategyMerge,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			data, err := tt.req.ToBytes()
			require.NoError(t, err)

			// Deserialize
			deserialized, err := LandRequestFromBytes(data)
			require.NoError(t, err)

			// Verify complete equality
			assert.Equal(t, tt.req, deserialized)
		})
	}
}
