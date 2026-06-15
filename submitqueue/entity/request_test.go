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
	"github.com/uber/submitqueue/entity/mergestrategy"
)

func TestRequest_ToBytes(t *testing.T) {
	req := Request{
		ID:    "test-queue/123",
		Queue: "test-queue",
		Change: change.Change{URIs: []string{
			"github://uber/submitqueue/pull/456/abcdef0123456789abcdef0123456789abcdef01",
			"github://uber/submitqueue/pull/789/0123456789abcdef0123456789abcdef01234567",
		}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
		State:        RequestStateStarted,
		Version:      1,
	}

	data, err := req.ToBytes()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify JSON contains expected fields
	jsonStr := string(data)
	assert.Contains(t, jsonStr, "test-queue/123")
	assert.Contains(t, jsonStr, "github://uber/submitqueue/pull/456/abcdef0123456789abcdef0123456789abcdef01")
	assert.Contains(t, jsonStr, "rebase")
	assert.Contains(t, jsonStr, "started")
}

func TestRequestFromBytes(t *testing.T) {
	original := Request{
		ID:           "my-queue/999",
		Queue:        "my-queue",
		Change:       change.Change{URIs: []string{"code.uber.internal.com/D111"}},
		LandStrategy: mergestrategy.MergeStrategyMerge,
		State:        RequestStateProcessing,
		Version:      3,
	}

	// Serialize
	data, err := original.ToBytes()
	require.NoError(t, err)

	// Deserialize
	deserialized, err := RequestFromBytes(data)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Queue, deserialized.Queue)
	assert.Equal(t, original.Change.URIs, deserialized.Change.URIs)
	assert.Equal(t, original.LandStrategy, deserialized.LandStrategy)
	assert.Equal(t, original.State, deserialized.State)
	assert.Equal(t, original.Version, deserialized.Version)
}

func TestRequestFromBytes_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"invalid": json"}`)

	_, err := RequestFromBytes(invalidJSON)
	assert.Error(t, err)
}

func TestRequestFromBytes_EmptyData(t *testing.T) {
	emptyJSON := []byte(`{}`)

	req, err := RequestFromBytes(emptyJSON)
	require.NoError(t, err)

	// Empty JSON should deserialize with zero values
	assert.Empty(t, req.ID)
	assert.Empty(t, req.Queue)
	assert.Equal(t, RequestStateUnknown, req.State)
	assert.Equal(t, mergestrategy.MergeStrategyUnknown, req.LandStrategy)
	assert.Equal(t, int32(0), req.Version)
}

func TestIsRequestStateTerminal(t *testing.T) {
	tests := []struct {
		state    RequestState
		terminal bool
	}{
		{RequestStateUnknown, false},
		{RequestStateStarted, false},
		{RequestStateValidated, false},
		{RequestStateProcessing, false},
		{RequestStateCancelling, false}, // intent only — not terminal
		{RequestStateLanded, true},
		{RequestStateError, true},
		{RequestStateCancelled, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			assert.Equal(t, tt.terminal, IsRequestStateTerminal(tt.state))
		})
	}
}

func TestIsRequestStateHalted(t *testing.T) {
	tests := []struct {
		state  RequestState
		halted bool
	}{
		{RequestStateUnknown, false},
		{RequestStateStarted, false},
		{RequestStateValidated, false},
		{RequestStateProcessing, false},
		{RequestStateCancelling, true}, // intent halts forward progress
		{RequestStateLanded, true},
		{RequestStateError, true},
		{RequestStateCancelled, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			assert.Equal(t, tt.halted, IsRequestStateHalted(tt.state))
		})
	}
}

func TestRequest_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "github stacked diff",
			req: Request{
				ID:    "queue1/100",
				Queue: "queue1",
				Change: change.Change{URIs: []string{
					"github://uber/repo-a/pull/101/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"github://uber/repo-a/pull/102/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"github://uber/repo-a/pull/103/cccccccccccccccccccccccccccccccccccccccc",
				}},
				LandStrategy: mergestrategy.MergeStrategySquashRebase,
				State:        RequestStateLanded,
				Version:      5,
			},
		},
		{
			name: "phabricator revision",
			req: Request{
				ID:           "queue2/200",
				Queue:        "queue2",
				Change:       change.Change{URIs: []string{"code.uber.internal.com/D12345"}},
				LandStrategy: mergestrategy.MergeStrategyRebase,
				State:        RequestStateStarted,
				Version:      1,
			},
		},
		{
			name: "github enterprise request",
			req: Request{
				ID:           "queue3/300",
				Queue:        "queue3",
				Change:       change.Change{URIs: []string{"github.uber.com/internal/service/999/deadbeef12"}},
				LandStrategy: mergestrategy.MergeStrategyMerge,
				State:        RequestStateError,
				Version:      10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			data, err := tt.req.ToBytes()
			require.NoError(t, err)

			// Deserialize
			deserialized, err := RequestFromBytes(data)
			require.NoError(t, err)

			// Verify complete equality
			assert.Equal(t, tt.req, deserialized)
		})
	}
}
