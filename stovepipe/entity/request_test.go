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
	"github.com/uber/submitqueue/platform/base/change"
)

func TestRequest_ToBytes(t *testing.T) {
	req := Request{
		ID:    "stovepipe-monorepo/123",
		Queue: "stovepipe-monorepo",
		Change: change.Change{URIs: []string{
			"git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/abcdef0123456789abcdef0123456789abcdef01",
		}},
		State:   RequestStateStarted,
		Version: 1,
	}

	data, err := req.ToBytes()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	jsonStr := string(data)
	assert.Contains(t, jsonStr, "stovepipe-monorepo/123")
	assert.Contains(t, jsonStr, "abcdef0123456789abcdef0123456789abcdef01")
	assert.Contains(t, jsonStr, "started")
}

func TestRequestFromBytes(t *testing.T) {
	original := Request{
		ID:      "my-queue/999",
		Queue:   "my-queue",
		Change:  change.Change{URIs: []string{"git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/0123456789"}},
		State:   RequestStateBuilding,
		Version: 3,
	}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := RequestFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Queue, deserialized.Queue)
	assert.Equal(t, original.Change.URIs, deserialized.Change.URIs)
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

	assert.Empty(t, req.ID)
	assert.Empty(t, req.Queue)
	assert.Equal(t, RequestStateUnknown, req.State)
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
		{RequestStateBatched, false},
		{RequestStateBuilding, false},
		{RequestStateSucceeded, true},
		{RequestStateFailed, true},
		{RequestStateError, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			assert.Equal(t, tt.terminal, IsRequestStateTerminal(tt.state))
		})
	}
}

func TestRequest_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "single commit started",
			req: Request{
				ID:      "queue1/100",
				Queue:   "queue1",
				Change:  change.Change{URIs: []string{"git://git.example.com/uber/repo-a/refs%2Fheads%2Fmain/aaaaaaaaaa"}},
				State:   RequestStateStarted,
				Version: 1,
			},
		},
		{
			name: "succeeded terminal",
			req: Request{
				ID:      "queue2/200",
				Queue:   "queue2",
				Change:  change.Change{URIs: []string{"git://git.example.com/uber/repo-b/refs%2Fheads%2Fmain/bbbbbbbbbb"}},
				State:   RequestStateSucceeded,
				Version: 5,
			},
		},
		{
			name: "failed terminal",
			req: Request{
				ID:      "queue3/300",
				Queue:   "queue3",
				Change:  change.Change{URIs: []string{"git://git.example.com/uber/repo-c/refs%2Fheads%2Fmain/cccccccccc"}},
				State:   RequestStateFailed,
				Version: 10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.req.ToBytes()
			require.NoError(t, err)

			deserialized, err := RequestFromBytes(data)
			require.NoError(t, err)

			assert.Equal(t, tt.req, deserialized)
		})
	}
}

func TestRequestID_SerializationRoundTrip(t *testing.T) {
	original := RequestID{ID: "stovepipe-monorepo/42"}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := RequestIDFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original, deserialized)
}

func TestRequestIDFromBytes_InvalidJSON(t *testing.T) {
	_, err := RequestIDFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}
