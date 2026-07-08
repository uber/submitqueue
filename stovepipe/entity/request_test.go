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
)

func TestRequest_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "accepted with resolved uri",
			req: Request{
				ID:       "request/monorepo/main/100",
				Queue:    "monorepo/main",
				URI:      "git://remote/monorepo/main/abcdef0123456789",
				Sequence: 100,
				State:    RequestStateAccepted,
				Version:  1,
			},
		},
		{
			name: "processing with strategy and baseline",
			req: Request{
				ID:            "request/monorepo/main/101",
				Queue:         "monorepo/main",
				URI:           "git://remote/monorepo/main/bbbb2222",
				Sequence:      101,
				State:         RequestStateProcessing,
				BuildStrategy: BuildStrategyIncrementalSinceGreen,
				BaseURI:       "git://remote/monorepo/main/green-aaaa",
				Version:       2,
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

func TestRequestFromBytes_InvalidJSON(t *testing.T) {
	_, err := RequestFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}

func TestRequestFromBytes_EmptyData(t *testing.T) {
	req, err := RequestFromBytes([]byte(`{}`))
	require.NoError(t, err)

	assert.Empty(t, req.ID)
	assert.Empty(t, req.Queue)
	assert.Empty(t, req.URI)
	assert.Equal(t, RequestStateUnknown, req.State)
	assert.Equal(t, int32(0), req.Version)
}

func TestRequestID_SerializationRoundTrip(t *testing.T) {
	original := RequestID{ID: "request/monorepo/main/100"}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := RequestIDFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original, deserialized)
}
