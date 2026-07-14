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

func TestBuildStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		status   BuildStatus
		expected bool
	}{
		{name: "succeeded is terminal", status: BuildStatusSucceeded, expected: true},
		{name: "failed is terminal", status: BuildStatusFailed, expected: true},
		{name: "cancelled is terminal", status: BuildStatusCancelled, expected: true},
		{name: "accepted is not terminal", status: BuildStatusAccepted, expected: false},
		{name: "running is not terminal", status: BuildStatusRunning, expected: false},
		{name: "unknown is not terminal", status: BuildStatusUnknown, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.IsTerminal())
		})
	}
}

func TestBuild_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		build Build
	}{
		{
			name: "accepted incremental build",
			build: Build{
				ID:        "bk-1001",
				RequestID: "request/monorepo/main/42",
				URI:       "git://remote/monorepo/main/deadbeef",
				BaseURI:   "git://remote/monorepo/main/cafef00d",
				Status:    BuildStatusAccepted,
				Version:   1,
			},
		},
		{
			name: "succeeded full build with no baseline",
			build: Build{
				ID:        "bk-1002",
				RequestID: "request/monorepo/main/43",
				URI:       "git://remote/monorepo/main/feedface",
				Status:    BuildStatusSucceeded,
				Version:   3,
			},
		},
		{
			name: "failed build",
			build: Build{
				ID:        "bk-1003",
				RequestID: "request/monorepo/main/44",
				URI:       "git://remote/monorepo/main/0ff1ce",
				BaseURI:   "git://remote/monorepo/main/cafef00d",
				Status:    BuildStatusFailed,
				Version:   2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.build.ToBytes()
			require.NoError(t, err)

			deserialized, err := BuildFromBytes(data)
			require.NoError(t, err)

			assert.Equal(t, tt.build, deserialized)
		})
	}
}

func TestBuildFromBytes_InvalidJSON(t *testing.T) {
	_, err := BuildFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}

func TestBuildFromBytes_EmptyData(t *testing.T) {
	build, err := BuildFromBytes([]byte(`{}`))
	require.NoError(t, err)

	assert.Empty(t, build.ID)
	assert.Empty(t, build.RequestID)
	assert.Equal(t, BuildStatusUnknown, build.Status)
	assert.Equal(t, int32(0), build.Version)
}

func TestBuildID_SerializationRoundTrip(t *testing.T) {
	original := BuildID{ID: "bk-1001"}

	data, err := original.ToBytes()
	require.NoError(t, err)

	deserialized, err := BuildIDFromBytes(data)
	require.NoError(t, err)

	assert.Equal(t, original, deserialized)
}

func TestBuildIDFromBytes_InvalidJSON(t *testing.T) {
	_, err := BuildIDFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}
