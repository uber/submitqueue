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
		{
			name:     "succeeded is terminal",
			status:   BuildStatusSucceeded,
			expected: true,
		},
		{
			name:     "failed is terminal",
			status:   BuildStatusFailed,
			expected: true,
		},
		{
			name:     "cancelled is terminal",
			status:   BuildStatusCancelled,
			expected: true,
		},
		{
			name:     "accepted is not terminal",
			status:   BuildStatusAccepted,
			expected: false,
		},
		{
			name:     "running is not terminal",
			status:   BuildStatusRunning,
			expected: false,
		},
		{
			name:     "unknown is not terminal",
			status:   BuildStatusUnknown,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.IsTerminal())
		})
	}
}

func TestBuild_ToBytes(t *testing.T) {
	build := Build{
		ID:      "build-1",
		BatchID: "batch-1",
		SpeculationPath: SpeculationPathInfo{
			Base: []string{"batch-0", "batch-prev"},
		},
		Score:  0.85,
		Status: BuildStatusAccepted,
	}

	data, err := build.ToBytes()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify JSON contains expected fields
	jsonStr := string(data)
	assert.Contains(t, jsonStr, "build-1")
	assert.Contains(t, jsonStr, "batch-1")
	assert.Contains(t, jsonStr, "accepted")
}

func TestBuildFromBytes(t *testing.T) {
	original := Build{
		ID:      "build-42",
		BatchID: "batch-7",
		SpeculationPath: SpeculationPathInfo{
			Base: []string{"batch-5", "batch-6"},
		},
		Score:  0.92,
		Status: BuildStatusAccepted,
	}

	// Serialize
	data, err := original.ToBytes()
	require.NoError(t, err)

	// Deserialize
	deserialized, err := BuildFromBytes(data)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.BatchID, deserialized.BatchID)
	assert.Equal(t, original.SpeculationPath.Base, deserialized.SpeculationPath.Base)
	assert.Equal(t, original.Score, deserialized.Score)
	assert.Equal(t, original.Status, deserialized.Status)
}

func TestBuildFromBytes_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"invalid": json"}`)

	_, err := BuildFromBytes(invalidJSON)
	assert.Error(t, err)
}

func TestBuildFromBytes_EmptyData(t *testing.T) {
	emptyJSON := []byte(`{}`)

	build, err := BuildFromBytes(emptyJSON)
	require.NoError(t, err)

	// Empty JSON should deserialize with zero values
	assert.Empty(t, build.ID)
	assert.Empty(t, build.BatchID)
	assert.Equal(t, BuildStatusUnknown, build.Status)
	assert.Equal(t, float32(0), build.Score)
}

func TestBuild_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		build Build
	}{
		{
			name: "accepted build with speculation path",
			build: Build{
				ID:      "build-100",
				BatchID: "batch-50",
				SpeculationPath: SpeculationPathInfo{
					Base: []string{"batch-48", "batch-49"},
				},
				Score:  0.75,
				Status: BuildStatusAccepted,
			},
		},
		{
			name: "succeeded build with no speculation base",
			build: Build{
				ID:      "build-200",
				BatchID: "batch-60",
				Score:   1.0,
				Status:  BuildStatusSucceeded,
			},
		},
		{
			name: "failed build with zero score",
			build: Build{
				ID:      "build-300",
				BatchID: "batch-70",
				SpeculationPath: SpeculationPathInfo{
					Base: []string{"batch-65"},
				},
				Score:  0,
				Status: BuildStatusFailed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			data, err := tt.build.ToBytes()
			require.NoError(t, err)

			// Deserialize
			deserialized, err := BuildFromBytes(data)
			require.NoError(t, err)

			// Verify complete equality
			assert.Equal(t, tt.build, deserialized)
		})
	}
}
