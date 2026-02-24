package build

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuildID(t *testing.T) {
	testCases := []struct {
		name     string
		provider string
		id       string
		expected string
	}{
		{
			name:     "buildkite with org/pipeline/number",
			provider: "buildkite",
			id:       "uber/submitqueue-ci/123",
			expected: "buildkite://uber/submitqueue-ci/123",
		},
		{
			name:     "jenkins with build number",
			provider: "jenkins",
			id:       "456",
			expected: "jenkins://456",
		},
		{
			name:     "mock with sequential number",
			provider: "mock",
			id:       "1",
			expected: "mock://1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buildID := NewBuildID(tc.provider, tc.id)
			assert.Equal(t, tc.expected, string(buildID))
			assert.Equal(t, tc.expected, buildID.String())
		})
	}
}

func TestParseBuildID(t *testing.T) {
	testCases := []struct {
		name             string
		buildID          BuildID
		expectedProvider string
		expectedID       string
		expectError      bool
	}{
		{
			name:             "valid buildkite ID",
			buildID:          "buildkite://uber/submitqueue-ci/123",
			expectedProvider: "buildkite",
			expectedID:       "uber/submitqueue-ci/123",
			expectError:      false,
		},
		{
			name:             "valid jenkins ID",
			buildID:          "jenkins://456",
			expectedProvider: "jenkins",
			expectedID:       "456",
			expectError:      false,
		},
		{
			name:             "valid mock ID",
			buildID:          "mock://1",
			expectedProvider: "mock",
			expectedID:       "1",
			expectError:      false,
		},
		{
			name:        "missing separator",
			buildID:     "buildkite-123",
			expectError: true,
		},
		{
			name:        "empty provider",
			buildID:     "://123",
			expectError: true,
		},
		{
			name:        "empty ID",
			buildID:     "buildkite://",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider, id, err := ParseBuildID(tc.buildID)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedProvider, provider)
			assert.Equal(t, tc.expectedID, id)
		})
	}
}

func TestBuildIDRoundTrip(t *testing.T) {
	testCases := []struct {
		provider string
		id       string
	}{
		{"buildkite", "uber/submitqueue-ci/123"},
		{"jenkins", "456"},
		{"mock", "1"},
	}

	for _, tc := range testCases {
		t.Run(tc.provider, func(t *testing.T) {
			// Create BuildID
			buildID := NewBuildID(tc.provider, tc.id)

			// Parse it back
			provider, id, err := ParseBuildID(buildID)
			require.NoError(t, err)

			// Verify round trip
			assert.Equal(t, tc.provider, provider)
			assert.Equal(t, tc.id, id)
		})
	}
}
