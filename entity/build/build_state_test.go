package build

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBuildState_IsTerminal tests the IsTerminal method for all build states.
func TestBuildState_IsTerminal(t *testing.T) {
	testCases := []struct {
		name     string
		state    BuildState
		expected bool
	}{
		{
			name:     "passed is terminal",
			state:    BuildStatePassed,
			expected: true,
		},
		{
			name:     "failed is terminal",
			state:    BuildStateFailed,
			expected: true,
		},
		{
			name:     "cancelled is terminal",
			state:    BuildStateCancelled,
			expected: true,
		},
		{
			name:     "queued is not terminal",
			state:    BuildStateQueued,
			expected: false,
		},
		{
			name:     "running is not terminal",
			state:    BuildStateRunning,
			expected: false,
		},
		{
			name:     "blocked is not terminal",
			state:    BuildStateBlocked,
			expected: false,
		},
		{
			name:     "unknown is not terminal",
			state:    BuildStateUnknown,
			expected: false,
		},
		{
			name:     "empty string is not terminal",
			state:    BuildState(""),
			expected: false,
		},
		{
			name:     "invalid state is not terminal",
			state:    BuildState("invalid"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.state.IsTerminal()
			assert.Equal(t, tc.expected, result,
				"IsTerminal() for %s should return %v", tc.state, tc.expected)
		})
	}
}

// TestBuildState_Constants verifies the string values of all build state constants.
func TestBuildState_Constants(t *testing.T) {
	testCases := []struct {
		name     string
		state    BuildState
		expected string
	}{
		{
			name:     "unknown",
			state:    BuildStateUnknown,
			expected: "",
		},
		{
			name:     "queued",
			state:    BuildStateQueued,
			expected: "queued",
		},
		{
			name:     "running",
			state:    BuildStateRunning,
			expected: "running",
		},
		{
			name:     "passed",
			state:    BuildStatePassed,
			expected: "passed",
		},
		{
			name:     "failed",
			state:    BuildStateFailed,
			expected: "failed",
		},
		{
			name:     "cancelled",
			state:    BuildStateCancelled,
			expected: "cancelled",
		},
		{
			name:     "blocked",
			state:    BuildStateBlocked,
			expected: "blocked",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, string(tc.state),
				"BuildState constant should have expected string value")
		})
	}
}

// TestBuildState_ZeroValue verifies that the zero value is BuildStateUnknown.
func TestBuildState_ZeroValue(t *testing.T) {
	var state BuildState
	assert.Equal(t, BuildStateUnknown, state, "Zero value should be BuildStateUnknown")
	assert.Equal(t, "", string(state), "Zero value should be empty string")
	assert.False(t, state.IsTerminal(), "Zero value should not be terminal")
}

// TestBuildState_TerminalStates verifies all terminal states are accounted for.
func TestBuildState_TerminalStates(t *testing.T) {
	terminalStates := []BuildState{
		BuildStatePassed,
		BuildStateFailed,
		BuildStateCancelled,
	}

	for _, state := range terminalStates {
		assert.True(t, state.IsTerminal(),
			"State %s should be terminal", state)
	}
}

// TestBuildState_NonTerminalStates verifies all non-terminal states are accounted for.
func TestBuildState_NonTerminalStates(t *testing.T) {
	nonTerminalStates := []BuildState{
		BuildStateUnknown,
		BuildStateQueued,
		BuildStateRunning,
		BuildStateBlocked,
	}

	for _, state := range nonTerminalStates {
		assert.False(t, state.IsTerminal(),
			"State %s should not be terminal", state)
	}
}
