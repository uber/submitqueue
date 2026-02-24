package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		status   BuildStatus
		expected bool
	}{
		{
			name:     "passed is terminal",
			status:   BuildStatusPassed,
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
			name:     "queued is not terminal",
			status:   BuildStatusQueued,
			expected: false,
		},
		{
			name:     "running is not terminal",
			status:   BuildStatusRunning,
			expected: false,
		},
		{
			name:     "blocked is not terminal",
			status:   BuildStatusBlocked,
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
