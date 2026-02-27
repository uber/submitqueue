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
