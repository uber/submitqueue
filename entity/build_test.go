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

func TestBuildChange_Creation(t *testing.T) {
	tests := []struct {
		name     string
		change   BuildChange
		wantID   string
		wantAction ChangeAction
	}{
		{
			name: "apply action",
			change: BuildChange{
				ChangeID: "PR-42",
				Action:   ChangeActionApply,
			},
			wantID:   "PR-42",
			wantAction: ChangeActionApply,
		},
		{
			name: "validate action",
			change: BuildChange{
				ChangeID: "D12345",
				Action:   ChangeActionValidate,
			},
			wantID:   "D12345",
			wantAction: ChangeActionValidate,
		},
		{
			name: "unknown action",
			change: BuildChange{
				ChangeID: "123",
				Action:   ChangeActionUnknown,
			},
			wantID:   "123",
			wantAction: ChangeActionUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantID, tt.change.ChangeID)
			assert.Equal(t, tt.wantAction, tt.change.Action)
		})
	}
}
