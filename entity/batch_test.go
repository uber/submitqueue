package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchState_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		state    BatchState
		terminal bool
	}{
		{name: "unknown", state: BatchStateUnknown, terminal: false},
		{name: "scheduled", state: BatchStateScheduled, terminal: false},
		{name: "speculating", state: BatchStateSpeculating, terminal: false},
		{name: "finalizing", state: BatchStateFinalizing, terminal: false},
		{name: "succeeded", state: BatchStateSucceeded, terminal: true},
		{name: "failed", state: BatchStateFailed, terminal: true},
		{name: "cancelled", state: BatchStateCancelled, terminal: true},
		{name: "arbitrary string", state: BatchState("something_else"), terminal: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.terminal, tt.state.IsTerminal())
		})
	}
}
