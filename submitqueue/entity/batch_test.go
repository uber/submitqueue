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
)

func TestBatchState_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		state    BatchState
		terminal bool
	}{
		{name: "unknown", state: BatchStateUnknown, terminal: false},
		{name: "creating", state: BatchStateCreating, terminal: false},
		{name: "created", state: BatchStateCreated, terminal: false},
		{name: "speculating", state: BatchStateSpeculating, terminal: false},
		{name: "landing", state: BatchStateLanding, terminal: false},
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

func TestIsCancellable(t *testing.T) {
	assert.True(t, BatchStateCreated.IsCancellable())
	assert.True(t, BatchStateSpeculating.IsCancellable())
	assert.True(t, BatchStateCancelling.IsCancellable())
	assert.True(t, BatchState("future").IsCancellable())
	assert.False(t, BatchStateUnknown.IsCancellable())
	assert.False(t, BatchStateCreating.IsCancellable())
	assert.False(t, BatchStateLanding.IsCancellable())
	assert.False(t, BatchStateSucceeded.IsCancellable())
	assert.False(t, BatchStateFailed.IsCancellable())
	assert.False(t, BatchStateCancelled.IsCancellable())
}

func TestActiveBatchStates_ExcludesCreating(t *testing.T) {
	assert.NotContains(t, ActiveBatchStates(), BatchStateCreating)
}

func TestAllBatchStates_SupersetOfStateSubsets(t *testing.T) {
	all := AllBatchStates()
	assert.NotContains(t, all, BatchStateUnknown)
	assert.Subset(t, all, ActiveBatchStates())
	assert.Subset(t, all, DependencyBatchStates())
	assert.Subset(t, all, []BatchState{BatchStateSucceeded, BatchStateFailed, BatchStateCancelled})
}

func TestDependencyBatchStates_ExcludesCreating(t *testing.T) {
	assert.NotContains(t, DependencyBatchStates(), BatchStateCreating)
}
