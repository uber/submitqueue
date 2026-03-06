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

func TestBatchState_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		state    BatchState
		terminal bool
	}{
		{name: "unknown", state: BatchStateUnknown, terminal: false},
		{name: "created", state: BatchStateCreated, terminal: false},
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

func TestBatch_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		batch Batch
	}{
		{
			name: "batch with single request",
			batch: Batch{
				ID:       "queueA/batch/1",
				Queue:    "queueA",
				Contains: []string{"queueA/1"},
				State:    BatchStateCreated,
				Version:  1,
			},
		},
		{
			name: "batch with multiple requests",
			batch: Batch{
				ID:       "queueB/batch/42",
				Queue:    "queueB",
				Contains: []string{"queueB/10", "queueB/11", "queueB/12"},
				State:    BatchStateSpeculating,
				Version:  3,
			},
		},
		{
			name: "batch with dependencies",
			batch: Batch{
				ID:       "queueA/batch/3",
				Queue:    "queueA",
				Contains: []string{"queueA/5"},
				Dependencies: []map[string]interface{}{
					{"id": "queueA/batch/1"},
					{"id": "queueA/batch/2"},
				},
				State:   BatchStateCreated,
				Version: 1,
			},
		},
		{
			name: "batch in terminal state",
			batch: Batch{
				ID:       "queueC/batch/99",
				Queue:    "queueC",
				Contains: []string{"queueC/50"},
				State:    BatchStateSucceeded,
				Version:  5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.batch.ToBytes()
			require.NoError(t, err)

			deserialized, err := BatchFromBytes(data)
			require.NoError(t, err)

			assert.Equal(t, tt.batch, deserialized)
		})
	}
}

func TestBatchFromBytes_InvalidJSON(t *testing.T) {
	_, err := BatchFromBytes([]byte(`{"invalid": json"}`))
	assert.Error(t, err)
}

func TestBatchFromBytes_EmptyJSON(t *testing.T) {
	batch, err := BatchFromBytes([]byte(`{}`))
	require.NoError(t, err)

	assert.Empty(t, batch.ID)
	assert.Empty(t, batch.Queue)
	assert.Nil(t, batch.Contains)
	assert.Nil(t, batch.Dependencies)
	assert.Equal(t, BatchStateUnknown, batch.State)
	assert.Equal(t, int32(0), batch.Version)
}

func TestBatchFromBytes_EmptyBytes(t *testing.T) {
	_, err := BatchFromBytes([]byte{})
	assert.Error(t, err)
}

func TestBatchFromBytes_NilBytes(t *testing.T) {
	_, err := BatchFromBytes(nil)
	assert.Error(t, err)
}
