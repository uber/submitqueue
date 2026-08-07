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

func TestRequestState_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		state    RequestState
		expected bool
	}{
		{name: "superseded is terminal", state: RequestStateSuperseded, expected: true},
		{name: "succeeded is terminal", state: RequestStateSucceeded, expected: true},
		{name: "failed is terminal", state: RequestStateFailed, expected: true},
		{name: "cancelled is terminal", state: RequestStateCancelled, expected: true},
		{name: "unknown is not terminal", state: RequestStateUnknown, expected: false},
		{name: "accepted is not terminal", state: RequestStateAccepted, expected: false},
		{name: "processing is not terminal", state: RequestStateProcessing, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.IsTerminal())
		})
	}
}

func TestRequestState_HasBuildOutcome(t *testing.T) {
	tests := []struct {
		name     string
		state    RequestState
		expected bool
	}{
		{name: "succeeded has an outcome", state: RequestStateSucceeded, expected: true},
		{name: "failed has an outcome", state: RequestStateFailed, expected: true},
		{name: "cancelled has an outcome", state: RequestStateCancelled, expected: true},
		{name: "superseded is terminal without an outcome", state: RequestStateSuperseded, expected: false},
		{name: "unknown has no outcome", state: RequestStateUnknown, expected: false},
		{name: "accepted has no outcome", state: RequestStateAccepted, expected: false},
		{name: "processing has no outcome", state: RequestStateProcessing, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.HasBuildOutcome())
		})
	}
}
