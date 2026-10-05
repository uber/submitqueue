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

func TestIsRequestStateTerminal(t *testing.T) {
	tests := []struct {
		state    RequestState
		terminal bool
	}{
		{RequestStateUnknown, false},
		{RequestStateStarted, false},
		{RequestStateValidated, false},
		{RequestStateProcessing, false},
		{RequestStateCancelling, false}, // intent only — not terminal
		{RequestStateLanded, true},
		{RequestStateError, true},
		{RequestStateCancelled, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			assert.Equal(t, tt.terminal, IsRequestStateTerminal(tt.state))
		})
	}
}

func TestIsRequestStateHalted(t *testing.T) {
	tests := []struct {
		state  RequestState
		halted bool
	}{
		{RequestStateUnknown, false},
		{RequestStateStarted, false},
		{RequestStateValidated, false},
		{RequestStateProcessing, false},
		{RequestStateCancelling, true}, // intent halts forward progress
		{RequestStateLanded, true},
		{RequestStateError, true},
		{RequestStateCancelled, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			assert.Equal(t, tt.halted, IsRequestStateHalted(tt.state))
		})
	}
}
