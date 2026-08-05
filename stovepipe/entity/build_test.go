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

func TestBuildStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name     string
		status   BuildStatus
		expected bool
	}{
		{name: "succeeded is terminal", status: BuildStatusSucceeded, expected: true},
		{name: "failed is terminal", status: BuildStatusFailed, expected: true},
		{name: "cancelled is terminal", status: BuildStatusCancelled, expected: true},
		{name: "accepted is not terminal", status: BuildStatusAccepted, expected: false},
		{name: "running is not terminal", status: BuildStatusRunning, expected: false},
		{name: "unknown is not terminal", status: BuildStatusUnknown, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.status.IsTerminal())
		})
	}
}
