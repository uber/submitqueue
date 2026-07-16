// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

func TestBuildStatusIsTerminal(t *testing.T) {
	tests := []struct {
		status BuildStatus
		want   bool
	}{
		{status: BuildAccepted, want: false},
		{status: BuildRunning, want: false},
		{status: BuildSucceeded, want: true},
		{status: BuildFailed, want: true},
		{status: BuildCancelled, want: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			assert.Equal(t, tt.want, tt.status.IsTerminal())
		})
	}
}
