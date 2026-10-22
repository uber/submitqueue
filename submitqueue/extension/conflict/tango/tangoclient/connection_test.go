// Copyright (c) 2026 Uber Technologies, Inc.
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

package tangoclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		want     bool
	}{
		{name: "unset", strategy: "unset", want: true},
		{name: "shell", strategy: "shell", want: true},
		{name: "native", strategy: "native", want: true},
		{name: "unknown", strategy: "fast"},
		{name: "empty", strategy: ""},
		{name: "wrong case", strategy: "Native"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ValidStrategy(tt.strategy))
		})
	}
}

func TestConnection_ChangedTargetsClient(t *testing.T) {
	tests := []struct {
		name      string
		strategy  string
		wantError bool
	}{
		{name: "empty strategy means unset"},
		{name: "named strategy", strategy: "native"},
		{name: "unknown strategy fails", strategy: "fast", wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connection, err := Dial("127.0.0.1:0")
			require.NoError(t, err)
			defer func() { require.NoError(t, connection.Close()) }()

			client, err := connection.ChangedTargetsClient("org/repo", tt.strategy)

			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, client)
		})
	}
}
