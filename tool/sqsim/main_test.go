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

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantOutput string
	}{
		{name: "list", args: []string{"list"}, wantCode: 0, wantOutput: "happy-path\n"},
		{name: "validate", args: []string{"validate", "happy-path"}, wantCode: 0, wantOutput: "happy-path is valid\n"},
		{name: "unknown scenario", args: []string{"validate", "missing"}, wantCode: 1},
		{name: "unknown command", args: []string{"missing"}, wantCode: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			assert.Equal(t, tt.wantCode, code)
			if tt.wantOutput != "" {
				assert.Equal(t, tt.wantOutput, stdout.String())
			}
		})
	}
}
