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

package id

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromCounter(t *testing.T) {
	tests := []struct {
		name    string
		value   int64
		want    string
		wantErr bool
	}{
		{name: "first", value: 1, want: "1"},
		{name: "maximum", value: math.MaxInt64, want: "9223372036854775807"},
		{name: "zero", wantErr: true},
		{name: "negative", value: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromCounter(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "valid", id: "42"},
		{name: "maximum", id: "9223372036854775807"},
		{name: "empty", wantErr: true},
		{name: "zero", id: "0", wantErr: true},
		{name: "negative", id: "-1", wantErr: true},
		{name: "leading zero", id: "042", wantErr: true},
		{name: "prefix", id: "request.42", wantErr: true},
		{name: "slash", id: "demo-queue/42", wantErr: true},
		{name: "overflow", id: "9223372036854775808", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.id)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name    string
		a       string
		b       string
		want    int
		wantErr bool
	}{
		{name: "older", a: "9", b: "10", want: -1},
		{name: "equal", a: "42", b: "42"},
		{name: "newer", a: "10", b: "9", want: 1},
		{name: "invalid first", a: "bad", b: "10", wantErr: true},
		{name: "invalid second", a: "9", b: "batch.10", wantErr: true},
		{name: "large counters retain precision", a: "9223372036854775806", b: "9223372036854775807", want: -1},
		{name: "legacy first ID", a: "queue/42", b: "1", wantErr: true},
		{name: "legacy second ID", a: "1", b: "queue/42", wantErr: true},
		{name: "both legacy IDs", a: "queue/9", b: "queue/10", wantErr: true},
		{name: "same legacy ID", a: "queue/42", b: "queue/42", wantErr: true},
		{name: "legacy queue containing slashes", a: "request/monorepo/main/9", b: "10", wantErr: true},
		{name: "empty first ID", b: "1", wantErr: true},
		{name: "zero first ID", a: "0", b: "1", wantErr: true},
		{name: "negative first ID", a: "-1", b: "1", wantErr: true},
		{name: "noncanonical first ID", a: "01", b: "1", wantErr: true},
		{name: "overflow first ID", a: "9223372036854775808", b: "1", wantErr: true},
		{name: "empty second ID", a: "1", wantErr: true},
		{name: "zero second ID", a: "1", b: "0", wantErr: true},
		{name: "negative second ID", a: "1", b: "-1", wantErr: true},
		{name: "noncanonical second ID", a: "1", b: "01", wantErr: true},
		{name: "overflow second ID", a: "1", b: "9223372036854775808", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compare(tt.a, tt.b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
