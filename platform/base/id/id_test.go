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
		{name: "legacy counters sort numerically", a: "queue/9", b: "queue/10", want: -1},
		{name: "newer legacy counter", a: "queue/10", b: "queue/9", want: 1},
		{name: "decimal is newer despite smaller counter", a: "1", b: "queue/42", want: 1},
		{name: "legacy is older despite larger counter", a: "queue/42", b: "1", want: -1},
		{name: "same counter across formats is not equal", a: "queue/42", b: "42", want: -1},
		{name: "same legacy ID", a: "queue/42", b: "queue/42"},
		{name: "queue containing slashes", a: "1", b: "request/monorepo/main/42", want: 1},
		{name: "legacy queue containing slashes", a: "request/monorepo/main/9", b: "request/monorepo/main/10", want: -1},
		{name: "maximum legacy counter is older than first decimal", a: "request/monorepo/main/9223372036854775807", b: "1", want: -1},
		{name: "large legacy counters retain precision", a: "queue/9223372036854775806", b: "queue/9223372036854775807", want: -1},
		{name: "invalid legacy suffix", a: "queue/bad", b: "queue/42", wantErr: true},
		{name: "zero legacy suffix", a: "1", b: "queue/0", wantErr: true},
		{name: "noncanonical legacy suffix", a: "queue/01", b: "queue/42", wantErr: true},
		{name: "empty first legacy prefix", a: "/9", b: "10", wantErr: true},
		{name: "empty second legacy prefix", a: "9", b: "/10", wantErr: true},
		{name: "empty legacy suffix", a: "queue/", b: "10", wantErr: true},
		{name: "overflow legacy suffix", a: "queue/9223372036854775808", b: "10", wantErr: true},
		{name: "zero decimal against legacy", a: "0", b: "queue/42", wantErr: true},
		{name: "noncanonical decimal against legacy", a: "queue/42", b: "01", wantErr: true},
		{name: "negative decimal against legacy", a: "-1", b: "queue/42", wantErr: true},
		{name: "empty decimal against legacy", a: "queue/42", wantErr: true},
		{name: "overflow decimal against legacy", a: "9223372036854775808", b: "queue/42", wantErr: true},
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
