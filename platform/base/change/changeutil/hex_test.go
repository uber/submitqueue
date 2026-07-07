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

package changeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFullHex(t *testing.T) {
	tests := []struct {
		name   string
		s      string
		length int
		want   bool
	}{
		{name: "valid 40-char SHA", s: "abcdef0123456789abcdef0123456789abcdef01", length: 40, want: true},
		{name: "valid custom length", s: "abc123", length: 6, want: true},
		{name: "too short", s: "abc", length: 40, want: false},
		{name: "too long", s: "abcdef0123456789abcdef0123456789abcdef0101", length: 40, want: false},
		{name: "uppercase rejected", s: "ABCDEF0123456789ABCDEF0123456789ABCDEF01", length: 40, want: false},
		{name: "non-hex rejected", s: "zzzzzz0123456789abcdef0123456789abcdef01", length: 40, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsFullHex(tt.s, tt.length))
		})
	}
}
