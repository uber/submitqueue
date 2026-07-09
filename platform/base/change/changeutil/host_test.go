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

package changeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsLowercaseASCII(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{name: "plain lowercase host", s: "github.example.com", want: true},
		{name: "lowercase host with port", s: "github.example.com:8443", want: true},
		{name: "empty string", s: "", want: true},
		{name: "digits and hyphens", s: "git-01.example-corp.com", want: true},
		{name: "uppercase letter rejected", s: "GitHub.example.com", want: false},
		{name: "all uppercase rejected", s: "GITHUB.EXAMPLE.COM", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsLowercaseASCII(tt.s))
		})
	}
}
