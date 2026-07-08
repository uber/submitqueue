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
	"github.com/stretchr/testify/require"
)

func TestCompareRequestID(t *testing.T) {
	const queue = "monorepo/main"

	tests := []struct {
		name    string
		a       string
		b       string
		want    int
		wantErr bool
	}{
		{
			name: "older vs newer",
			a:    "request/monorepo/main/7",
			b:    "request/monorepo/main/10",
			want: -1,
		},
		{
			name: "newer vs older",
			a:    "request/monorepo/main/10",
			b:    "request/monorepo/main/7",
			want: 1,
		},
		{
			name: "equal",
			a:    "request/monorepo/main/42",
			b:    "request/monorepo/main/42",
			want: 0,
		},
		{
			name: "numeric not lexicographic",
			a:    "request/monorepo/main/9",
			b:    "request/monorepo/main/10",
			want: -1,
		},
		{
			name:    "wrong queue prefix",
			a:       "request/other/1",
			b:       "request/monorepo/main/2",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CompareRequestID(queue, tt.a, tt.b)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
