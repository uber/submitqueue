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

package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChangeID(t *testing.T) {
	revision := "c3a4d5e6f7890123456789abcdef0123456789ab"

	tests := []struct {
		name    string
		raw     string
		want    ChangeID
		wantErr bool
	}{
		{
			name: "valid",
			raw:  "git://uber/monorepo/main/" + revision,
			want: ChangeID{
				Owner:    "uber",
				Repo:     "monorepo",
				Branch:   "main",
				Revision: revision,
			},
		},
		{
			name: "nested owner",
			raw:  "git://uber/deepteam/monorepo/main/" + revision,
			want: ChangeID{
				Owner:    "uber/deepteam",
				Repo:     "monorepo",
				Branch:   "main",
				Revision: revision,
			},
		},
		{
			name:    "wrong scheme",
			raw:     "github://uber/monorepo/main/" + revision,
			wantErr: true,
		},
		{
			name:    "missing revision",
			raw:     "git://uber/monorepo/main",
			wantErr: true,
		},
		{
			name:    "short revision",
			raw:     "git://uber/monorepo/main/deadbeef",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseChangeID(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.raw, got.String())
		})
	}
}
