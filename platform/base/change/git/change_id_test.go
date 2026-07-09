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
	sha := "c3a4d5e6f7890123456789abcdef0123456789ab"

	tests := []struct {
		name    string
		raw     string
		want    ChangeID
		wantErr bool
	}{
		{
			name: "branch ref",
			raw:  "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/" + sha,
			want: ChangeID{
				Scheme:    "git",
				Remote:    "git.example.com",
				Repo:      "uber/monorepo",
				Ref:       "refs/heads/main",
				CommitSHA: sha,
			},
		},
		{
			name: "host with port",
			raw:  "git://git.example.com:9418/uber/monorepo/refs%2Fheads%2Fmain/" + sha,
			want: ChangeID{
				Scheme:    "git",
				Remote:    "git.example.com:9418",
				Repo:      "uber/monorepo",
				Ref:       "refs/heads/main",
				CommitSHA: sha,
			},
		},
		{
			name: "single-segment repo path",
			raw:  "git://git.example.com/monorepo/refs%2Fheads%2Fmain/" + sha,
			want: ChangeID{
				Scheme:    "git",
				Remote:    "git.example.com",
				Repo:      "monorepo",
				Ref:       "refs/heads/main",
				CommitSHA: sha,
			},
		},
		{
			name: "branch ref with slash",
			raw:  "git://git.example.com/uber/monorepo/refs%2Fheads%2Ffeature%2Fx/" + sha,
			want: ChangeID{
				Scheme:    "git",
				Remote:    "git.example.com",
				Repo:      "uber/monorepo",
				Ref:       "refs/heads/feature/x",
				CommitSHA: sha,
			},
		},
		{
			name: "tag ref",
			raw:  "git://git.example.com/uber/monorepo/refs%2Ftags%2Fv1.0/" + sha,
			want: ChangeID{
				Scheme:    "git",
				Remote:    "git.example.com",
				Repo:      "uber/monorepo",
				Ref:       "refs/tags/v1.0",
				CommitSHA: sha,
			},
		},
		{
			name: "nested repo path",
			raw:  "git://git.example.com/uber/deepteam/monorepo/refs%2Fheads%2Fmain/" + sha,
			want: ChangeID{
				Scheme:    "git",
				Remote:    "git.example.com",
				Repo:      "uber/deepteam/monorepo",
				Ref:       "refs/heads/main",
				CommitSHA: sha,
			},
		},
		{
			name:    "wrong scheme",
			raw:     "github://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/" + sha,
			wantErr: true,
		},
		{
			name:    "missing host",
			raw:     "git:///uber/monorepo/refs%2Fheads%2Fmain/" + sha,
			wantErr: true,
		},
		{
			name:    "uppercase host",
			raw:     "git://Git.example.com/uber/monorepo/refs%2Fheads%2Fmain/" + sha,
			wantErr: true,
		},
		{
			name:    "missing commit SHA",
			raw:     "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain",
			wantErr: true,
		},
		{
			name:    "abbreviated SHA",
			raw:     "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/deadbeef",
			wantErr: true,
		},
		{
			name:    "unqualified ref",
			raw:     "git://git.example.com/uber/monorepo/main/" + sha,
			wantErr: true,
		},
		{
			name:    "malformed percent-encoding",
			raw:     "git://git.example.com/uber/monorepo/refs%2/" + sha,
			wantErr: true,
		},
		{
			name:    "empty repo path",
			raw:     "git://git.example.com//refs%2Fheads%2Fmain/" + sha,
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
