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

package github

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Sample 40-char lowercase hex SHAs used across the test cases.
const (
	sha1Full = "1111111111111111111111111111111111111111"
	sha2Full = "2222222222222222222222222222222222222222"
	shaAFull = "abcdef0123456789abcdef0123456789abcdef01"
	shaBFull = "0123456789abcdef0123456789abcdef01234567"
	shaCFull = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
)

func TestParseChangeID(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    ChangeID
		wantErr bool
	}{
		{
			name: "valid github scheme",
			raw:  "github://uber/submitqueue/pull/123/" + shaAFull,
			want: ChangeID{
				Scheme:        "github",
				Org:           "uber",
				Repo:          "submitqueue",
				PRNumber:      123,
				HeadCommitSHA: shaAFull,
			},
		},
		{
			name: "valid ghe scheme",
			raw:  "ghe://uber/monorepo/pull/456/" + shaCFull,
			want: ChangeID{
				Scheme:        "ghe",
				Org:           "uber",
				Repo:          "monorepo",
				PRNumber:      456,
				HeadCommitSHA: shaCFull,
			},
		},
		{
			name: "valid ghes scheme",
			raw:  "ghes://org/repo/pull/1/" + sha1Full,
			want: ChangeID{
				Scheme:        "ghes",
				Org:           "org",
				Repo:          "repo",
				PRNumber:      1,
				HeadCommitSHA: sha1Full,
			},
		},
		{
			name: "nested org path",
			raw:  "github://uber/frontend/webapp/pull/42/" + shaAFull,
			want: ChangeID{
				Scheme:        "github",
				Org:           "uber/frontend",
				Repo:          "webapp",
				PRNumber:      42,
				HeadCommitSHA: shaAFull,
			},
		},
		{
			name:    "missing pull segment",
			raw:     "github://uber/submitqueue/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "wrong literal segment (issues instead of pull)",
			raw:     "github://uber/submitqueue/issues/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "missing separator",
			raw:     "github/uber/submitqueue/pull/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "empty scheme",
			raw:     "://uber/submitqueue/pull/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "too few segments",
			raw:     "github://uber/pull/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "only one segment",
			raw:     "github://" + shaAFull,
			wantErr: true,
		},
		{
			name:    "empty owner",
			raw:     "github:///submitqueue/pull/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "empty repo",
			raw:     "github://uber//pull/123/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "non-numeric PR number",
			raw:     "github://uber/submitqueue/pull/abc/" + shaAFull,
			wantErr: true,
		},
		{
			name:    "empty SHA",
			raw:     "github://uber/submitqueue/pull/123/",
			wantErr: true,
		},
		{
			name:    "abbreviated SHA",
			raw:     "github://uber/submitqueue/pull/123/abc123def",
			wantErr: true,
		},
		{
			name:    "uppercase SHA",
			raw:     "github://uber/submitqueue/pull/123/ABCDEF0123456789ABCDEF0123456789ABCDEF01",
			wantErr: true,
		},
		{
			name:    "non-hex SHA",
			raw:     "github://uber/submitqueue/pull/123/zzzzzz0123456789abcdef0123456789abcdef01",
			wantErr: true,
		},
		{
			name:    "SHA too long",
			raw:     "github://uber/submitqueue/pull/123/" + shaAFull + "ab",
			wantErr: true,
		},
		{
			name:    "empty string",
			raw:     "",
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
		})
	}
}

func TestChangeID_String(t *testing.T) {
	tests := []struct {
		name string
		id   ChangeID
		want string
	}{
		{
			name: "github",
			id: ChangeID{
				Scheme:        "github",
				Org:           "uber",
				Repo:          "submitqueue",
				PRNumber:      123,
				HeadCommitSHA: shaAFull,
			},
			want: "github://uber/submitqueue/pull/123/" + shaAFull,
		},
		{
			name: "ghe",
			id: ChangeID{
				Scheme:        "ghe",
				Org:           "corp",
				Repo:          "app",
				PRNumber:      99,
				HeadCommitSHA: shaCFull,
			},
			want: "ghe://corp/app/pull/99/" + shaCFull,
		},
		{
			name: "ghes",
			id: ChangeID{
				Scheme:        "ghes",
				Org:           "org",
				Repo:          "repo",
				PRNumber:      1,
				HeadCommitSHA: sha1Full,
			},
			want: "ghes://org/repo/pull/1/" + sha1Full,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.id.String())
		})
	}
}

func TestChangeID_OwnerRepo(t *testing.T) {
	id := ChangeID{
		Scheme:        "github",
		Org:           "uber",
		Repo:          "submitqueue",
		PRNumber:      1,
		HeadCommitSHA: shaAFull,
	}
	assert.Equal(t, "uber/submitqueue", id.OwnerRepo())
}

func TestParseChangeID_RoundTrip(t *testing.T) {
	originals := []string{
		"github://uber/submitqueue/pull/123/" + shaAFull,
		"ghe://corp/monorepo/pull/99/" + shaCFull,
		"ghes://org/repo/pull/1/" + sha2Full,
		"github://uber/frontend/webapp/pull/42/" + shaBFull,
	}

	for _, raw := range originals {
		t.Run(raw, func(t *testing.T) {
			parsed, err := ParseChangeID(raw)
			require.NoError(t, err)
			assert.Equal(t, raw, parsed.String())
		})
	}
}
