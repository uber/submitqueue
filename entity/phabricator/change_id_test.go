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

package phabricator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseChangeID covers one case per distinct branch in the parser.
// The happy path is also the round-trip subject (see TestParseChangeID_RoundTrip),
// which proves String() inverts ParseChangeID — so there is no separate
// TestChangeID_String.
func TestParseChangeID(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    ChangeID
		wantErr bool
	}{
		{
			name: "valid",
			raw:  "phab://D123/456",
			want: ChangeID{Scheme: "phab", RevisionID: 123, DiffID: 456},
		},
		{name: "missing separator", raw: "phab:D123/456", wantErr: true},
		{name: "wrong scheme", raw: "github://D123/456", wantErr: true},
		{name: "wrong segment count", raw: "phab://D123", wantErr: true},
		{name: "missing revision prefix", raw: "phab://123/456", wantErr: true},
		{name: "revision prefix without digits", raw: "phab://D/456", wantErr: true},
		{name: "non-numeric revision", raw: "phab://Dabc/456", wantErr: true},
		{name: "non-positive revision", raw: "phab://D0/456", wantErr: true},
		{name: "empty diff", raw: "phab://D123/", wantErr: true},
		{name: "non-numeric diff", raw: "phab://D123/abc", wantErr: true},
		{name: "non-positive diff", raw: "phab://D123/0", wantErr: true},
		{name: "leading zero revision", raw: "phab://D01/2", wantErr: true},
		{name: "leading zero diff", raw: "phab://D1/02", wantErr: true},
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

func TestChangeID_Revision(t *testing.T) {
	id := ChangeID{Scheme: "phab", RevisionID: 12345, DiffID: 67890}
	assert.Equal(t, "D12345", id.Revision())
}

func TestParseChangeID_RoundTrip(t *testing.T) {
	raw := "phab://D123/456"
	parsed, err := ParseChangeID(raw)
	require.NoError(t, err)
	assert.Equal(t, raw, parsed.String())
}
