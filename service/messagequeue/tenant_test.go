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

package messagequeue

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRequiredTenants(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    []string
		wantErr bool
	}{
		{
			name:  "comma-separated tenants",
			value: " monorepo/main, monorepo/release ",
			want:  []string{"monorepo/main", "monorepo/release"},
		},
		{
			name:  "duplicates removed in first-seen order",
			value: "monorepo/main,monorepo/release,monorepo/main",
			want:  []string{"monorepo/main", "monorepo/release"},
		},
		{name: "empty", wantErr: true},
		{name: "whitespace and commas", value: " , , ", wantErr: true},
		{name: "tenant exceeds byte limit", value: strings.Repeat("x", maxTenantLength+1), wantErr: true},
		{name: "tenant contains non-ASCII characters", value: "monorepo/café", wantErr: true},
		{name: "tenant contains NUL", value: "monorepo/\x00main", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRequiredTenants(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateTenantSetsEqual(t *testing.T) {
	tests := []struct {
		name    string
		first   []string
		second  []string
		wantErr bool
	}{
		{name: "same tenants", first: []string{"a", "b"}, second: []string{"b", "a"}},
		{name: "duplicates ignored", first: []string{"a", "a"}, second: []string{"a"}},
		{name: "first has extra tenant", first: []string{"a", "b"}, second: []string{"a"}, wantErr: true},
		{name: "second has extra tenant", first: []string{"a"}, second: []string{"a", "b"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantSetsEqual("first", tt.first, "second", tt.second)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateTenantSubset(t *testing.T) {
	tests := []struct {
		name     string
		superset []string
		subset   []string
		wantErr  bool
	}{
		{name: "subset", superset: []string{"a", "b"}, subset: []string{"b"}},
		{name: "equal", superset: []string{"a"}, subset: []string{"a"}},
		{name: "duplicates ignored", superset: []string{"a"}, subset: []string{"a", "a"}},
		{name: "unknown tenant", superset: []string{"a"}, subset: []string{"b"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTenantSubset("superset", tt.superset, "subset", tt.subset)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
