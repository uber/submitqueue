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

package fakemarker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/entity/change"
)

func TestToken(t *testing.T) {
	tests := []struct {
		name string
		uris []string
		want string
	}{
		{
			name: "no uris",
			uris: nil,
			want: "",
		},
		{
			name: "no marker",
			uris: []string{"github://o/r/pull/1/a"},
			want: "",
		},
		{
			name: "marker at end of uri",
			uris: []string{"github://o/r/pull/1/a?sq-fake=build-fail"},
			want: "build-fail",
		},
		{
			name: "marker trimmed at & delimiter",
			uris: []string{"github://o/r/pull/1/a?sq-fake=build-fail&attempt=2"},
			want: "build-fail",
		},
		{
			name: "marker trimmed at # delimiter",
			uris: []string{"github://o/r/pull/1/a?sq-fake=build-fail#frag"},
			want: "build-fail",
		},
		{
			name: "marker before a query param it precedes",
			uris: []string{"github://o/r/pull/1/a?sq-fake=push-error&foo=bar#frag"},
			want: "push-error",
		},
		{
			name: "marker on a later uri",
			uris: []string{"github://o/r/pull/1/a", "github://o/r/pull/2/b?sq-fake=conflict"},
			want: "conflict",
		},
		{
			name: "first marker wins",
			uris: []string{"github://o/r/pull/1/a?sq-fake=first", "github://o/r/pull/2/b?sq-fake=second"},
			want: "first",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Token(tt.uris))
		})
	}
}

func TestTokenInChanges(t *testing.T) {
	tests := []struct {
		name    string
		changes []change.Change
		want    string
	}{
		{
			name:    "no changes",
			changes: nil,
			want:    "",
		},
		{
			name:    "no marker",
			changes: []change.Change{{URIs: []string{"github://o/r/pull/1/a"}}},
			want:    "",
		},
		{
			name:    "marker on first change",
			changes: []change.Change{{URIs: []string{"github://o/r/pull/1/a?sq-fake=build-fail&attempt=2"}}},
			want:    "build-fail",
		},
		{
			name: "marker on later change",
			changes: []change.Change{
				{URIs: []string{"github://o/r/pull/1/a"}},
				{URIs: []string{"github://o/r/pull/2/b?sq-fake=push-error"}},
			},
			want: "push-error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TokenInChanges(tt.changes))
		})
	}
}
