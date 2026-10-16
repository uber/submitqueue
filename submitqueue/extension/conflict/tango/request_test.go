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

package tango

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
)

func TestChangedTargetsRequestFor(t *testing.T) {
	base := Base{Remote: "git://host/repo", Revision: "9f2c1d0e3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d"}

	tests := []struct {
		name    string
		changes []change.Change
		want    []string
	}{
		{name: "no changes", want: nil},
		{name: "one change", changes: []change.Change{{URIs: []string{"a"}}}, want: []string{"a"}},
		{
			name:    "URIs are flattened in batch order",
			changes: []change.Change{{URIs: []string{"a", "b"}}, {URIs: []string{"c"}}, {URIs: []string{"d", "e"}}},
			want:    []string{"a", "b", "c", "d", "e"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := changedTargetsRequestFor(base, mergestrategy.MergeStrategySquashRebase, tt.changes)

			assert.Equal(t, ChangedTargetsRequest{
				Remote:       base.Remote,
				BaseRevision: base.Revision,
				ChangeURIs:   tt.want,
				Strategy:     mergestrategy.MergeStrategySquashRebase,
			}, got)
		})
	}
}
