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
)

func TestSpeculationPath_Equal(t *testing.T) {
	tests := []struct {
		name  string
		path  SpeculationPath
		other SpeculationPath
		equal bool
	}{
		{
			name:  "equal paths",
			path:  SpeculationPath{Base: []string{"q/batch/1", "q/batch/2"}, Head: "q/batch/3"},
			other: SpeculationPath{Base: []string{"q/batch/1", "q/batch/2"}, Head: "q/batch/3"},
			equal: true,
		},
		{
			name:  "different head",
			path:  SpeculationPath{Base: []string{"q/batch/1"}, Head: "q/batch/2"},
			other: SpeculationPath{Base: []string{"q/batch/1"}, Head: "q/batch/3"},
			equal: false,
		},
		{
			name:  "different base order",
			path:  SpeculationPath{Base: []string{"q/batch/1", "q/batch/2"}, Head: "q/batch/3"},
			other: SpeculationPath{Base: []string{"q/batch/2", "q/batch/1"}, Head: "q/batch/3"},
			equal: false,
		},
		{
			name:  "different base length",
			path:  SpeculationPath{Base: []string{"q/batch/1"}, Head: "q/batch/3"},
			other: SpeculationPath{Base: []string{"q/batch/1", "q/batch/2"}, Head: "q/batch/3"},
			equal: false,
		},
		{
			name:  "both empty",
			path:  SpeculationPath{},
			other: SpeculationPath{},
			equal: true,
		},
		{
			name:  "nil base equals empty base",
			path:  SpeculationPath{Base: nil, Head: "q/batch/1"},
			other: SpeculationPath{Base: []string{}, Head: "q/batch/1"},
			equal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.equal, tt.path.Equal(tt.other))
			// Equal must be symmetric.
			assert.Equal(t, tt.equal, tt.other.Equal(tt.path))
		})
	}
}

func TestSpeculationTree_PathIndex(t *testing.T) {
	tree := SpeculationTree{
		BatchID: "q/batch/3",
		Paths: []SpeculationPathInfo{
			{ID: "q/batch/3/path/0", Path: SpeculationPath{Base: []string{"q/batch/1", "q/batch/2"}, Head: "q/batch/3"}},
			{ID: "q/batch/3/path/1", Path: SpeculationPath{Base: []string{"q/batch/1"}, Head: "q/batch/3"}},
			{ID: "q/batch/3/path/2", Path: SpeculationPath{Head: "q/batch/3"}},
		},
	}

	tests := []struct {
		name string
		id   string
		want int
	}{
		{
			name: "first path found",
			id:   "q/batch/3/path/0",
			want: 0,
		},
		{
			name: "last path found",
			id:   "q/batch/3/path/2",
			want: 2,
		},
		{
			name: "unknown id not found",
			id:   "q/batch/3/path/9",
			want: -1,
		},
		{
			name: "empty id not found",
			id:   "",
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tree.PathIndex(tt.id))
		})
	}
}

func TestSpeculationTree_PathIndex_EmptyTree(t *testing.T) {
	assert.Equal(t, -1, SpeculationTree{}.PathIndex("q/batch/1/path/0"))
}
