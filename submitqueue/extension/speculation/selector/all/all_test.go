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

package all

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestSelect(t *testing.T) {
	candidatePath := entity.SpeculationPath{Head: "q/batch/1"}
	selectedPath := entity.SpeculationPath{Base: []string{"q/batch/0"}, Head: "q/batch/1"}
	passedPath := entity.SpeculationPath{Head: "q/batch/2"}

	tests := []struct {
		name  string
		paths []entity.SpeculationPathInfo
		want  []entity.PathDecision
	}{
		{
			name: "mixed status tree promotes only candidates",
			paths: []entity.SpeculationPathInfo{
				{ID: "p/candidate", Path: candidatePath, Status: entity.SpeculationPathStatusCandidate},
				{ID: "p/selected", Path: selectedPath, Status: entity.SpeculationPathStatusSelected},
				{ID: "p/passed", Path: passedPath, Status: entity.SpeculationPathStatusPassed},
			},
			want: []entity.PathDecision{
				{PathID: "p/candidate", Action: entity.SpeculationPathActionPromote},
			},
		},
		{
			name: "no candidates yields no decisions",
			paths: []entity.SpeculationPathInfo{
				{ID: "p/selected", Path: selectedPath, Status: entity.SpeculationPathStatusSelected},
				{ID: "p/passed", Path: passedPath, Status: entity.SpeculationPathStatusPassed},
			},
			want: nil,
		},
	}

	s := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := entity.SpeculationTree{BatchID: "q/batch/1", Paths: tt.paths}
			got, err := s.Select(context.Background(), tree)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
