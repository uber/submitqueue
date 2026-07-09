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

package chain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestEnumerate(t *testing.T) {
	tests := []struct {
		name    string
		batchID string
		deps    []entity.Batch
		want    []entity.SpeculationPath
	}{
		{
			name:    "no deps yields one path with an empty base",
			batchID: "q/batch/1",
			deps:    nil,
			want:    []entity.SpeculationPath{{Head: "q/batch/1"}},
		},
		{
			name:    "deps preserve input order in base",
			batchID: "q/batch/4",
			deps: []entity.Batch{
				{ID: "q/batch/3"},
				{ID: "q/batch/1"},
				{ID: "q/batch/2"},
			},
			want: []entity.SpeculationPath{{
				Base: []string{"q/batch/3", "q/batch/1", "q/batch/2"},
				Head: "q/batch/4",
			}},
		},
	}

	e := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := e.Enumerate(context.Background(), entity.Batch{ID: tt.batchID}, tt.deps)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			require.Len(t, got, 1)
			assert.Equal(t, tt.batchID, got[0].Head)
		})
	}
}
