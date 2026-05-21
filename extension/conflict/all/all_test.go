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
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/conflict"
)

func TestAnalyze(t *testing.T) {
	batch := entity.Batch{ID: "queueA/batch/10"}

	tests := []struct {
		name     string
		inFlight []entity.Batch
		want     []conflict.Conflict
	}{
		{
			name:     "no in-flight batches yields no conflicts",
			inFlight: nil,
			want:     nil,
		},
		{
			name:     "empty in-flight slice yields no conflicts",
			inFlight: []entity.Batch{},
			want:     nil,
		},
		{
			name: "every in-flight batch is reported in input order",
			inFlight: []entity.Batch{
				{ID: "queueA/batch/1"},
				{ID: "queueA/batch/2"},
				{ID: "queueA/batch/3"},
			},
			want: []conflict.Conflict{
				{BatchID: "queueA/batch/1", Type: conflict.ConflictTypeConservative},
				{BatchID: "queueA/batch/2", Type: conflict.ConflictTypeConservative},
				{BatchID: "queueA/batch/3", Type: conflict.ConflictTypeConservative},
			},
		},
	}

	a := New()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := a.Analyze(context.Background(), batch, tt.inFlight)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
