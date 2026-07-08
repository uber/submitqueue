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

package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestScore_ReturnsSeededTree(t *testing.T) {
	tree := entity.SpeculationTree{
		BatchID: "q/batch/2",
		Paths: []entity.SpeculationPathInfo{
			{Path: entity.SpeculationPath{Head: "q/batch/2"}, Score: 0.9},
		},
	}
	got, err := New().Set("q/batch/2", tree).Score(context.Background(), entity.Batch{ID: "q/batch/2"})
	require.NoError(t, err)
	assert.Equal(t, tree, got)
}

func TestScore_UnseededReturnsEmptyTreeWithID(t *testing.T) {
	got, err := New().Score(context.Background(), entity.Batch{ID: "q/batch/9"})
	require.NoError(t, err)
	assert.Equal(t, entity.SpeculationTree{BatchID: "q/batch/9"}, got)
}

func TestScore_FailWith(t *testing.T) {
	sentinel := errors.New("boom")
	_, err := New().FailWith(sentinel).Score(context.Background(), entity.Batch{ID: "q/batch/1"})
	require.ErrorIs(t, err, sentinel)
}
