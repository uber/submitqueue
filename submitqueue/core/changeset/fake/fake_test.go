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

func TestResolverChanges(t *testing.T) {
	r := New().
		Set("q/batch/1", entity.Change{URIs: []string{"u1"}}).
		Set("q/batch/2", entity.Change{URIs: []string{"u2"}}, entity.Change{URIs: []string{"u3"}})

	got, err := r.ChangesForBatch(context.Background(), entity.Batch{ID: "q/batch/2"})
	require.NoError(t, err)
	assert.Equal(t, []entity.Change{{URIs: []string{"u2"}}, {URIs: []string{"u3"}}}, got)

	unseeded, err := r.ChangesForBatch(context.Background(), entity.Batch{ID: "q/batch/unseeded"})
	require.NoError(t, err)
	assert.Empty(t, unseeded)
}

func TestResolverDetailed(t *testing.T) {
	want := entity.BatchChanges{
		BatchID: "q/batch/1",
		Queue:   "q",
		Changes: []entity.ChangeInfo{{URI: "u1"}},
	}
	r := New().SetDetailed("q/batch/1", want)

	got, err := r.DetailedForBatch(context.Background(), entity.Batch{ID: "q/batch/1", Queue: "q"})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestResolverDetailedUnseeded(t *testing.T) {
	got, err := New().DetailedForBatch(context.Background(), entity.Batch{ID: "q/batch/9", Queue: "q"})
	require.NoError(t, err)
	assert.Equal(t, entity.BatchChanges{BatchID: "q/batch/9", Queue: "q"}, got)
}

func TestResolverFailWith(t *testing.T) {
	sentinel := errors.New("boom")
	r := New().FailWith(sentinel)

	_, err := r.ChangesForBatch(context.Background(), entity.Batch{ID: "q/batch/1"})
	require.ErrorIs(t, err, sentinel)

	_, err = r.DetailedForBatch(context.Background(), entity.Batch{ID: "q/batch/1"})
	require.ErrorIs(t, err, sentinel)
}
