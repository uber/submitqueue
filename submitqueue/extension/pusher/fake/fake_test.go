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
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ pusher.Pusher = New(changesetfake.New())
}

func TestPusher_Push_Committed(t *testing.T) {
	changes := []entity.Change{
		{URIs: []string{"github://owner/repo/pull/1/abc"}},
		{URIs: []string{"github://owner/repo/pull/2/def"}},
	}
	p := New(changesetfake.New().Set("b", changes...))

	res, err := p.Push(context.Background(), []entity.Batch{{ID: "b"}})
	require.NoError(t, err)
	require.Len(t, res.Batches, 1)
	require.Len(t, res.Batches[0].Outcomes, len(changes))

	seen := map[string]bool{}
	for i, out := range res.Batches[0].Outcomes {
		assert.Equal(t, changes[i], out.Change)
		assert.Equal(t, entity.OutcomeStatusCommitted, out.Status)
		require.Len(t, out.CommitSHAs, 1)
		assert.False(t, seen[out.CommitSHAs[0]], "commit SHAs must be unique")
		seen[out.CommitSHAs[0]] = true
	}
}

func TestPusher_Push_ConflictMarker(t *testing.T) {
	p := New(changesetfake.New().Set("b", entity.Change{URIs: []string{"github://owner/repo/pull/1/abc?sq-fake=conflict"}}))
	_, err := p.Push(context.Background(), []entity.Batch{{ID: "b"}})
	assert.True(t, errors.Is(err, pusher.ErrConflict))
}

func TestPusher_Push_ErrorMarker(t *testing.T) {
	p := New(changesetfake.New().Set("b", entity.Change{URIs: []string{"github://owner/repo/pull/1/abc?sq-fake=push-error"}}))
	res, err := p.Push(context.Background(), []entity.Batch{{ID: "b"}})
	require.Error(t, err)
	// Atomicity: on error no outcomes are reported.
	assert.Empty(t, res.Batches)
}
