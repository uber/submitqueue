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
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ pusher.Pusher = New()
}

func TestPusher_Push_Committed(t *testing.T) {
	p := New()
	changes := []entity.Change{
		{URIs: []string{"github://owner/repo/pull/1/abc"}},
		{URIs: []string{"github://owner/repo/pull/2/def"}},
	}

	res, err := p.Push(context.Background(), changes)
	require.NoError(t, err)
	require.Len(t, res.Outcomes, len(changes))

	seen := map[string]bool{}
	for i, out := range res.Outcomes {
		assert.Equal(t, changes[i], out.Change)
		assert.Equal(t, pusher.OutcomeStatusCommitted, out.Status)
		require.Len(t, out.CommitSHAs, 1)
		assert.False(t, seen[out.CommitSHAs[0]], "commit SHAs must be unique")
		seen[out.CommitSHAs[0]] = true
	}
}

func TestPusher_Push_ConflictMarker(t *testing.T) {
	p := New()
	_, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{"github://owner/repo/pull/1/abc?sq-fake=conflict"}},
	})
	assert.True(t, errors.Is(err, pusher.ErrConflict))
}

func TestPusher_Push_ErrorMarker(t *testing.T) {
	p := New()
	res, err := p.Push(context.Background(), []entity.Change{
		{URIs: []string{"github://owner/repo/pull/1/abc?sq-fake=push-error"}},
	})
	require.Error(t, err)
	// Atomicity: on error no outcomes are reported.
	assert.Empty(t, res.Outcomes)
}
