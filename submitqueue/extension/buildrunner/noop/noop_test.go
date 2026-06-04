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

package noop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

func TestNew_ImplementsInterface(t *testing.T) {
	var _ buildrunner.BuildRunner = New()
}

func TestRunner_Trigger(t *testing.T) {
	r := New()
	ctx := context.Background()

	id1, err := r.Trigger(ctx,
		[]entity.Change{{URIs: []string{"github://owner/repo/pull/1"}}},
		[]entity.Change{{URIs: []string{"github://owner/repo/pull/2"}}},
		entity.BuildMetadata{"requester": "alice"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, id1.ID)

	// IDs are unique across calls, even with empty inputs.
	id2, err := r.Trigger(ctx, nil, nil, nil)
	require.NoError(t, err)
	assert.NotEqual(t, id1, id2)
}

func TestRunner_Status(t *testing.T) {
	r := New()

	status, meta, err := r.Status(context.Background(), entity.BuildID{ID: "any-id"})
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
	assert.Empty(t, meta)
}

func TestRunner_Cancel(t *testing.T) {
	r := New()
	assert.NoError(t, r.Cancel(context.Background(), entity.BuildID{ID: "any-id"}))
}
