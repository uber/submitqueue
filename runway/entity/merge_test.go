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
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
)

func TestMergeRequestRoundTrip(t *testing.T) {
	req := MergeRequest{
		ID:        "queue-a/42",
		QueueName: "queue-a",
		Steps: []MergeStep{
			{
				StepID:   "queue-a/1",
				Changes:  []change.Change{{URIs: []string{"github://uber/repo/pull/1/" + "0123456789abcdef0123456789abcdef01234567"}}},
				Strategy: mergestrategy.MergeStrategyRebase,
			},
			{
				StepID:   "queue-a/2",
				Changes:  []change.Change{{URIs: []string{"github://uber/repo/pull/2/" + "89abcdef0123456789abcdef0123456789abcdef"}}},
				Strategy: mergestrategy.MergeStrategyMerge,
			},
		},
	}

	data, err := req.ToBytes()
	require.NoError(t, err)

	got, err := MergeRequestFromBytes(data)
	require.NoError(t, err)
	assert.Equal(t, req, got)
}

func TestMergeResultRoundTrip(t *testing.T) {
	// A committing merge reports the revisions each step produced on the target;
	// a dry-run check leaves OutputIDs empty and reports a per-step reason on
	// failure. Both shapes share the one MergeResult contract.
	t.Run("merged with produced revisions", func(t *testing.T) {
		res := MergeResult{
			ID:      "queue-a/42",
			Success: true,
			Steps: []StepResult{
				{StepID: "queue-a/1", OutputIDs: []string{"0123456789abcdef0123456789abcdef01234567"}},
			},
		}

		data, err := res.ToBytes()
		require.NoError(t, err)

		got, err := MergeResultFromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, res, got)
	})

	t.Run("failed with per-step reason", func(t *testing.T) {
		res := MergeResult{
			ID:      "queue-a/42",
			Success: false,
			Reason:  "conflict in foo.go",
			Steps:   []StepResult{{StepID: "queue-a/2", Reason: "conflict in foo.go"}},
		}

		data, err := res.ToBytes()
		require.NoError(t, err)

		got, err := MergeResultFromBytes(data)
		require.NoError(t, err)
		assert.Equal(t, res, got)
	})
}
