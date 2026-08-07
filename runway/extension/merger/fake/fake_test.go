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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/runway/extension/merger"
)

const baseURI = "github://github.example.com/uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"

// requestWith builds a two-step request whose second step carries the given
// URIs, so tests can prove the token is found on any step, not just the first.
func requestWith(uris ...string) *runwaymq.MergeRequest {
	return &runwaymq.MergeRequest{
		Id:        "queue-a/42",
		QueueName: "queue-a",
		Steps: []*runwaymq.MergeStep{
			{
				StepId:   "queue-a/1",
				Change:   &changepb.Change{Uris: []string{baseURI}},
				Strategy: strategypb.Strategy_REBASE,
			},
			{
				StepId:   "queue-a/2",
				Change:   &changepb.Change{Uris: uris},
				Strategy: strategypb.Strategy_REBASE,
			},
		},
	}
}

func TestUnmarkedRequestSucceeds(t *testing.T) {
	req := requestWith(baseURI)

	t.Run("check mergeability reports no outputs", func(t *testing.T) {
		res, err := New().CheckMergeability(context.Background(), req)
		require.NoError(t, err)

		assert.Equal(t, req.GetId(), res.GetId())
		assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
		require.Len(t, res.GetSteps(), 2)
		assert.Equal(t, "queue-a/1", res.GetSteps()[0].GetStepId())
		assert.Empty(t, res.GetSteps()[0].GetOutputs())
		assert.Equal(t, "queue-a/2", res.GetSteps()[1].GetStepId())
		assert.Empty(t, res.GetSteps()[1].GetOutputs())
	})

	t.Run("merge reports one output per step", func(t *testing.T) {
		res, err := New().Merge(context.Background(), req)
		require.NoError(t, err)

		assert.Equal(t, req.GetId(), res.GetId())
		assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
		require.Len(t, res.GetSteps(), 2)
		require.Len(t, res.GetSteps()[0].GetOutputs(), 1)
		require.Len(t, res.GetSteps()[1].GetOutputs(), 1)
		assert.NotEqual(t,
			res.GetSteps()[0].GetOutputs()[0].GetId(),
			res.GetSteps()[1].GetOutputs()[0].GetId(),
			"each step should produce a distinct revision id")
	})
}

func TestMarkedRequestFails(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  error // nil means "an error that is neither terminal sentinel"
	}{
		{name: "conflict", token: tokenConflict, want: merger.ErrConflict},
		{name: "invalid", token: tokenInvalid, want: merger.ErrInvalidRequest},
		{name: "plain error", token: tokenError, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := requestWith(baseURI + "?sq-fake=" + tt.token)

			for _, call := range []struct {
				name string
				fn   func(*runwaymq.MergeRequest) (*runwaymq.MergeResult, error)
			}{
				{"CheckMergeability", func(r *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
					return New().CheckMergeability(context.Background(), r)
				}},
				{"Merge", func(r *runwaymq.MergeRequest) (*runwaymq.MergeResult, error) {
					return New().Merge(context.Background(), r)
				}},
			} {
				t.Run(call.name, func(t *testing.T) {
					res, err := call.fn(req)
					require.Error(t, err)
					assert.Nil(t, res)

					if tt.want != nil {
						assert.ErrorIs(t, err, tt.want)
						assert.True(t, merger.IsTerminal(err), "sentinel failures must be terminal")
						return
					}
					assert.False(t, merger.IsTerminal(err),
						"an unmarked failure must not look terminal, so it dead-letters")
				})
			}
		})
	}
}

func TestUnrecognizedTokenSucceeds(t *testing.T) {
	req := requestWith(baseURI + "?sq-fake=some-other-fakes-token")

	res, err := New().Merge(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
}

func TestFirstRecognizedTokenWins(t *testing.T) {
	req := &runwaymq.MergeRequest{
		Id:        "queue-a/42",
		QueueName: "queue-a",
		Steps: []*runwaymq.MergeStep{
			{StepId: "queue-a/1", Change: &changepb.Change{Uris: []string{baseURI + "?sq-fake=" + tokenConflict}}},
			{StepId: "queue-a/2", Change: &changepb.Change{Uris: []string{baseURI + "?sq-fake=" + tokenInvalid}}},
		},
	}

	_, err := New().Merge(context.Background(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, merger.ErrConflict)
	assert.NotErrorIs(t, err, merger.ErrInvalidRequest)
}
