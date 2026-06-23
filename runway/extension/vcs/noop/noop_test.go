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
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
)

func testRequest() *runwaymq.MergeRequest {
	return &runwaymq.MergeRequest{
		Id:        "queue-a/42",
		QueueName: "queue-a",
		Steps: []*runwaymq.MergeStep{
			{
				StepId:   "queue-a/1",
				Changes:  []*changepb.Change{{Uris: []string{"github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"}}},
				Strategy: strategypb.Strategy_REBASE,
			},
			{
				StepId:   "queue-a/2",
				Changes:  []*changepb.Change{{Uris: []string{"github://uber/repo/pull/2/89abcdef0123456789abcdef0123456789abcdef"}}},
				Strategy: strategypb.Strategy_MERGE,
			},
		},
	}
}

func TestCheckMergeability(t *testing.T) {
	v := New()
	req := testRequest()

	res, err := v.CheckMergeability(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, req.GetId(), res.GetId())
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 2)
	assert.Equal(t, "queue-a/1", res.GetSteps()[0].GetStepId())
	assert.Empty(t, res.GetSteps()[0].GetOutputs())
	assert.Equal(t, "queue-a/2", res.GetSteps()[1].GetStepId())
	assert.Empty(t, res.GetSteps()[1].GetOutputs())
}

func TestMerge(t *testing.T) {
	v := New()
	req := testRequest()

	res, err := v.Merge(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, req.GetId(), res.GetId())
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, res.GetOutcome())
	require.Len(t, res.GetSteps(), 2)
	assert.Equal(t, "queue-a/1", res.GetSteps()[0].GetStepId())
	require.Len(t, res.GetSteps()[0].GetOutputs(), 1)
	assert.NotEmpty(t, res.GetSteps()[0].GetOutputs()[0].GetId())
	assert.Equal(t, "queue-a/2", res.GetSteps()[1].GetStepId())
	require.Len(t, res.GetSteps()[1].GetOutputs(), 1)
	assert.NotEmpty(t, res.GetSteps()[1].GetOutputs()[0].GetId())
}

func TestMerge_UniqueOutputIDs(t *testing.T) {
	v := New()
	req := testRequest()

	res1, err := v.Merge(context.Background(), req)
	require.NoError(t, err)
	res2, err := v.Merge(context.Background(), req)
	require.NoError(t, err)

	assert.NotEqual(t, res1.GetSteps()[0].GetOutputs()[0].GetId(), res2.GetSteps()[0].GetOutputs()[0].GetId())
}
