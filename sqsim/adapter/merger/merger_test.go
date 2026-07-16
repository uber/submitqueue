// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merger

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	mergerext "github.com/uber/submitqueue/runway/extension/merger"
	"github.com/uber/submitqueue/sqsim"
	"github.com/uber/submitqueue/sqsim/model"
)

const testQueue = "sqsim"

func TestMergerImplementsInterface(t *testing.T) {
	var _ mergerext.Merger = (*Merger)(nil)
}

func TestCheckMergeabilityFailureRecovers(t *testing.T) {
	adapter := newMerger(t,
		sqsim.NewMergeConflictCheckBehavior().Invoke(
			sqsim.MergeConflictCheckFault(sqsim.RetryableErrorBeforeSideEffect()),
			sqsim.MergeConflictCheckSucceeded(),
		),
		sqsim.SuccessfulMerge(),
	)
	req := mergeRequest("check/1")

	_, err := adapter.CheckMergeability(context.Background(), req)
	require.Error(t, err)
	result, err := adapter.CheckMergeability(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, req.GetId(), result.GetId())
}

func TestCheckMergeabilityConflictIsRetained(t *testing.T) {
	adapter := newMerger(t, sqsim.ConflictingMergeConflictCheck(), sqsim.SuccessfulMerge())
	req := mergeRequest("check/1")

	_, err := adapter.CheckMergeability(context.Background(), req)
	require.ErrorIs(t, err, mergerext.ErrConflict)
	_, err = adapter.CheckMergeability(context.Background(), req)
	require.ErrorIs(t, err, mergerext.ErrConflict)
}

func TestMergeResponseLossReturnsRetainedResult(t *testing.T) {
	adapter := newMerger(t,
		sqsim.SuccessfulMergeConflictCheck(),
		sqsim.NewMergeBehavior().Invoke(
			sqsim.MergeSucceededAfter(0).Fault(sqsim.RetryableErrorAfterSideEffect()),
		),
	)
	req := mergeRequest("merge/1")

	_, err := adapter.Merge(context.Background(), req)
	require.Error(t, err)
	result, err := adapter.Merge(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, result.GetSteps(), 1)
	require.Len(t, result.GetSteps()[0].GetOutputs(), 1)
	assert.Len(t, result.GetSteps()[0].GetOutputs()[0].GetId(), 40)
}

func newMerger(t *testing.T, check *sqsim.MergeConflictCheckBehaviorBuilder, merge *sqsim.MergeBehaviorBuilder) *Merger {
	t.Helper()
	behavior := sqsim.NewBehavior().
		BuildRunner(sqsim.SuccessfulBuildRunner()).
		MergeConflictCheck(check).
		Merge(merge)
	scenario, err := sqsim.NewScenario().
		Timeout(time.Minute).
		Land(sqsim.NewLand("l1").Queue(testQueue).Behavior(behavior).Expect(sqsim.RequestLanded)).
		Build()
	require.NoError(t, err)
	profile, err := model.Compile("merger", scenario)
	require.NoError(t, err)
	runtime, err := model.NewRuntime(profile, &fakeClock{now: time.Unix(0, 0)})
	require.NoError(t, err)
	return New(runtime, testQueue)
}

func mergeRequest(id string) *runwaymq.MergeRequest {
	return &runwaymq.MergeRequest{
		Id:        id,
		QueueName: testQueue,
		Steps: []*runwaymq.MergeStep{{
			StepId: "step/1",
			Changes: []*changepb.Change{{
				Uris: []string{"sqsim://local/merger/l1"},
			}},
		}},
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Wait(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.mu.Lock()
		c.now = c.now.Add(duration)
		c.mu.Unlock()
		return nil
	}
}
