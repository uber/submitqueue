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

package buildrunner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/sqsim"
	"github.com/uber/submitqueue/sqsim/model"
	changesetfake "github.com/uber/submitqueue/submitqueue/core/changeset/fake"
	"github.com/uber/submitqueue/submitqueue/entity"
	buildrunnerext "github.com/uber/submitqueue/submitqueue/extension/buildrunner"
)

const (
	testQueue = "sqsim"
	testURI   = "sqsim://local/build-runner/l1"
)

func TestRunnerImplementsBuildRunner(t *testing.T) {
	var _ buildrunnerext.BuildRunner = (*Runner)(nil)
}

func TestTriggerResponseLossReturnsSameBuildOnRetry(t *testing.T) {
	runner, clock := newRunner(t, sqsim.NewBuildRunnerBehavior().Trigger(
		sqsim.BuildCreatedWithFault(
			sqsim.RetryableErrorAfterSideEffect(),
			sqsim.StatusAt(0, sqsim.BuildAccepted),
			sqsim.StatusAt(time.Second, sqsim.BuildSucceeded),
		),
	))
	head := entity.Batch{ID: "sqsim/batch/1", Queue: testQueue}

	first, err := runner.Trigger(context.Background(), nil, head, nil)
	require.Error(t, err)
	assert.Empty(t, first.ID)

	second, err := runner.Trigger(context.Background(), nil, head, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, second.ID)

	status, _, err := runner.Status(context.Background(), second)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusAccepted, status)
	clock.Advance(time.Second)
	status, _, err = runner.Status(context.Background(), second)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}

func TestTriggerFailureBeforeSideEffectRecovers(t *testing.T) {
	runner, _ := newRunner(t, sqsim.NewBuildRunnerBehavior().Trigger(
		sqsim.BuildTriggerFault(sqsim.RetryableErrorBeforeSideEffect()),
		sqsim.BuildCreated(sqsim.StatusAt(0, sqsim.BuildSucceeded)),
	))
	head := entity.Batch{ID: "sqsim/batch/1", Queue: testQueue}

	_, err := runner.Trigger(context.Background(), nil, head, nil)
	require.Error(t, err)

	buildID, err := runner.Trigger(context.Background(), nil, head, nil)
	require.NoError(t, err)
	status, _, err := runner.Status(context.Background(), buildID)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}

func TestStatusFaultRecovers(t *testing.T) {
	runner, _ := newRunner(t, sqsim.NewBuildRunnerBehavior().
		Trigger(sqsim.BuildCreated(sqsim.StatusAt(0, sqsim.BuildSucceeded))).
		StatusFaultOnCall(1, sqsim.RetryableErrorBeforeSideEffect()))
	head := entity.Batch{ID: "sqsim/batch/1", Queue: testQueue}
	buildID, err := runner.Trigger(context.Background(), nil, head, nil)
	require.NoError(t, err)

	_, _, err = runner.Status(context.Background(), buildID)
	require.Error(t, err)
	status, _, err := runner.Status(context.Background(), buildID)
	require.NoError(t, err)
	assert.Equal(t, entity.BuildStatusSucceeded, status)
}

func newRunner(t *testing.T, buildRunner *sqsim.BuildRunnerBehaviorBuilder) (*Runner, *fakeClock) {
	t.Helper()
	behavior := sqsim.NewBehavior().
		BuildRunner(buildRunner).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())
	scenario, err := sqsim.NewScenario().
		Timeout(time.Minute).
		Land(sqsim.NewLand("l1").Queue(testQueue).Behavior(behavior).Expect(sqsim.RequestLanded)).
		Build()
	require.NoError(t, err)
	profile, err := model.Compile("build-runner", scenario)
	require.NoError(t, err)
	clock := &fakeClock{now: time.Unix(0, 0)}
	runtime, err := model.NewRuntime(profile, clock)
	require.NoError(t, err)
	resolver := changesetfake.New().Set("sqsim/batch/1", change.Change{URIs: []string{testURI}})
	return New(runtime, resolver, testQueue), clock
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
		c.Advance(duration)
		return nil
	}
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
