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

package runner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/sqsim"
)

func TestRunSubmitsObservesAndVerifiesScenario(t *testing.T) {
	scenario, err := sqsim.NewScenario().
		Timeout(time.Minute).
		Land(
			sqsim.NewLand("l1").Queue("sqsim").SubmitAfter(time.Second).Behavior(testBehavior()).Expect(sqsim.RequestError),
			sqsim.NewLand("l2").Queue("sqsim").Behavior(testBehavior()).Expect(sqsim.RequestLanded),
		).
		Build()
	require.NoError(t, err)

	clock := &testClock{now: time.Unix(0, 0)}
	gateway := &testGateway{
		statuses: map[string][]string{
			"sqsim/1": {"accepted", "landed"},
			"sqsim/2": {"accepted", "building", "error"},
		},
	}
	var snapshots []Snapshot
	report, err := Run(context.Background(), Options{
		ScenarioName: "mixed",
		Scenario:     scenario,
		Gateway:      gateway,
		Clock:        clock,
		PollInterval: time.Second,
		Observer: ObserverFunc(func(snapshot Snapshot) {
			snapshots = append(snapshots, snapshot)
		}),
	})
	require.NoError(t, err)
	assert.True(t, report.Passed)
	assert.Equal(t, []string{
		"sqsim://local/mixed/l2",
		"sqsim://local/mixed/l1",
	}, gateway.uris)
	assert.True(t, snapshots[len(snapshots)-1].Done)
	require.Len(t, report.Requests[1].History, 1)
}

func TestRunReportsExpectationMismatch(t *testing.T) {
	scenario, err := sqsim.NewScenario().
		Timeout(time.Minute).
		Land(sqsim.NewLand("l1").Queue("sqsim").Behavior(testBehavior()).Expect(sqsim.RequestLanded)).
		Build()
	require.NoError(t, err)
	gateway := &testGateway{statuses: map[string][]string{"sqsim/1": {"error"}}}

	report, err := Run(context.Background(), Options{
		ScenarioName: "mismatch",
		Scenario:     scenario,
		Gateway:      gateway,
		Clock:        &testClock{now: time.Unix(0, 0)},
		PollInterval: time.Second,
	})
	require.NoError(t, err)
	assert.False(t, report.Passed)
}

func testBehavior() *sqsim.BehaviorBuilder {
	return sqsim.NewBehavior().
		BuildRunner(sqsim.SuccessfulBuildRunner()).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())
}

type testGateway struct {
	mu       sync.Mutex
	uris     []string
	statuses map[string][]string
	polls    map[string]int
}

func (g *testGateway) Land(_ context.Context, _ string, uri string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.uris = append(g.uris, uri)
	return fmt.Sprintf("sqsim/%d", len(g.uris)), nil
}

func (g *testGateway) List(_ context.Context, _ string, _, _ int64, _ string) ([]Summary, string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.polls == nil {
		g.polls = make(map[string]int)
	}
	summaries := make([]Summary, 0, len(g.uris))
	for i := range g.uris {
		sqid := fmt.Sprintf("sqsim/%d", i+1)
		statuses := g.statuses[sqid]
		poll := g.polls[sqid]
		if poll >= len(statuses) {
			poll = len(statuses) - 1
		}
		summaries = append(summaries, Summary{SQID: sqid, Status: statuses[poll]})
		g.polls[sqid]++
	}
	return summaries, "", nil
}

func (g *testGateway) Summary(_ context.Context, sqid string) (Summary, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	statuses := g.statuses[sqid]
	if len(statuses) == 0 {
		return Summary{}, fmt.Errorf("not found")
	}
	return Summary{SQID: sqid, Status: statuses[len(statuses)-1]}, nil
}

func (g *testGateway) History(_ context.Context, _ string) ([]HistoryEvent, error) {
	return []HistoryEvent{{Status: "terminal"}}, nil
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Wait(ctx context.Context, duration time.Duration) error {
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
