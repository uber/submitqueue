// Copyright (c) 2026 Uber Technologies, Inc.
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

package issuetrack

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func (f *fixture) source(buildID, requestID string, status entity.BuildStatus) {
	f.builds[buildID] = entity.Build{ID: buildID, RequestID: requestID, Status: status, Version: 1}
	request := entity.Request{ID: requestID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 1}
	if status.IsTerminal() {
		request.TerminalBuildID = buildID
		request.State = entity.RequestStateFailed
	}
	f.requests[requestID] = request
}

func (f *fixture) reportIssue(build, task, unit string) {
	if f.buildIssues[build] == nil {
		f.buildIssues[build] = map[issueKey]entity.BuildIssue{}
	}
	f.buildIssues[build][issueKey{task, unit}] = entity.BuildIssue{TaskID: task, UnitID: unit}
}

func (f *fixture) seedIssue(task, unit string, status entity.IssueStatus) {
	f.states[issueKey{task, unit}] = entity.IssueState{TaskID: task, UnitID: unit, Status: status, FirstSeenBuildID: "older", Version: 7}
}

func (f *fixture) message(build string, final bool) entityqueue.Message {
	f.t.Helper()
	f.sequence++
	payload, err := stovepipemq.Marshal(&stovepipemq.IssueTrack{BuildId: build, QueueName: testQueue, Finalize: final})
	require.NoError(f.t, err)
	msg := entityqueue.NewMessage(fmt.Sprintf("notice/%d", f.sequence), payload, testQueue, nil)
	msg.Tenant = testQueue
	return msg
}

func (f *fixture) deliver(msg entityqueue.Message, hold bool) error {
	f.t.Helper()
	delivery := consumermock.NewMockDelivery(f.mocks)
	delivery.EXPECT().Message().Return(msg)
	if hold {
		delivery.EXPECT().Hold(int64(0))
	}
	return f.controller.Process(context.Background(), delivery)
}

func TestRefreshOpensReportedIssuesAndKeepsOtherState(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusRunning)
	f.seedIssue("old", "", entity.IssueStatusOpen)
	f.reportIssue("build", "task", "unit")
	msg := f.message("build", false)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"old", ""}].Status)
	require.Equal(t, int32(7), f.states[issueKey{"old", ""}].Version)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
	require.Len(t, f.logs, 1)

	f.reportIssue("build", "task", "next")
	require.NoError(t, f.deliver(msg, false))
	require.NotContains(t, f.states, issueKey{"task", "next"})
	require.NoError(t, f.deliver(f.message("build", false), false))
	require.Contains(t, f.states, issueKey{"task", "next"})
	require.Len(t, f.logs, 2)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
}

func TestFinalizationOpensKeepsAndClosesIssues(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	f.seedIssue("a", "", entity.IssueStatusOpen)
	f.seedIssue("b", "unit", entity.IssueStatusOpen)
	f.seedIssue("closed", "", entity.IssueStatusClosed)
	f.reportIssue("build", "b", "unit")
	f.reportIssue("build", "c", "unit")
	require.NoError(t, f.deliver(f.message("build", true), false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"a", ""}].Status)
	require.Equal(t, int32(8), f.states[issueKey{"a", ""}].Version)
	require.Equal(t, "older", f.states[issueKey{"b", "unit"}].FirstSeenBuildID)
	require.Equal(t, int32(7), f.states[issueKey{"b", "unit"}].Version)
	require.Equal(t, "build", f.states[issueKey{"c", "unit"}].FirstSeenBuildID)
	require.Equal(t, int32(7), f.states[issueKey{"closed", ""}].Version)
	require.Len(t, f.logs, 2)
	require.Contains(t, f.logs, logKey{issueKey{"a", ""}, "build", entity.IssueEventResolved})
	require.Contains(t, f.logs, logKey{issueKey{"c", "unit"}, "build", entity.IssueEventOpened})
}

func TestEmptyFinalBuildClosesIssuesButRetainsTheirRows(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusSucceeded)
	f.seedIssue("task", "", entity.IssueStatusOpen)
	require.NoError(t, f.deliver(f.message("build", true), false))
	require.Len(t, f.states, 1)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", ""}].Status)
	require.Len(t, f.logs, 1)
}

func TestReportedIssueReopensTheSameRecord(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusRunning)
	f.seedIssue("task", "unit", entity.IssueStatusClosed)
	f.reportIssue("build", "task", "unit")
	require.NoError(t, f.deliver(f.message("build", false), false))
	issue := f.states[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusOpen, issue.Status)
	require.Equal(t, int32(8), issue.Version)
	require.Equal(t, "build", issue.FirstSeenBuildID)
}

func TestClosedRecordsStillAdvanceTheCursor(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusSucceeded)
	for i := 0; i < 101; i++ {
		f.seedIssue("task", fmt.Sprintf("unit/%03d", i), entity.IssueStatusClosed)
	}
	msg := f.message("build", true)
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, "unit/099", f.progress.CursorUnitID)
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
	require.False(t, f.progress.HasPlannedChange())
	require.Empty(t, f.logs)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, "unit/100", f.progress.CursorUnitID)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Empty(t, f.logs)
}

func TestRefreshCheckpointsAtTheBatchBoundary(t *testing.T) {
	for _, count := range []int{0, 99, 100, 103} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusRunning)
			for i := 0; i < count; i++ {
				f.reportIssue("build", "task", fmt.Sprintf("unit/%03d", i))
			}
			msg := f.message("build", false)
			require.NoError(t, f.deliver(msg, count >= 100))
			require.Len(t, f.states, min(count, 100))
			if count >= 100 {
				require.Equal(t, "unit/099", f.progress.CursorUnitID)
				require.NoError(t, f.deliver(msg, false))
			}
			require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
			require.Len(t, f.states, count)
			require.Len(t, f.logs, count)
		})
	}
}

func TestShortReadsWithMoreThanOneBatchOfCombinedKeys(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	for i := 0; i < 99; i++ {
		f.reportIssue("build", "task", fmt.Sprintf("unit/%03d", 2*i))
		f.seedIssue("task", fmt.Sprintf("unit/%03d", 2*i+1), entity.IssueStatusOpen)
	}
	msg := f.message("build", true)
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, "unit/099", f.progress.CursorUnitID)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, "unit/197", f.progress.CursorUnitID)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	open, closed := 0, 0
	for _, issue := range f.states {
		if issue.Status == entity.IssueStatusOpen {
			open++
		} else {
			closed++
		}
	}
	require.Equal(t, 99, open)
	require.Equal(t, 99, closed)
	require.Len(t, f.logs, 198)
}

func TestCanceledBuildKeepsExistingIssuesOpen(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusCancelled)
	f.seedIssue("old", "", entity.IssueStatusOpen)
	f.reportIssue("build", "new", "")
	f.failures["issue/list"] = 1
	require.NoError(t, f.deliver(f.message("build", true), false))
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"old", ""}].Status)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"new", ""}].Status)
	require.Len(t, f.logs, 1)
}

func TestFinalNotificationBeforeLateRefreshAndOlderBuild(t *testing.T) {
	f := newFixture(t)
	f.source("older-build", "9", entity.BuildStatusSucceeded)
	f.source("newer-build", "10", entity.BuildStatusSucceeded)
	require.NoError(t, f.deliver(f.message("newer-build", true), false))
	before := f.progress
	f.reportIssue("newer-build", "late", "")
	f.reportIssue("older-build", "old", "")
	require.NoError(t, f.deliver(f.message("newer-build", false), false))
	require.NoError(t, f.deliver(f.message("older-build", true), false))
	require.Equal(t, before, f.progress)
	require.Empty(t, f.states)
}

func TestAnotherNotificationFinishesSavedWorkBeforeItsOwnBuild(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.source("build/101", "101", entity.BuildStatusSucceeded)
	f.reportIssue("build/100", "task", "unit")
	f.retain(openingChange())
	incoming := f.message("build/101", true)
	require.NoError(t, f.deliver(incoming, true))
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Equal(t, "build/100", f.progress.BuildID)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"task", "unit"}].Status)
	require.NoError(t, f.deliver(incoming, false))
	require.Equal(t, "build/101", f.progress.BuildID)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", "unit"}].Status)
	require.Equal(t, int32(2), f.states[issueKey{"task", "unit"}].Version)
}

func TestIssueMutationWaitsForItsPlannedChange(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusRunning)
	f.reportIssue("build", "task", "unit")
	f.failures["progress/update"] = 1
	msg := f.message("build", false)
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.Empty(t, f.states)
	require.Empty(t, f.logs)
	require.False(t, f.progress.HasPlannedChange())
	require.NoError(t, f.deliver(msg, false))
	require.Len(t, f.states, 1)
	require.Len(t, f.logs, 1)
}

func TestRetryRepairsHistoryBeforeFinalizing(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	f.reportIssue("build", "task", "unit")
	f.failures["history/create"] = 1
	msg := f.message("build", true)
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	saved := f.progress.PlannedChange
	require.True(t, f.progress.HasPlannedChange())
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.Equal(t, "unit", f.progress.CursorUnitID)
	require.Equal(t, int32(0), saved.ExpectedVersion)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.False(t, f.progress.HasPlannedChange())
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.Equal(t, "unit", f.progress.CursorUnitID)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
	require.Equal(t, saved.TimestampMs, f.logs[logKey{issueKey{"task", "unit"}, "build", entity.IssueEventOpened}].TimestampMs)
}

func TestRetryFinishesTheCursorIssueBeforeScanningTheNextBatch(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusRunning)
	for i := 0; i < 101; i++ {
		f.reportIssue("build", "task", fmt.Sprintf("unit/%03d", i))
	}
	f.failures["history/create"] = 1
	msg := f.message("build", false)

	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.Equal(t, "unit/000", f.progress.CursorUnitID)
	require.True(t, f.progress.HasPlannedChange())
	require.Len(t, f.states, 1)
	require.Empty(t, f.logs)

	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, "unit/099", f.progress.CursorUnitID)
	require.False(t, f.progress.HasPlannedChange())
	require.Len(t, f.states, 100)
	require.Len(t, f.logs, 100)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit/000"}].Version)

	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, "unit/100", f.progress.CursorUnitID)
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Len(t, f.states, 101)
	require.Len(t, f.logs, 101)
}

func TestPlanningFailuresDoNotChangeIssues(t *testing.T) {
	for _, operation := range []string{"build-issues/list", "issue/list", "build-issues/get", "issue/get"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusSucceeded)
			f.seedIssue("task", "unit", entity.IssueStatusOpen)
			f.failures[operation] = 1
			require.ErrorIs(t, f.deliver(f.message("build", true), false), assert.AnError)
			require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"task", "unit"}].Status)
			require.Empty(t, f.logs)
			require.False(t, f.progress.HasPlannedChange())
		})
	}
}

func TestFinalizationRequiresTheWinningTerminalBuild(t *testing.T) {
	for _, status := range []entity.BuildStatus{entity.BuildStatusRunning, entity.BuildStatusSucceeded} {
		t.Run(string(status), func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", status)
			request := f.requests["10"]
			request.TerminalBuildID = ""
			f.requests["10"] = request
			require.Error(t, f.deliver(f.message("build", true), false))
			require.False(t, f.hasProgress)
			require.Empty(t, f.states)
		})
	}
	f := newFixture(t)
	f.source("loser", "10", entity.BuildStatusFailed)
	request := f.requests["10"]
	request.TerminalBuildID = "winner"
	f.requests["10"] = request
	require.NoError(t, f.deliver(f.message("loser", true), false))
	require.False(t, f.hasProgress)
}

func TestProcessRejectsInvalidMessageAndSource(t *testing.T) {
	for _, kind := range []string{"payload", "partition", "tenant", "missing message ID", "missing build", "request queue", "invalid request ID"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusRunning)
			msg := f.message("build", false)
			switch kind {
			case "payload":
				msg.Payload = []byte("invalid")
			case "partition":
				msg.PartitionKey = "other"
			case "tenant":
				msg.Tenant = "other"
			case "missing message ID":
				msg.ID = ""
			case "missing build":
				delete(f.builds, "build")
			case "request queue":
				request := f.requests["10"]
				request.Queue = "other"
				f.requests["10"] = request
			case "invalid request ID":
				request := f.requests["10"]
				request.ID = "010"
				f.requests["10"] = request
			}
			require.Error(t, f.deliver(msg, false))
			require.False(t, f.hasProgress)
			require.Empty(t, f.states)
		})
	}
}

func TestLosingThePlannedChangeCASCannotWriteAnIssue(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.source("build/101", "101", entity.BuildStatusSucceeded)
	f.reportIssue("build/100", "task", "unit")
	old := f.message("build/100", false)
	paused, resume := make(chan struct{}), make(chan struct{})
	var intercepted atomic.Bool
	f.beforeProgressUpdate = func(next entity.IssueTrackingState) {
		if next.HasPlannedChange() && intercepted.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}
	result := make(chan error, 1)
	go func() { result <- f.deliver(old, false) }()
	<-paused

	require.NoError(t, f.deliver(old, false))
	require.NoError(t, f.deliver(f.message("build/101", true), false))
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", "unit"}].Status)
	close(resume)
	require.ErrorIs(t, <-result, storage.ErrVersionMismatch)
	require.Equal(t, int32(2), f.states[issueKey{"task", "unit"}].Version)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", "unit"}].Status)
	require.Len(t, f.logs, 2)
}
