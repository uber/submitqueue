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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
)

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
	msg := f.message("build", true)
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"a", ""}].Status)
	require.Equal(t, int32(8), f.states[issueKey{"a", ""}].Version)
	require.Equal(t, "older", f.states[issueKey{"b", "unit"}].FirstSeenBuildID)
	require.Equal(t, int32(8), f.states[issueKey{"b", "unit"}].Version)
	require.Equal(t, "build", f.states[issueKey{"c", "unit"}].FirstSeenBuildID)
	require.Equal(t, int32(8), f.states[issueKey{"closed", ""}].Version)
	require.Len(t, f.logs, 2)
	require.Contains(t, f.logs, logKey{issueKey{"a", ""}, "build", entity.IssueStatusClosed})
	require.Contains(t, f.logs, logKey{issueKey{"c", "unit"}, "build", entity.IssueStatusOpen})
}

func TestEmptyFinalBuildClosesIssuesButRetainsTheirRows(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusSucceeded)
	f.seedIssue("task", "", entity.IssueStatusOpen)
	msg := f.message("build", true)
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
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
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, "unit/099", f.progress.CursorUnitID)
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
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

func TestFinalizationTraversesBuildIssuesThenExistingStates(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	for i := 0; i < 99; i++ {
		f.reportIssue("build", "task", fmt.Sprintf("unit/%03d", 2*i))
		f.seedIssue("task", fmt.Sprintf("unit/%03d", 2*i+1), entity.IssueStatusOpen)
	}
	msg := f.message("build", true)
	require.NoError(t, f.deliver(msg, true))
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
	msg := f.message("newer-build", true)
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
	before := f.progress
	f.reportIssue("newer-build", "late", "")
	f.reportIssue("older-build", "old", "")
	require.NoError(t, f.deliver(f.message("newer-build", false), false))
	require.NoError(t, f.deliver(f.message("older-build", true), false))
	require.Equal(t, before, f.progress)
	require.Empty(t, f.states)
}

func TestAnotherNotificationFinishesTheCursorBeforeItsOwnBuild(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.source("build/101", "101", entity.BuildStatusSucceeded)
	f.reportIssue("build/100", "task", "unit")
	f.retain(currentCursor())
	incoming := f.message("build/101", true)
	require.NoError(t, f.deliver(incoming, true))
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Equal(t, "build/100", f.progress.BuildID)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"task", "unit"}].Status)
	require.NoError(t, f.deliver(incoming, true))
	require.NoError(t, f.deliver(incoming, false))
	require.Equal(t, "build/101", f.progress.BuildID)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", "unit"}].Status)
	require.Equal(t, int32(2), f.states[issueKey{"task", "unit"}].Version)
}

func TestIssueMutationWaitsForItsCursor(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusRunning)
	f.reportIssue("build", "task", "unit")
	f.failures["progress/update"] = 1
	msg := f.message("build", false)
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.Empty(t, f.states)
	require.Empty(t, f.logs)
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
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.Equal(t, "unit", f.progress.CursorUnitID)
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.Equal(t, "unit", f.progress.CursorUnitID)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
	require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", "unit"}, "build", entity.IssueStatusOpen}].TimestampMs)
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
	require.Len(t, f.states, 1)
	require.Empty(t, f.logs)

	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, "unit/099", f.progress.CursorUnitID)
	require.Len(t, f.states, 100)
	require.Len(t, f.logs, 100)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit/000"}].Version)

	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, "unit/100", f.progress.CursorUnitID)
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Len(t, f.states, 101)
	require.Len(t, f.logs, 101)
}

func TestReadFailuresDoNotChangeIssues(t *testing.T) {
	for _, operation := range []string{"build-issues/list", "issue/list", "build-issues/get", "issue/get"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusSucceeded)
			f.seedIssue("task", "unit", entity.IssueStatusOpen)
			msg := f.message("build", true)
			if operation != "build-issues/list" {
				require.NoError(t, f.deliver(msg, true))
			}
			f.failures[operation] = 1
			require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
			require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"task", "unit"}].Status)
			require.Empty(t, f.logs)
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

func TestCursorRetriesPartialWrites(t *testing.T) {
	for _, operation := range []string{"issue/create", "issue/update", "history/create", "complete-pass"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build", "10", entity.BuildStatusRunning)
			f.reportIssue("build", "task", "unit")
			if operation == "issue/update" {
				f.seedIssue("task", "unit", entity.IssueStatusClosed)
			}
			if operation == "complete-pass" {
				f.failOnPhase = entity.IssueTrackingPhaseRefreshed
			} else {
				f.failures[operation] = 1
			}
			msg := f.message("build", false)
			require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
			require.Equal(t, "task", f.progress.CursorTaskID)
			require.Equal(t, "unit", f.progress.CursorUnitID)
			require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
			require.NoError(t, f.deliver(msg, false))
			require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
			require.Equal(t, "build", f.states[issueKey{"task", "unit"}].LastProcessedBuildID)
			require.Equal(t, "build", f.states[issueKey{"task", "unit"}].LastChangedBuildID)
			require.Len(t, f.logs, 1)
			if operation == "issue/update" {
				require.Equal(t, int32(8), f.states[issueKey{"task", "unit"}].Version)
			} else {
				require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
			}
		})
	}
}

func TestHistoryRecordsTheActualChangingBuildNotTheLatestProcessedBuild(t *testing.T) {
	f := newFixture(t)
	f.seedIssue("task", "unit", entity.IssueStatusOpen)
	f.source("build/100", "100", entity.BuildStatusSucceeded)
	f.failures["history/create"] = 1
	msg := f.message("build/100", true)
	require.NoError(t, f.deliver(msg, true))
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	changed := f.states[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusClosed, changed.Status)
	require.Equal(t, "build/100", changed.LastChangedBuildID)
	require.Equal(t, "build/100", changed.LastProcessedBuildID)
	require.Equal(t, "older", changed.FirstSeenBuildID)
	require.Empty(t, f.logs)
	f.controller.nowMs = func() int64 { return 2000 }
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", "unit"}, "build/100", entity.IssueStatusClosed}].TimestampMs)
	require.Equal(t, changed, f.states[issueKey{"task", "unit"}])

	f.source("build/101", "101", entity.BuildStatusSucceeded)
	next := f.message("build/101", true)
	require.NoError(t, f.deliver(next, true))
	require.NoError(t, f.deliver(next, false))
	retained := f.states[issueKey{"task", "unit"}]
	require.Equal(t, "build/101", retained.LastProcessedBuildID)
	require.Equal(t, "build/100", retained.LastChangedBuildID)
	require.Equal(t, int64(1000), retained.ChangedAtMs)
	require.Equal(t, int32(9), retained.Version)
	require.Len(t, f.logs, 1)
}

func TestTaskWideFailureSuppressesClosuresOnlyForItsTask(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	for _, task := range []string{"affected", "unrelated"} {
		f.seedIssue(task, "", entity.IssueStatusOpen)
		f.seedIssue(task, "unit", entity.IssueStatusOpen)
		f.seedIssue(task, "closed", entity.IssueStatusClosed)
	}
	f.seedIssue("affected", "reopen", entity.IssueStatusClosed)
	f.reportIssue("build/100", "affected", "")
	f.reportIssue("build/100", "affected", "reopen")
	msg := f.message("build/100", true)

	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	for _, key := range []issueKey{{"affected", ""}, {"affected", "unit"}, {"affected", "reopen"}} {
		require.Equal(t, entity.IssueStatusOpen, f.states[key].Status)
		require.Contains(t, f.openIssues, key)
	}
	for _, key := range []issueKey{{"affected", "closed"}, {"unrelated", ""}, {"unrelated", "unit"}, {"unrelated", "closed"}} {
		require.Equal(t, entity.IssueStatusClosed, f.states[key].Status)
		require.NotContains(t, f.openIssues, key)
	}
	require.Len(t, f.logs, 3)
	require.Contains(t, f.logs, logKey{issueKey{"affected", "reopen"}, "build/100", entity.IssueStatusOpen})
	require.Contains(t, f.logs, logKey{issueKey{"unrelated", ""}, "build/100", entity.IssueStatusClosed})
	require.Contains(t, f.logs, logKey{issueKey{"unrelated", "unit"}, "build/100", entity.IssueStatusClosed})

	f.source("build/101", "101", entity.BuildStatusSucceeded)
	next := f.message("build/101", true)
	require.NoError(t, f.deliver(next, true))
	require.NoError(t, f.deliver(next, false))
	for _, issue := range f.states {
		require.Equal(t, entity.IssueStatusClosed, issue.Status)
	}
	require.Empty(t, f.openIssues)
}

func TestTaskWideFailureInLaterBatchProtectsOnlyItsTask(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	f.seedIssue("old", "unit", entity.IssueStatusOpen)
	f.seedIssue("zzz", "unit", entity.IssueStatusOpen)
	for i := 0; i < 105; i++ {
		f.reportIssue("build", fmt.Sprintf("task/%03d", i), "unit")
	}
	f.reportIssue("build", "zzz", "")
	msg := f.message("build", true)

	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"old", "unit"}].Status)
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"old", "unit"}].Status)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"zzz", "unit"}].Status)
	require.Len(t, f.logs, 107)
}

func TestTaskWideFlagSurvivesNoOpOpeningBatchAndRetry(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	for i := 0; i < 104; i++ {
		unit := fmt.Sprintf("unit/%03d", i)
		if i == 0 {
			unit = ""
		}
		f.reportIssue("build/100", "task", unit)
		f.seedIssue("task", unit, entity.IssueStatusOpen)
		key := issueKey{"task", unit}
		issue := f.states[key]
		issue.LastProcessedBuildID = "build/100"
		f.states[key] = issue
	}
	prior := currentCursor()
	prior.Phase = entity.IssueTrackingPhaseRefreshed
	f.retain(prior)
	msg := f.message("build/100", true)

	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
	require.True(t, f.progress.Finalize)
	require.True(t, f.progress.HasTaskWideIssues)
	require.Equal(t, "unit/099", f.progress.CursorUnitID)
	require.Empty(t, f.logs)
	f.failures["build-issues/list"] = 1
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.True(t, f.progress.HasTaskWideIssues)
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.True(t, f.progress.HasTaskWideIssues)
	for _, issue := range f.states {
		require.Equal(t, int32(7), issue.Version)
	}
}

func TestAnotherNotificationRetainsTheSavedTerminalIntent(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.source("build/101", "101", entity.BuildStatusRunning)
	f.reportIssue("build/100", "task/a", "unit")
	f.reportIssue("build/100", "task/b", "unit")
	progress := currentCursor()
	progress.CursorTaskID, progress.CursorUnitID = "task/a", "unit"
	progress.Finalize = true
	f.retain(progress)
	msg := f.message("build/101", false)

	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, "build/100", f.progress.BuildID)
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
	require.True(t, f.progress.Finalize)
	require.Empty(t, f.progress.CursorTaskID)
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, "build/101", f.progress.BuildID)
	require.False(t, f.progress.Finalize)
	require.False(t, f.progress.HasTaskWideIssues)
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
}

func TestOpeningCursorCannotCloseAnUnreportedIssue(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.seedIssue("task", "unit", entity.IssueStatusOpen)
	progress := currentCursor()
	progress.Finalize = true
	progress.HasTaskWideIssues = true
	f.retain(progress)
	msg := f.message("build/100", true)
	msg.ID = progress.NotificationID

	require.Error(t, f.deliver(msg, false))
	issue := f.states[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusOpen, issue.Status)
	require.Equal(t, int32(7), issue.Version)
	require.Empty(t, f.logs)
}

func TestTaskWideHistoryRetryThenClosesUnrelatedIssues(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	f.seedIssue("old", "unit", entity.IssueStatusOpen)
	f.reportIssue("build", "task", "")
	f.failures["history/create"] = 1
	msg := f.message("build", true)

	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.True(t, f.progress.HasTaskWideIssues)
	require.True(t, f.progress.Finalize)
	require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
	f.controller.nowMs = func() int64 { return 2000 }
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"old", "unit"}].Status)
	require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", ""}, "build", entity.IssueStatusOpen}].TimestampMs)
}

func TestRetryResetsCursorBeforeClosing(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusFailed)
	f.seedIssue("old", "unit", entity.IssueStatusOpen)
	f.reportIssue("build", "task", "unit")
	f.failOnPhase = entity.IssueTrackingPhaseFinalizing
	msg := f.message("build", true)

	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.True(t, f.progress.Finalize)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"old", "unit"}].Status)
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
	require.Empty(t, f.progress.CursorTaskID)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"old", "unit"}].Status)
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
	require.Len(t, f.logs, 2)
}

func TestNewerRefreshDoesNotDiscardOlderFinalization(t *testing.T) {
	for _, observation := range []string{"empty", "unrelated failure", "same failure"} {
		t.Run(observation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/100", "100", entity.BuildStatusSucceeded)
			f.source("build/101", "101", entity.BuildStatusRunning)
			f.seedIssue("old", "unit", entity.IssueStatusOpen)
			if observation == "unrelated failure" {
				f.reportIssue("build/101", "new", "unit")
			} else if observation == "same failure" {
				f.reportIssue("build/101", "old", "unit")
			}
			require.NoError(t, f.deliver(f.message("build/101", false), false))
			final := f.message("build/100", true)
			require.NoError(t, f.deliver(final, true))
			require.NoError(t, f.deliver(final, false))
			require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
			if observation == "same failure" {
				require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"old", "unit"}].Status)
				require.Equal(t, "build/101", f.states[issueKey{"old", "unit"}].LastProcessedBuildID)
				require.Empty(t, f.logs)
			} else {
				require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"old", "unit"}].Status)
				require.Contains(t, f.logs, logKey{issueKey{"old", "unit"}, "build/100", entity.IssueStatusClosed})
			}
			if observation == "unrelated failure" {
				require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"new", "unit"}].Status)
				require.Equal(t, "build/101", f.states[issueKey{"new", "unit"}].LastProcessedBuildID)
			}
		})
	}
}

func TestFinalizedBoundarySurvivesLaterRefreshes(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusSucceeded)
	f.source("build/101", "101", entity.BuildStatusSucceeded)
	f.source("build/102", "102", entity.BuildStatusRunning)
	f.seedIssue("resolved", "unit", entity.IssueStatusOpen)
	final := f.message("build/101", true)
	require.NoError(t, f.deliver(final, true))
	require.NoError(t, f.deliver(final, false))
	f.reportIssue("build/102", "new", "unit")
	require.NoError(t, f.deliver(f.message("build/102", false), false))
	before := f.progress
	for _, build := range []string{"build/100", "build/101"} {
		f.reportIssue(build, "resolved", "unit")
		f.reportIssue(build, "stale", "unit")
		for _, terminal := range []bool{false, true} {
			require.NoError(t, f.deliver(f.message(build, terminal), false))
		}
	}
	require.Equal(t, before, f.progress)
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"resolved", "unit"}].Status)
	require.NotContains(t, f.states, issueKey{"stale", "unit"})
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"new", "unit"}].Status)
	require.Len(t, f.logs, 2)
}

func TestOlderFinalizationSkipsNewerIssuesAcrossBatches(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.source("build/101", "101", entity.BuildStatusRunning)
	f.seedIssue("zzz", "resolved", entity.IssueStatusOpen)
	for i := 0; i < 105; i++ {
		unit := fmt.Sprintf("unit/%03d", i)
		f.reportIssue("build/100", "task", unit)
		f.reportIssue("build/101", "task", unit)
	}
	refresh := f.message("build/101", false)
	require.NoError(t, f.deliver(refresh, true))
	require.NoError(t, f.deliver(refresh, false))
	final := f.message("build/100", true)
	require.NoError(t, f.deliver(final, true))
	require.NoError(t, f.deliver(final, true))
	require.NoError(t, f.deliver(final, true))
	require.NoError(t, f.deliver(final, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	for key, state := range f.states {
		if key.task == "task" {
			require.Equal(t, entity.IssueStatusOpen, state.Status)
			require.Equal(t, "build/101", state.LastProcessedBuildID)
		}
	}
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"zzz", "resolved"}].Status)
	require.Len(t, f.logs, 106)
}

func TestFailedCompletionCheckpointDoesNotLoseFinalization(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusSucceeded)
	f.source("build/101", "101", entity.BuildStatusRunning)
	f.seedIssue("old", "unit", entity.IssueStatusOpen)
	final := f.message("build/100", true)
	require.NoError(t, f.deliver(final, true))
	f.failOnPhase = entity.IssueTrackingPhaseFinalized
	require.ErrorIs(t, f.deliver(final, false), assert.AnError)
	refresh := f.message("build/101", false)
	require.NoError(t, f.deliver(refresh, true))
	require.NoError(t, f.deliver(refresh, false))
	f.reportIssue("build/100", "stale", "unit")
	require.NoError(t, f.deliver(f.message("build/100", false), false))
	require.NotContains(t, f.states, issueKey{"stale", "unit"})
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"old", "unit"}].Status)
	require.Len(t, f.logs, 1)
}

func TestLegacyTerminalBoundarySurvivesANewerRefresh(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusSucceeded)
	f.source("build/101", "101", entity.BuildStatusRunning)
	legacy := currentCursor()
	legacy.Finalize, legacy.Phase = true, entity.IssueTrackingPhaseFinalized
	f.retain(legacy)
	f.reportIssue("build/101", "new", "unit")
	require.NoError(t, f.deliver(f.message("build/101", false), false))
	before := f.progress
	f.reportIssue("build/100", "stale", "unit")
	require.NoError(t, f.deliver(f.message("build/100", false), false))
	require.NoError(t, f.deliver(f.message("build/100", true), false))
	require.Equal(t, before, f.progress)
	require.NotContains(t, f.states, issueKey{"stale", "unit"})
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"new", "unit"}].Status)
}
