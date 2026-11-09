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
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestRequestIssuesAcrossBatches(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.seedIssue("closed", "unit", entity.IssueStatusClosed)
	f.seedIssue("close-me", "unit", entity.IssueStatusOpen)
	f.seedIssue("existing", "unit", entity.IssueStatusOpen)
	f.seedIssue("reopen", "unit", entity.IssueStatusClosed)
	f.reportIssue("build/100", "existing", "unit")
	f.reportIssue("build/100", "reopen", "unit")
	for i := 0; i < 230; i++ {
		f.reportIssue("build/100", "test", fmt.Sprintf("unit-%03d", i))
	}
	progress := currentCursor()
	progress.CursorTaskID, progress.CursorUnitID = "", ""
	progress.Finalize = true
	f.retain(progress)
	for f.progress.Phase != entity.IssueTrackingPhaseFinalized {
		next, err := f.controller.reconcileBatch(context.Background(), f.store, testQueue, f.progress)
		require.NoError(t, err)
		require.Equal(t, next, f.progress)
	}
	results := f.requestIssues["100"]
	require.Len(t, results, 233)
	require.Equal(t, entity.RequestIssueKindExisting, results[issueKey{"existing", "unit"}].Kind)
	require.Equal(t, "older", results[issueKey{"existing", "unit"}].FirstSeenBuildID)
	require.Equal(t, entity.RequestIssueKindNew, results[issueKey{"reopen", "unit"}].Kind)
	require.Equal(t, entity.RequestIssueKindClosed, results[issueKey{"close-me", "unit"}].Kind)
	require.NotContains(t, results, issueKey{"closed", "unit"})
	for i := 0; i < 230; i++ {
		require.Equal(t, entity.RequestIssueKindNew, results[issueKey{"test", fmt.Sprintf("unit-%03d", i)}].Kind)
	}

	f.source("build/101", "101", entity.BuildStatusSucceeded)
	next := currentCursor()
	next.RequestID, next.BuildID = "101", "build/101"
	next.Version = f.progress.Version
	next.Finalize = true
	next.CursorTaskID, next.CursorUnitID = "", ""
	f.retain(next)
	for f.progress.Phase != entity.IssueTrackingPhaseFinalized {
		_, err := f.controller.reconcileBatch(context.Background(), f.store, testQueue, f.progress)
		require.NoError(t, err)
	}
	require.Equal(t, results, f.requestIssues["100"])
	require.Equal(t, entity.RequestIssueKindNew, f.requestIssues["100"][issueKey{"reopen", "unit"}].Kind)
	require.Equal(t, entity.RequestIssueKindClosed, f.requestIssues["101"][issueKey{"reopen", "unit"}].Kind)
}

func TestRequestIssueFailureKeepsCursor(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			f := newFixture(t)
			f.source("build/100", "100", entity.BuildStatusRunning)
			if existing {
				f.seedIssue("task", "unit", entity.IssueStatusOpen)
			}
			f.reportIssue("build/100", "task", "unit")
			f.reportIssue("build/100", "z", "unit")
			progress := currentCursor()
			progress.CursorTaskID, progress.CursorUnitID = "", ""
			f.retain(progress)
			f.failures["request-issue/create"] = 1
			_, err := f.controller.reconcileBatch(context.Background(), f.store, testQueue, progress)
			require.ErrorIs(t, err, assert.AnError)
			require.Equal(t, "task", f.progress.CursorTaskID)
			require.NotContains(t, f.states, issueKey{"z", "unit"})
			require.Empty(t, f.requestIssues)
			event := entity.IssueEventOpened
			if existing {
				event = entity.IssueEventReported
			}
			retained := f.logs[logKey{issueKey{"task", "unit"}, "build/100", event}]
			require.NotZero(t, retained.TimestampMs)
			_, err = f.controller.reconcileBatch(context.Background(), f.store, testQueue, f.progress)
			require.NoError(t, err)
			kind := entity.RequestIssueKindNew
			if existing {
				kind = entity.RequestIssueKindExisting
			}
			require.Equal(t, kind, f.requestIssues["100"][issueKey{"task", "unit"}].Kind)
			require.Len(t, f.requestIssues["100"], 2)
			require.Equal(t, retained, f.logs[logKey{issueKey{"task", "unit"}, "build/100", event}])
		})
	}
}

func TestRecoveryRepairsRequestIssueBeforeAbandoning(t *testing.T) {
	for _, kind := range []entity.RequestIssueKind{entity.RequestIssueKindNew, entity.RequestIssueKindExisting, entity.RequestIssueKindClosed} {
		t.Run(string(kind), func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusFailed)
			progress := currentCursor()
			if kind != entity.RequestIssueKindNew {
				f.seedIssue("task", "unit", entity.IssueStatusOpen)
			}
			if kind == entity.RequestIssueKindClosed {
				progress.Phase, progress.Finalize = entity.IssueTrackingPhaseFinalizing, true
			} else {
				f.reportIssue("build/100", "task", "unit")
			}
			msg := f.message("build/100", true)
			progress.NotificationID = msg.ID
			f.retain(progress)
			f.failures["request-issue/create"] = 1
			_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, kind != entity.RequestIssueKindClosed)
			require.ErrorIs(t, err, assert.AnError)
			f.failures["request-issue/create"] = 1
			require.ErrorIs(t, f.recover(msg, false), assert.AnError)
			require.Equal(t, entity.IssueTrackingPhaseAbandoning, f.progress.Phase)
			require.NoError(t, f.recover(msg, false))
			require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
			require.Equal(t, kind, f.requestIssues["100"][issueKey{"task", "unit"}].Kind)
		})
	}
}

func TestRecoveryDoesNotAttributeUnappliedCursor(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.seedIssue("task", "unit", entity.IssueStatusOpen)
	progress := currentCursor()
	msg := f.message("build/100", true)
	progress.NotificationID = msg.ID
	f.retain(progress)
	require.NoError(t, f.recover(msg, false))
	require.Empty(t, f.requestIssues)
}

func TestTaskWideSuppressionDoesNotReportUnseenIssues(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.seedIssue("old", "unit", entity.IssueStatusOpen)
	f.reportIssue("build/100", "entry", "")
	msg := f.message("build/100", true)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Len(t, f.requestIssues["100"], 1)
	require.Equal(t, entity.RequestIssueKindNew, f.requestIssues["100"][issueKey{"entry", ""}].Kind)
	require.NotContains(t, f.requestIssues["100"], issueKey{"old", "unit"})
}

func TestRequestIssueReplayRejectsConflict(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	progress := currentCursor()
	f.retain(progress)
	f.reportIssue("build/100", "task", "unit")
	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.NoError(t, err)
	retained := f.requestIssues["100"][issueKey{"task", "unit"}]
	retained.Kind = entity.RequestIssueKindExisting
	f.requestIssues["100"][issueKey{"task", "unit"}] = retained
	_, err = f.controller.processIssue(context.Background(), f.store, f.progress, issueKey{"task", "unit"}, true)
	require.ErrorContains(t, err, "conflicts")
}

func TestReportedIssueFailureKeepsCursor(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.seedIssue("task", "unit", entity.IssueStatusOpen)
	f.reportIssue("build/100", "task", "unit")
	progress := currentCursor()
	progress.CursorTaskID, progress.CursorUnitID = "", ""
	f.retain(progress)
	f.failures["history/create"] = 1
	_, err := f.controller.reconcileBatch(context.Background(), f.store, testQueue, progress)
	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.Empty(t, f.requestIssues["100"])
	_, err = f.controller.reconcileBatch(context.Background(), f.store, testQueue, f.progress)
	require.NoError(t, err)
	require.Contains(t, f.logs, logKey{issueKey{"task", "unit"}, "build/100", entity.IssueEventReported})
	require.Equal(t, entity.RequestIssueKindExisting, f.requestIssues["100"][issueKey{"task", "unit"}].Kind)
}
