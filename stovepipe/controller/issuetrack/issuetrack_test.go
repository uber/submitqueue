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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestBatchRejectsInvalidClosingPass(t *testing.T) {
	for _, scenario := range []string{"missing terminal intent", "task-wide failure", "canceled build", "running build", "not terminal build"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/100", "100", entity.BuildStatusSucceeded)
			f.seedIssue("task", "unit", entity.IssueStatusOpen)
			original := f.states[issueKey{"task", "unit"}]
			progress := currentCursor()
			progress.Phase = entity.IssueTrackingPhaseFinalizing
			progress.Finalize = true

			switch scenario {
			case "missing terminal intent":
				progress.Finalize = false
			case "task-wide failure":
				progress.HasTaskWideIssues = true
			case "canceled build":
				f.source("build/100", "100", entity.BuildStatusCancelled)
			case "running build":
				f.source("build/100", "100", entity.BuildStatusRunning)
			case "not terminal build":
				request := f.requests["100"]
				request.TerminalBuildID = "another-build"
				f.requests["100"] = request
			}
			f.progress = progress
			f.hasProgress = true

			_, err := f.controller.reconcileBatch(context.Background(), f.store, testQueue, progress)
			require.Error(t, err)
			require.Equal(t, original, f.states[issueKey{"task", "unit"}])
			require.Equal(t, progress, f.progress)
			require.Empty(t, f.logs)
		})
	}
}

func TestProcessIssueOpensAndCloses(t *testing.T) {
	for _, reported := range []bool{true, false} {
		name := "close"
		if reported {
			name = "open"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/100", "100", entity.BuildStatusSucceeded)
			progress := currentCursor()
			progress.Phase = entity.IssueTrackingPhaseFinalizing
			progress.Finalize = true
			progress.CursorTaskID, progress.CursorUnitID = "", ""
			f.retain(progress)
			if !reported {
				f.seedIssue("task", "unit", entity.IssueStatusOpen)
			}

			next, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, reported)
			require.NoError(t, err)
			require.Equal(t, "task", next.CursorTaskID)
			require.Equal(t, "unit", next.CursorUnitID)
			require.Equal(t, int32(2), next.Version)
			issue := f.states[issueKey{"task", "unit"}]
			summary := f.summaries[issueKey{"task", "unit"}]
			require.Equal(t, issue.Status, summary.Status)
			require.Equal(t, issue.FirstSeenBuildID, summary.FirstSeenBuildID)
			require.Equal(t, "100", summary.RequestID)
			require.Equal(t, issue.LastChangedBuildID, summary.BuildID)
			require.Equal(t, issue.ChangedAtMs, summary.ChangedAtMs)
			require.Equal(t, "build/100", issue.LastProcessedBuildID)
			require.Equal(t, "build/100", issue.LastChangedBuildID)
			require.Equal(t, int64(1000), issue.ChangedAtMs)
			if reported {
				require.Equal(t, issue.FirstSeenBuildID, f.openIssues[issueKey{"task", "unit"}].FirstSeenBuildID)
				require.Equal(t, entity.IssueStatusOpen, issue.Status)
				require.Equal(t, "build/100", issue.FirstSeenBuildID)
				require.Equal(t, int32(1), issue.Version)
				require.Contains(t, f.logs, logKey{issueKey{"task", "unit"}, "build/100", entity.IssueStatusOpen})
			} else {
				require.NotContains(t, f.openIssues, issueKey{"task", "unit"})
				require.Equal(t, entity.IssueStatusClosed, issue.Status)
				require.Equal(t, "older", issue.FirstSeenBuildID)
				require.Equal(t, int32(8), issue.Version)
				require.Contains(t, f.logs, logKey{issueKey{"task", "unit"}, "build/100", entity.IssueStatusClosed})
			}
		})
	}
}

func TestUnchangedIssueRecordsTheProcessedBuild(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.seedIssue("task", "unit", entity.IssueStatusOpen)
	progress := currentCursor()
	f.retain(progress)

	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.NoError(t, err)
	issue := f.states[issueKey{"task", "unit"}]
	require.Equal(t, "build/100", issue.LastProcessedBuildID)
	require.Equal(t, "older", issue.LastChangedBuildID)
	require.Equal(t, "older", issue.FirstSeenBuildID)
	require.Equal(t, int64(500), issue.ChangedAtMs)
	require.Equal(t, int32(8), issue.Version)
	require.Empty(t, f.logs)
}

func TestHistoryRetryUsesTheStoredTransition(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.reportIssue("build/100", "task", "unit")
	progress := currentCursor()
	f.retain(progress)
	f.failures["history/create"] = 1

	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.ErrorIs(t, err, assert.AnError)
	applied := f.states[issueKey{"task", "unit"}]
	require.Empty(t, f.logs)
	f.controller.nowMs = func() int64 { return 2000 }

	_, err = f.controller.processIssue(context.Background(), f.store, f.progress, issueKey{"task", "unit"}, false)
	require.NoError(t, err)
	require.Equal(t, applied, f.states[issueKey{"task", "unit"}])
	require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", "unit"}, "build/100", entity.IssueStatusOpen}].TimestampMs)
}

func TestCursorFailureDoesNotMutateTheIssue(t *testing.T) {
	f := newFixture(t)
	progress := currentCursor()
	f.retain(progress)
	f.failures["progress/update"] = 1

	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.ErrorIs(t, err, assert.AnError)
	require.Empty(t, f.states)
	require.Empty(t, f.logs)
	require.Equal(t, progress, f.progress)
}

func TestSummaryRetryRepairsTheRetainedTransition(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.reportIssue("build/100", "task", "unit")
	progress := currentCursor()
	f.retain(progress)
	f.failures["summary/create"] = 1

	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.ErrorIs(t, err, assert.AnError)
	applied := f.states[issueKey{"task", "unit"}]
	require.Len(t, f.logs, 1)
	require.Empty(t, f.summaries)
	require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
	f.controller.nowMs = func() int64 { return 2000 }

	_, err = f.controller.processIssue(context.Background(), f.store, f.progress, issueKey{"task", "unit"}, false)
	require.NoError(t, err)
	require.Equal(t, applied, f.states[issueKey{"task", "unit"}])
	require.Len(t, f.logs, 1)
	summary := f.summaries[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusOpen, summary.Status)
	require.Equal(t, applied.ChangedAtMs, summary.ChangedAtMs)
	require.Equal(t, applied.FirstSeenBuildID, summary.FirstSeenBuildID)
}

func TestOlderSourceCannotChangeTheIssue(t *testing.T) {
	f := newFixture(t)
	f.source("newer", "101", entity.BuildStatusSucceeded)
	f.seedIssue("task", "unit", entity.IssueStatusClosed)
	current := f.states[issueKey{"task", "unit"}]
	current.LastProcessedBuildID = "newer"
	f.states[issueKey{"task", "unit"}] = current
	progress := currentCursor()
	f.retain(progress)

	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.ErrorIs(t, err, storage.ErrVersionMismatch)
	require.Equal(t, current, f.states[issueKey{"task", "unit"}])
	require.Equal(t, progress, f.progress)
	require.Empty(t, f.logs)
}

func TestOpenIssueFailureKeepsTheCursorForRetry(t *testing.T) {
	f := newFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.reportIssue("build/100", "task", "unit")
	progress := currentCursor()
	f.retain(progress)
	f.failures["open-issue/create"] = 1

	_, err := f.controller.processIssue(context.Background(), f.store, progress, issueKey{"task", "unit"}, true)
	require.ErrorIs(t, err, assert.AnError)
	accepted := f.states[issueKey{"task", "unit"}]
	require.Contains(t, f.summaries, issueKey{"task", "unit"})
	require.Empty(t, f.openIssues)
	require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)
	require.Equal(t, "task", f.progress.CursorTaskID)
	require.Equal(t, "unit", f.progress.CursorUnitID)

	_, err = f.controller.processIssue(context.Background(), f.store, f.progress, issueKey{"task", "unit"}, false)
	require.NoError(t, err)
	require.Equal(t, accepted, f.states[issueKey{"task", "unit"}])
	require.Equal(t, accepted.FirstSeenBuildID, f.openIssues[issueKey{"task", "unit"}].FirstSeenBuildID)
	require.Len(t, f.logs, 1)
}
