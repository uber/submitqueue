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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type recoveryFixture struct {
	*fixture
	dlq    *DLQController
	events map[string]entity.RequestLog
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	f := &recoveryFixture{fixture: newFixture(t), events: map[string]entity.RequestLog{}}
	materializer := requestlogmock.NewMockMaterializer(f.mocks)
	materializer.EXPECT().PersistLog(gomock.Any(), f.store, gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ storage.Storage, log entity.RequestLog) error {
			if err := f.fail("recovery/history"); err != nil {
				return err
			}
			if log.Event == entity.RequestEventIssuesReconciled {
				return f.materializer.PersistLog(ctx, f.store, log)
			}
			log.TimestampMs = 1000
			require.NoError(t, log.Validate())
			require.Equal(t, entity.RequestEventIssueTrackingAbandoned, log.Event)
			f.events[log.RequestID+"/"+log.ID] = log
			return nil
		}).AnyTimes()
	f.dlq = NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, materializer, consumer.TopicKey("issuetrack_dlq"), "test-dlq")
	f.dlq.controller.nowMs = func() int64 { return 1000 }
	return f
}

func (f *recoveryFixture) recover(msg entityqueue.Message, hold bool) error {
	f.t.Helper()
	delivery := consumermock.NewMockDelivery(f.mocks)
	delivery.EXPECT().Message().Return(msg)
	delivery.EXPECT().Failure().Return(failure.Failure{Message: "storage unavailable"}, true).AnyTimes()
	if hold {
		delivery.EXPECT().Hold(int64(0))
	}
	return f.dlq.Process(context.Background(), delivery)
}

func TestRecoveryKeepsAcceptedIssueChanges(t *testing.T) {
	for _, status := range []entity.IssueStatus{entity.IssueStatusOpen, entity.IssueStatusClosed} {
		t.Run(string(status), func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusFailed)
			f.seedIssue("already-opened", "unit", entity.IssueStatusOpen)
			f.seedIssue("already-closed", "unit", entity.IssueStatusClosed)
			f.seedIssue("unprocessed", "unit", entity.IssueStatusOpen)
			f.seedIssue("task", "unit", status)
			progress := currentCursor()
			progress.Phase, progress.Finalize = entity.IssueTrackingPhaseFinalizing, true
			msg := f.message("build/100", true)
			progress.NotificationID = msg.ID
			f.retain(progress)
			original := f.states[issueKey{"task", "unit"}]
			untouched := map[issueKey]entity.IssueState{}
			for key, state := range f.states {
				untouched[key] = state
			}
			delete(f.initialLogs, logKey{issueKey{"task", "unit"}, original.LastChangedBuildID, status})
			f.failures["issue/list"], f.failures["build-issues/list"], f.failures["build-issues/get"] = 1, 1, 1

			require.NoError(t, f.recover(msg, false))
			require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
			for key, expected := range untouched {
				if key == (issueKey{"task", "unit"}) {
					expected.Version++
				}
				require.Equal(t, expected, f.states[key])
			}
			require.Contains(t, f.logs, logKey{issueKey{"task", "unit"}, original.LastChangedBuildID, status})
			require.Len(t, f.events, 1)
			require.Equal(t, 1, f.failures["issue/list"])
			require.Equal(t, 1, f.failures["build-issues/list"])
			require.Equal(t, 1, f.failures["build-issues/get"])
			for _, log := range f.events {
				require.Equal(t, "100", log.RequestID)
				require.Equal(t, testQueue, log.Queue)
				require.Equal(t, map[string]string{requestlog.MetadataKeyBuildID: "build/100"}, log.Metadata)
			}
		})
	}
}

func TestRecoveryFinishesOnlyTheUncreatedCursor(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.reportIssue("build/100", "task", "unit")
	f.reportIssue("build/100", "unprocessed", "unit")
	progress := currentCursor()
	msg := f.message("build/100", false)
	progress.NotificationID = msg.ID
	f.retain(progress)

	require.NoError(t, f.recover(msg, false))
	require.Len(t, f.states, 1)
	issue := f.states[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusOpen, issue.Status)
	require.Equal(t, "build/100", issue.FirstSeenBuildID)
	require.Equal(t, int64(1000), issue.ChangedAtMs)
	require.Len(t, f.logs, 1)
	require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
	request, build := f.requests["100"], f.builds["build/100"]
	require.NoError(t, f.recover(msg, false))
	require.Equal(t, issue, f.states[issueKey{"task", "unit"}])
	require.Len(t, f.events, 1)
	require.Equal(t, request, f.requests["100"])
	require.Equal(t, build, f.builds["build/100"])
}

func TestRecoveryKeepsCompletedClosingBatches(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusSucceeded)
	for i := 0; i < 205; i++ {
		f.seedIssue(fmt.Sprintf("task-%03d", i), "unit", entity.IssueStatusOpen)
	}
	f.reportIssue("build/100", "reported", "unit")
	msg := f.message("build/100", true)
	require.NoError(t, f.deliver(msg, true))
	require.NoError(t, f.deliver(msg, true))
	require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
	f.failures["issue/list"] = 1
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)

	accepted := make(map[issueKey]entity.IssueState, len(f.states))
	closed := 0
	for key, state := range f.states {
		accepted[key] = state
		if state.Status == entity.IssueStatusClosed {
			closed++
		}
	}
	require.Equal(t, 99, closed)
	cursor := issueKey{f.progress.CursorTaskID, f.progress.CursorUnitID}
	require.NoError(t, f.recover(msg, false))
	for key, expected := range accepted {
		if key == cursor {
			expected.Version++
		}
		require.Equal(t, expected, f.states[key])
	}
	require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
}

func TestRecoveryDoesNotReopenAnExistingClosedCursor(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.seedIssue("task", "unit", entity.IssueStatusClosed)
	f.reportIssue("build/100", "task", "unit")
	progress := currentCursor()
	msg := f.message("build/100", false)
	progress.NotificationID = msg.ID
	f.retain(progress)

	require.NoError(t, f.recover(msg, false))
	require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", "unit"}].Status)
	require.Equal(t, "older", f.states[issueKey{"task", "unit"}].FirstSeenBuildID)
	require.NotContains(t, f.logs, logKey{issueKey{"task", "unit"}, "build/100", entity.IssueStatusOpen})
}

func TestRecoveryRetriesOnlyRecoveryWork(t *testing.T) {
	for _, step := range []string{"build/get", "request/get", "progress/get", "progress/update", "issue/get", "issue/update", "history/create", "recovery/history", "finish abandonment"} {
		t.Run(step, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusFailed)
			f.seedIssue("task", "unit", entity.IssueStatusClosed)
			progress := currentCursor()
			msg := f.message("build/100", true)
			progress.Finalize, progress.NotificationID = true, msg.ID
			f.retain(progress)
			if step == "finish abandonment" {
				f.failOnPhase = entity.IssueTrackingPhaseAbandoned
			} else {
				f.failures[step] = 1
			}
			require.ErrorIs(t, f.recover(msg, false), assert.AnError)
			require.NotEqual(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)

			require.NoError(t, f.recover(msg, false))
			require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
			require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"task", "unit"}].Status)
			require.Len(t, f.events, 1)
			require.NoError(t, f.recover(msg, false))
			require.Len(t, f.events, 1)
		})
	}
}

func TestRecoveryRetainsAnOpeningAcrossRetries(t *testing.T) {
	for _, step := range []string{"issue/create", "history/create", "recovery/history"} {
		t.Run(step, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusRunning)
			progress := currentCursor()
			msg := f.message("build/100", false)
			progress.NotificationID = msg.ID
			f.retain(progress)
			f.failures[step] = 1
			require.ErrorIs(t, f.recover(msg, false), assert.AnError)
			f.dlq.controller.nowMs = func() int64 { return 2000 }

			require.NoError(t, f.recover(msg, false))
			issue := f.states[issueKey{"task", "unit"}]
			log := f.logs[logKey{issueKey{"task", "unit"}, "build/100", entity.IssueStatusOpen}]
			require.Equal(t, "build/100", issue.FirstSeenBuildID)
			require.Equal(t, issue.ChangedAtMs, log.TimestampMs)
			if step == "issue/create" {
				require.Equal(t, int64(2000), issue.ChangedAtMs)
			} else {
				require.Equal(t, int64(1000), issue.ChangedAtMs)
			}
		})
	}
}

func TestPrimaryWaitsForRecoveryThenAdmitsANewerBuild(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusFailed)
	f.source("build/101", "101", entity.BuildStatusRunning)
	f.reportIssue("build/101", "new", "unit")
	progress := currentCursor()
	failed := f.message("build/100", true)
	progress.Finalize, progress.NotificationID = true, failed.ID
	f.retain(progress)
	f.failures["issue/get"] = 1
	require.ErrorIs(t, f.recover(failed, false), assert.AnError)
	require.Equal(t, entity.IssueTrackingPhaseAbandoning, f.progress.Phase)

	newer := f.message("build/101", false)
	delivery := consumermock.NewMockDelivery(f.mocks)
	delivery.EXPECT().Message().Return(newer)
	delivery.EXPECT().Hold(_recoveryDelayMs)
	require.NoError(t, f.controller.Process(context.Background(), delivery))
	require.Empty(t, f.states)

	require.NoError(t, f.recover(failed, false))
	require.NoError(t, f.deliver(newer, false))
	require.Equal(t, "build/101", f.progress.BuildID)
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"new", "unit"}].Status)
	require.NoError(t, f.recover(failed, false))
	require.Equal(t, "build/101", f.progress.BuildID)
}

func TestRecoveryOfAHelperAbandonsTheSavedPassFirst(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	f.source("build/101", "101", entity.BuildStatusFailed)
	f.retain(currentCursor())
	msg := f.message("build/101", true)

	require.NoError(t, f.recover(msg, true))
	require.Equal(t, "build/100", f.progress.BuildID)
	require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
	require.NoError(t, f.recover(msg, false))
	require.Equal(t, "build/101", f.progress.BuildID)
	require.True(t, f.progress.Finalize)
	require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
	require.Len(t, f.events, 2)
}

func TestTerminalNotificationCanFollowAnAbandonedRefresh(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "abandoned refresh", true: "abandoned terminal pass"}[terminal], func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusSucceeded)
			f.seedIssue("previous", "unit", entity.IssueStatusOpen)
			f.reportIssue("build/100", "task", "unit")
			progress := currentCursor()
			failed := f.message("build/100", terminal)
			progress.Finalize, progress.NotificationID = terminal, failed.ID
			f.retain(progress)
			require.NoError(t, f.recover(failed, false))

			final := f.message("build/100", true)
			if terminal {
				require.NoError(t, f.deliver(final, false))
				require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
				require.Equal(t, entity.IssueStatusOpen, f.states[issueKey{"previous", "unit"}].Status)
			} else {
				require.NoError(t, f.deliver(final, true))
				require.NoError(t, f.deliver(final, false))
				require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
				require.Equal(t, entity.IssueStatusClosed, f.states[issueKey{"previous", "unit"}].Status)
			}
		})
	}
}

func TestRecoveryBeforeAStateWasCreated(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	msg := f.message("build/100", false)
	require.NoError(t, f.recover(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
	require.Empty(t, f.states)
	old := currentCursor()
	require.ErrorIs(t, f.store.GetIssueTrackingStore().Create(context.Background(), old), storage.ErrAlreadyExists)
	require.NoError(t, f.deliver(msg, false))
	require.Equal(t, entity.IssueTrackingPhaseAbandoned, f.progress.Phase)
}

func TestRecoveryHistorySupportsLongNotificationIDs(t *testing.T) {
	f := newRecoveryFixture(t)
	f.source("build/100", "100", entity.BuildStatusRunning)
	msg := f.message("build/100", false)
	msg.ID = strings.Repeat("n", 1020)
	require.NoError(t, f.recover(msg, false))
	require.NoError(t, f.recover(msg, false))
	require.Len(t, f.events, 1)
	for _, log := range f.events {
		require.LessOrEqual(t, len(log.ID), 255)
	}
}

func TestRecoveryIgnoresCompletedAndUnrelatedPasses(t *testing.T) {
	for _, scenario := range []string{"completed refresh", "completed terminal pass", "newer build", "different refresh"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusFailed)
			msg := f.message("build/100", false)
			progress := currentCursor()
			progress.NotificationID = msg.ID
			switch scenario {
			case "completed refresh":
				progress.Phase = entity.IssueTrackingPhaseRefreshed
			case "completed terminal pass":
				progress.Phase, progress.Finalize = entity.IssueTrackingPhaseFinalized, true
			case "newer build":
				progress.RequestID, progress.BuildID, progress.NotificationID = "101", "build/101", "newer"
			case "different refresh":
				progress.NotificationID = "another-notification"
			}
			f.retain(progress)
			require.NoError(t, f.recover(msg, false))
			require.Equal(t, progress, f.progress)
			require.Empty(t, f.events)
			require.Empty(t, f.states)
		})
	}
}

func TestRecoveryAcknowledgesUnusableMessages(t *testing.T) {
	for _, scenario := range []string{"payload", "queue", "partition", "message ID", "missing build", "missing request", "request queue", "request ID"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.source("build/100", "100", entity.BuildStatusFailed)
			msg := f.message("build/100", true)
			switch scenario {
			case "payload":
				msg.Payload = []byte("invalid")
			case "queue":
				msg.Tenant = "another-queue"
			case "partition":
				msg.PartitionKey = "another-queue"
			case "message ID":
				msg.ID = ""
			case "missing build":
				delete(f.builds, "build/100")
			case "missing request":
				delete(f.requests, "100")
			case "request queue":
				request := f.requests["100"]
				request.Queue = "another-queue"
				f.requests["100"] = request
			case "request ID":
				request := f.requests["100"]
				request.ID = "invalid"
				f.requests["100"] = request
			}
			require.NoError(t, f.recover(msg, false))
			require.False(t, f.hasProgress)
			require.Empty(t, f.states)
		})
	}
}
