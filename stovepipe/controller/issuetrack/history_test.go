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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/zap"
)

func (f *fixture) recoverHistory(msg entityqueue.Message, original failure.Failure) error {
	f.t.Helper()
	c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, f.materializer, messagequeue.TopicKeyIssueTrack, "recovery")
	d := consumermock.NewMockDelivery(f.mocks)
	d.EXPECT().Message().Return(msg)
	d.EXPECT().Failure().Return(original, true).AnyTimes()
	return c.Process(context.Background(), d)
}

func TestReconciliationHistoryClosureModes(t *testing.T) {
	for _, mode := range []string{"reconciled", "suppressed_cancelled", "suppressed_task_wide_issue"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			status := entity.BuildStatusFailed
			if mode == "suppressed_cancelled" {
				status = entity.BuildStatusCancelled
			}
			f.source("build", "10", status)
			f.seedIssue("old", "unit", entity.IssueStatusOpen)
			if mode == "suppressed_task_wide_issue" {
				f.reportIssue("build", "task", "")
			}
			msg := f.message("build", true)
			require.NoError(t, f.deliver(msg, mode == "reconciled"))
			if mode == "reconciled" {
				require.NoError(t, f.deliver(msg, false))
			}
			require.Len(t, f.requestHistory, 1)
			log := f.requestHistory["10:event/issues_reconciled/build"]
			require.Equal(t, entity.RequestEventIssuesReconciled, log.Event)
			require.Equal(t, mode, log.Metadata[requestlog.MetadataKeyClosureMode])
			expectedStatus := entity.IssueStatusOpen
			if mode == "reconciled" {
				expectedStatus = entity.IssueStatusClosed
			}
			require.Equal(t, expectedStatus, f.states[issueKey{"old", "unit"}].Status)
			require.NoError(t, f.deliver(msg, false))
			require.Equal(t, log, f.requestHistory["10:event/issues_reconciled/build"])
		})
	}
}

func TestCompletedReconciliationRepairsHistoryBeforeReplacingCheckpoint(t *testing.T) {
	f := newFixture(t)
	f.source("old", "10", entity.BuildStatusSucceeded)
	msg := f.message("old", true)
	require.NoError(t, f.deliver(msg, true))
	f.failures["request-history/create"] = 1
	require.ErrorIs(t, f.deliver(msg, false), assert.AnError)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Empty(t, f.requestHistory)
	completed := f.progress
	f.source("new", "11", entity.BuildStatusRunning)
	newer := f.message("new", false)
	f.failures["request-history/create"] = 1
	err := f.deliver(newer, false)
	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, completed, f.progress)
	require.NoError(t, f.deliver(newer, false))
	require.Equal(t, "new", f.progress.BuildID)
	require.Contains(t, f.requestHistory, "10:event/issues_reconciled/old")
	require.Len(t, f.requestHistory, 1) // A nonterminal refresh adds no event.
}

func TestIssueTrackingDLQRepairsCompletedPassInsteadOfAbandoning(t *testing.T) {
	f := newFixture(t)
	f.source("build", "10", entity.BuildStatusSucceeded)
	msg := f.message("build", true)
	require.NoError(t, f.deliver(msg, true))
	f.failures["request-history/create"] = 1
	err := f.deliver(msg, false)
	require.ErrorIs(t, err, assert.AnError)
	checkpoint := f.progress
	require.NoError(t, f.recoverHistory(msg, failure.Failure{}))
	require.Equal(t, checkpoint, f.progress)
	require.Len(t, f.requestHistory, 1)
	require.Contains(t, f.requestHistory, "10:event/issues_reconciled/build")
}
