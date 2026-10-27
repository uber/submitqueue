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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestPausedCreateCannotReopenAnIssueAfterNewerClosure(t *testing.T) {
	f := newFixture(t)
	old := openingChange()
	f.retain(old)
	paused, resume := make(chan struct{}), make(chan struct{})
	var intercepted atomic.Bool
	f.beforeCreate = func() {
		if intercepted.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}
	result := make(chan error, 1)
	go func() { _, err := f.apply(old); result <- err }()
	<-paused

	next, err := f.apply(old)
	require.NoError(t, err)
	closing := next
	closing.Version = next.Version + 2
	closing.RequestID = "101"
	closing.BuildID = "build/101"
	closing.NotificationID = "notice/101"
	closing.Phase = entity.IssueTrackingPhaseFinalizing
	closing.CursorTaskID, closing.CursorUnitID = "task", "unit"
	closing.PlannedChange = entity.PlannedIssueChange{Status: entity.IssueStatusClosed, FirstSeenBuildID: "build/100", ExpectedVersion: 1, TimestampMs: 2000}
	f.retain(closing)
	_, err = f.apply(closing)
	require.NoError(t, err)

	close(resume)
	require.ErrorIs(t, <-result, storage.ErrVersionMismatch)
	issue := f.states[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusClosed, issue.Status)
	require.Equal(t, int32(2), issue.Version)
	require.Len(t, f.logs, 2)
	require.Equal(t, "build/101", f.progress.BuildID)
}

func TestPausedCloseCannotOverwriteANewerOpening(t *testing.T) {
	f := newFixture(t)
	old := openingChange()
	old.Phase = entity.IssueTrackingPhaseFinalizing
	old.PlannedChange.Status = entity.IssueStatusClosed
	old.PlannedChange.FirstSeenBuildID = "older"
	old.PlannedChange.ExpectedVersion = 1
	f.states[issueKey{"task", "unit"}] = entity.IssueState{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "older", Version: 1}
	f.retain(old)
	paused, resume := make(chan struct{}), make(chan struct{})
	var intercepted atomic.Bool
	f.beforeUpdate = func() {
		if intercepted.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}
	result := make(chan error, 1)
	go func() { _, err := f.apply(old); result <- err }()
	<-paused

	_, err := f.apply(old)
	require.NoError(t, err)
	reopening := openingChange()
	reopening.RequestID = "101"
	reopening.BuildID = "build/101"
	reopening.NotificationID = "notice/101"
	reopening.Version = f.progress.Version + 2
	reopening.PlannedChange.FirstSeenBuildID = "build/101"
	reopening.PlannedChange.ExpectedVersion = 2
	reopening.PlannedChange.TimestampMs = 2000
	f.retain(reopening)
	_, err = f.apply(reopening)
	require.NoError(t, err)

	close(resume)
	require.ErrorIs(t, <-result, storage.ErrVersionMismatch)
	issue := f.states[issueKey{"task", "unit"}]
	require.Equal(t, entity.IssueStatusOpen, issue.Status)
	require.Equal(t, int32(3), issue.Version)
	require.Equal(t, "build/101", issue.FirstSeenBuildID)
	require.Len(t, f.logs, 2)
}

func TestLateHistoryWriteCannotDuplicateAnAcceptedTransition(t *testing.T) {
	f := newFixture(t)
	old := openingChange()
	f.retain(old)
	paused, resume := make(chan struct{}), make(chan struct{})
	var intercepted atomic.Bool
	f.beforeHistory = func() {
		if intercepted.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}
	result := make(chan error, 1)
	go func() { _, err := f.apply(old); result <- err }()
	<-paused

	_, err := f.apply(old)
	require.NoError(t, err)
	close(resume)
	require.ErrorIs(t, <-result, storage.ErrVersionMismatch)
	require.Len(t, f.logs, 1)
	require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", "unit"}, "build/100", entity.IssueEventOpened}].TimestampMs)
	require.False(t, f.progress.HasPlannedChange())
	require.Equal(t, int32(1), f.states[issueKey{"task", "unit"}].Version)
}
