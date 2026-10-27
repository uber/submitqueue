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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const testQueue = "validation/main"

type issueKey struct{ task, unit string }
type logKey struct {
	issueKey
	build string
	event entity.IssueEvent
}

type fixture struct {
	mu            sync.Mutex
	t             *testing.T
	mocks         *gomock.Controller
	controller    *Controller
	factory       *storagemock.MockFactory
	store         storage.Storage
	states        map[issueKey]entity.IssueState
	logs          map[logKey]entity.IssueLog
	progress      entity.IssueTrackingState
	hasProgress   bool
	failures      map[string]int
	beforeCreate  func()
	beforeUpdate  func()
	beforeHistory func()
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, mocks: gomock.NewController(t), states: map[issueKey]entity.IssueState{}, logs: map[logKey]entity.IssueLog{}, failures: map[string]int{}}
	f.factory = storagemock.NewMockFactory(f.mocks)
	store := storagemock.NewMockStorage(f.mocks)
	f.store = store
	states := storagemock.NewMockIssueStateStore(f.mocks)
	history := storagemock.NewMockIssueLogStore(f.mocks)
	tracking := storagemock.NewMockIssueTrackingStore(f.mocks)
	f.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(store, nil).AnyTimes()
	store.EXPECT().GetIssueStateStore().Return(states).AnyTimes()
	store.EXPECT().GetIssueLogStore().Return(history).AnyTimes()
	store.EXPECT().GetIssueTrackingStore().Return(tracking).AnyTimes()

	states.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, task, unit string) (entity.IssueState, error) {
		if err := f.fail("issue/get"); err != nil {
			return entity.IssueState{}, err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		issue, ok := f.states[issueKey{task, unit}]
		if !ok {
			return entity.IssueState{}, storage.ErrNotFound
		}
		return issue, nil
	}).AnyTimes()
	states.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.IssueState) error {
		if f.beforeCreate != nil {
			f.beforeCreate()
		}
		if err := f.fail("issue/create"); err != nil {
			return err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		key := issueKey{issue.TaskID, issue.UnitID}
		if _, ok := f.states[key]; ok {
			return storage.ErrAlreadyExists
		}
		f.states[key] = issue
		return nil
	}).AnyTimes()
	states.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.IssueState, oldVersion, newVersion int32) error {
		if f.beforeUpdate != nil {
			f.beforeUpdate()
		}
		if err := f.fail("issue/update"); err != nil {
			return err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		key := issueKey{issue.TaskID, issue.UnitID}
		current, ok := f.states[key]
		if !ok || current.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		issue.Version = newVersion
		f.states[key] = issue
		return nil
	}).AnyTimes()
	history.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.IssueLog) error {
		if f.beforeHistory != nil {
			f.beforeHistory()
		}
		if err := f.fail("history/create"); err != nil {
			return err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		key := logKey{issueKey{log.TaskID, log.UnitID}, log.BuildID, log.Event}
		if _, ok := f.logs[key]; ok {
			return storage.ErrAlreadyExists
		}
		f.logs[key] = log
		return nil
	}).AnyTimes()
	tracking.EXPECT().Get(gomock.Any()).DoAndReturn(func(context.Context) (entity.IssueTrackingState, error) {
		if err := f.fail("progress/get"); err != nil {
			return entity.IssueTrackingState{}, err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if !f.hasProgress {
			return entity.IssueTrackingState{}, storage.ErrNotFound
		}
		return f.progress, nil
	}).AnyTimes()
	tracking.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, state entity.IssueTrackingState) error {
		if err := f.fail("progress/create"); err != nil {
			return err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.hasProgress {
			return storage.ErrAlreadyExists
		}
		f.progress = state
		f.hasProgress = true
		return nil
	}).AnyTimes()
	tracking.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, state entity.IssueTrackingState, oldVersion, newVersion int32) error {
		if err := f.fail("progress/update"); err != nil {
			return err
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if !f.hasProgress || f.progress.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		state.Version = newVersion
		f.progress = state
		return nil
	}).AnyTimes()
	f.controller = NewController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, messagequeue.TopicKeyIssueTrack, "test")
	return f
}

func (f *fixture) fail(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures[name] > 0 {
		f.failures[name]--
		return assert.AnError
	}
	return nil
}

func openingChange() entity.IssueTrackingState {
	return entity.IssueTrackingState{RequestID: "100", BuildID: "build/100", NotificationID: "notice/100", CursorTaskID: "task", CursorUnitID: "unit", Phase: entity.IssueTrackingPhaseRefreshing, Version: 1,
		PlannedChange: entity.PlannedIssueChange{Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/100", TimestampMs: 1000}}
}

func (f *fixture) retain(progress entity.IssueTrackingState) {
	f.t.Helper()
	require.NoError(f.t, progress.Validate())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress = progress
	f.hasProgress = true
}

func (f *fixture) apply(progress entity.IssueTrackingState) (entity.IssueTrackingState, error) {
	f.t.Helper()
	return f.controller.applyPlannedChange(context.Background(), f.store, progress)
}

func TestApplyPlannedIssueChanges(t *testing.T) {
	for _, name := range []string{"create", "close", "reopen"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			progress := openingChange()
			switch name {
			case "close":
				f.states[issueKey{"task", "unit"}] = entity.IssueState{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "older", Version: 7}
				progress.Phase = entity.IssueTrackingPhaseFinalizing
				progress.PlannedChange.Status = entity.IssueStatusClosed
				progress.PlannedChange.FirstSeenBuildID = "older"
				progress.PlannedChange.ExpectedVersion = 7
			case "reopen":
				f.states[issueKey{"task", "unit"}] = entity.IssueState{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusClosed, FirstSeenBuildID: "older", Version: 7}
				progress.PlannedChange.ExpectedVersion = 7
			}
			f.retain(progress)
			before := progress
			next, err := f.apply(progress)
			require.NoError(t, err)
			require.Equal(t, before, progress)
			require.Equal(t, next, f.progress)
			require.False(t, next.HasPlannedChange())
			require.Equal(t, int32(2), next.Version)
			require.Equal(t, "task", next.CursorTaskID)
			require.Equal(t, "unit", next.CursorUnitID)
			issue := f.states[issueKey{"task", "unit"}]
			require.Equal(t, progress.PlannedChange.Status, issue.Status)
			require.Equal(t, progress.PlannedChange.ExpectedVersion+1, issue.Version)
			require.Equal(t, progress.PlannedChange.FirstSeenBuildID, issue.FirstSeenBuildID)
			event := entity.IssueEventOpened
			if name == "close" {
				event = entity.IssueEventResolved
			}
			require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", "unit"}, "build/100", event}].TimestampMs)
		})
	}
}

func TestApplyPlannedChangeRetriesPartialWrites(t *testing.T) {
	for _, operation := range []string{"issue/create", "issue/update", "history/create", "progress/update"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			progress := openingChange()
			if operation == "issue/update" {
				progress.PlannedChange.ExpectedVersion = 3
				f.states[issueKey{"task", "unit"}] = entity.IssueState{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusClosed, FirstSeenBuildID: "older", Version: 3}
			}
			f.retain(progress)
			f.failures[operation] = 1
			_, err := f.apply(progress)
			require.ErrorIs(t, err, assert.AnError)
			require.Equal(t, progress, f.progress)
			next, err := f.apply(progress)
			require.NoError(t, err)
			require.False(t, next.HasPlannedChange())
			require.Len(t, f.logs, 1)
			require.Equal(t, progress.PlannedChange.ExpectedVersion+1, f.states[issueKey{"task", "unit"}].Version)
		})
	}
}

func TestApplyPlannedChangeRejectsDifferentStoredResults(t *testing.T) {
	for _, name := range []string{"newer version", "different opening", "different status"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			progress := openingChange()
			f.retain(progress)
			existing := entity.IssueState{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/100", Version: 1}
			switch name {
			case "newer version":
				existing.Version = 2
			case "different opening":
				existing.FirstSeenBuildID = "other"
			case "different status":
				existing.Status = entity.IssueStatusClosed
			}
			f.states[issueKey{"task", "unit"}] = existing
			_, err := f.apply(progress)
			require.ErrorIs(t, err, storage.ErrVersionMismatch)
			require.Equal(t, existing, f.states[issueKey{"task", "unit"}])
			require.Empty(t, f.logs)
			require.Equal(t, progress, f.progress)
		})
	}
}

func TestApplyPlannedChangeRejectsMissingOrInvalidWork(t *testing.T) {
	f := newFixture(t)
	progress := openingChange()
	progress.PlannedChange = entity.PlannedIssueChange{}
	_, err := f.apply(progress)
	require.Error(t, err)
	progress = openingChange()
	progress.PlannedChange.ExpectedVersion = -1
	_, err = f.apply(progress)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.controller.applyPlannedChange(ctx, f.store, openingChange())
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.states)
	require.Empty(t, f.logs)
}

func TestPlannedChangeWaitsForReplayVerification(t *testing.T) {
	f := newFixture(t)
	progress := openingChange()
	f.retain(progress)
	f.states[issueKey{"task", "unit"}] = entity.IssueState{TaskID: "task", UnitID: "unit", Status: entity.IssueStatusOpen, FirstSeenBuildID: "build/100", Version: 1}
	f.failures["issue/get"] = 1
	_, err := f.apply(progress)
	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, progress, f.progress)
	require.Empty(t, f.logs)
	_, err = f.apply(progress)
	require.NoError(t, err)
	require.Len(t, f.logs, 1)
}
