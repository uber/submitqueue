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
	"sort"
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

type logKey struct {
	issueKey
	build string
	event entity.IssueEvent
}

type fixture struct {
	failOnPhase entity.IssueTrackingPhase
	t           *testing.T
	mocks       *gomock.Controller
	controller  *Controller
	factory     *storagemock.MockFactory
	store       storage.Storage
	states      map[issueKey]entity.IssueState
	logs        map[logKey]entity.IssueLog
	progress    entity.IssueTrackingState
	hasProgress bool
	failures    map[string]int
	initialLogs map[logKey]entity.IssueLog
	builds      map[string]entity.Build
	requests    map[string]entity.Request
	buildIssues map[string]map[issueKey]entity.BuildIssue
	sequence    int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, mocks: gomock.NewController(t), states: map[issueKey]entity.IssueState{}, logs: map[logKey]entity.IssueLog{}, failures: map[string]int{}, initialLogs: map[logKey]entity.IssueLog{}}
	f.factory = storagemock.NewMockFactory(f.mocks)
	f.builds = map[string]entity.Build{}
	f.requests = map[string]entity.Request{}
	f.buildIssues = map[string]map[issueKey]entity.BuildIssue{}
	f.source("older", "1", entity.BuildStatusSucceeded)
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
		issue, ok := f.states[issueKey{task, unit}]
		if !ok {
			return entity.IssueState{}, storage.ErrNotFound
		}
		return issue, nil
	}).AnyTimes()
	states.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.IssueState) error {
		if err := f.fail("issue/create"); err != nil {
			return err
		}
		key := issueKey{issue.TaskID, issue.UnitID}
		if _, ok := f.states[key]; ok {
			return storage.ErrAlreadyExists
		}
		f.states[key] = issue
		return nil
	}).AnyTimes()
	states.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.IssueState, oldVersion, newVersion int32) error {
		if err := f.fail("issue/update"); err != nil {
			return err
		}
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
		if err := f.fail("history/create"); err != nil {
			return err
		}
		key := logKey{issueKey{log.TaskID, log.UnitID}, log.BuildID, log.Event}
		if _, ok := f.initialLogs[key]; ok {
			return storage.ErrAlreadyExists
		}
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
		if !f.hasProgress {
			return entity.IssueTrackingState{}, storage.ErrNotFound
		}
		return f.progress, nil
	}).AnyTimes()
	tracking.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, state entity.IssueTrackingState) error {
		if err := f.fail("progress/create"); err != nil {
			return err
		}
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
		if f.failOnPhase != "" && state.Phase == f.failOnPhase {
			f.failOnPhase = ""
			return assert.AnError
		}
		if !f.hasProgress || f.progress.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		state.Version = newVersion
		f.progress = state
		return nil
	}).AnyTimes()

	builds := storagemock.NewMockBuildStore(f.mocks)
	requests := storagemock.NewMockRequestStore(f.mocks)
	buildIssues := storagemock.NewMockBuildIssueStore(f.mocks)
	store.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
	store.EXPECT().GetBuildIssueStore().Return(buildIssues).AnyTimes()
	builds.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.Build, error) {
		if err := f.fail("build/get"); err != nil {
			return entity.Build{}, err
		}
		value, ok := f.builds[id]
		if !ok {
			return entity.Build{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()
	requests.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.Request, error) {
		if err := f.fail("request/get"); err != nil {
			return entity.Request{}, err
		}
		value, ok := f.requests[id]
		if !ok {
			return entity.Request{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()
	buildIssues.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, build, task, unit string) (entity.BuildIssue, error) {
		if err := f.fail("build-issues/get"); err != nil {
			return entity.BuildIssue{}, err
		}
		value, ok := f.buildIssues[build][issueKey{task, unit}]
		if !ok {
			return entity.BuildIssue{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()
	buildIssues.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, build string, bounds storage.BuildIssueRange) ([]entity.BuildIssue, error) {
		if err := f.fail("build-issues/list"); err != nil {
			return nil, err
		}
		keys := make([]issueKey, 0)
		for key := range f.buildIssues[build] {
			if key.task > bounds.After.TaskID || (key.task == bounds.After.TaskID && key.unit > bounds.After.UnitID) {
				keys = append(keys, key)
			}
		}
		sortKeys(keys)
		if len(keys) > bounds.Limit {
			keys = keys[:bounds.Limit]
		}
		result := make([]entity.BuildIssue, 0, len(keys))
		for _, key := range keys {
			result = append(result, f.buildIssues[build][key])
		}
		return result, nil
	}).AnyTimes()
	states.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, bounds storage.IssueStateRange) ([]entity.IssueState, error) {
		if err := f.fail("issue/list"); err != nil {
			return nil, err
		}
		keys := make([]issueKey, 0)
		for key := range f.states {
			if key.task > bounds.After.TaskID || (key.task == bounds.After.TaskID && key.unit > bounds.After.UnitID) {
				keys = append(keys, key)
			}
		}
		sortKeys(keys)
		if len(keys) > bounds.Limit {
			keys = keys[:bounds.Limit]
		}
		result := make([]entity.IssueState, 0, len(keys))
		for _, key := range keys {
			result = append(result, f.states[key])
		}
		return result, nil
	}).AnyTimes()
	f.controller = NewController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, messagequeue.TopicKeyIssueTrack, "test")
	f.controller.nowMs = func() int64 { return 1000 }
	return f
}

func (f *fixture) fail(name string) error {
	if f.failures[name] > 0 {
		f.failures[name]--
		return assert.AnError
	}
	return nil
}

func currentCursor() entity.IssueTrackingState {
	return entity.IssueTrackingState{RequestID: "100", BuildID: "build/100", NotificationID: "notice/100", CursorTaskID: "task", CursorUnitID: "unit", Phase: entity.IssueTrackingPhaseRefreshing, Version: 1}
}

func (f *fixture) retain(progress entity.IssueTrackingState) {
	f.t.Helper()
	require.NoError(f.t, progress.Validate())
	f.progress, f.hasProgress = progress, true
}

func sortKeys(keys []issueKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].task != keys[j].task {
			return keys[i].task < keys[j].task
		}
		return keys[i].unit < keys[j].unit
	})
}

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
	f.states[issueKey{task, unit}] = entity.IssueState{TaskID: task, UnitID: unit, Status: status, FirstSeenBuildID: "older", Version: 7, LastProcessedBuildID: "older", LastChangedBuildID: "older", ChangedAtMs: 500}
	event := entity.IssueEventOpened
	if status == entity.IssueStatusClosed {
		event = entity.IssueEventResolved
	}
	f.initialLogs[logKey{issueKey{task, unit}, "older", event}] = entity.IssueLog{TaskID: task, UnitID: unit, BuildID: "older", Event: event, TimestampMs: 500}
}
