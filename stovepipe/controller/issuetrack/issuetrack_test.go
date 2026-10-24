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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/publish"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
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
	t            *testing.T
	ctrl         *gomock.Controller
	controller   *Controller
	builds       map[string]entity.Build
	requests     map[string]entity.Request
	observations map[string]map[issueKey]entity.BuildIssue
	opens        map[issueKey]entity.OpenIssue
	logs         map[logKey]entity.IssueLog
	progress     entity.IssueTrackingState
	hasProgress  bool
	failures     map[string]int
	createRace   bool
	updateRace   bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	f := &fixture{
		t:            t,
		ctrl:         gomock.NewController(t),
		builds:       map[string]entity.Build{},
		requests:     map[string]entity.Request{},
		observations: map[string]map[issueKey]entity.BuildIssue{},
		opens:        map[issueKey]entity.OpenIssue{},
		logs:         map[logKey]entity.IssueLog{},
		failures:     map[string]int{},
	}

	stores := storagemock.NewMockFactory(f.ctrl)
	store := storagemock.NewMockStorage(f.ctrl)
	builds := storagemock.NewMockBuildStore(f.ctrl)
	requests := storagemock.NewMockRequestStore(f.ctrl)
	observations := storagemock.NewMockBuildIssueStore(f.ctrl)
	opens := storagemock.NewMockOpenIssueStore(f.ctrl)
	logs := storagemock.NewMockIssueLogStore(f.ctrl)
	progress := storagemock.NewMockIssueTrackingStore(f.ctrl)

	stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(store, nil).AnyTimes()
	store.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
	store.EXPECT().GetBuildIssueStore().Return(observations).AnyTimes()
	store.EXPECT().GetOpenIssueStore().Return(opens).AnyTimes()
	store.EXPECT().GetIssueLogStore().Return(logs).AnyTimes()
	store.EXPECT().GetIssueTrackingStore().Return(progress).AnyTimes()

	builds.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key string) (entity.Build, error) {
		value, ok := f.builds[key]
		if !ok {
			return entity.Build{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()

	requests.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key string) (entity.Request, error) {
		value, ok := f.requests[key]
		if !ok {
			return entity.Request{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()

	observations.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, buildID string, bounds storage.BuildIssueRange) ([]entity.BuildIssue, error) {
		if err := f.fail("observations/list"); err != nil {
			return nil, err
		}
		keys := make([]issueKey, 0)
		for key := range f.observations[buildID] {
			if bounds.After.TaskID == "" || key.task > bounds.After.TaskID || (key.task == bounds.After.TaskID && key.unit > bounds.After.UnitID) {
				keys = append(keys, key)
			}
		}
		sortKeys(keys)
		if len(keys) > bounds.Limit {
			keys = keys[:bounds.Limit]
		}
		result := make([]entity.BuildIssue, 0, len(keys))
		for _, key := range keys {
			result = append(result, f.observations[buildID][key])
		}
		return result, nil
	}).AnyTimes()

	observations.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, buildID, taskID, unitID string) (entity.BuildIssue, error) {
		if err := f.fail("observations/get"); err != nil {
			return entity.BuildIssue{}, err
		}
		value, ok := f.observations[buildID][issueKey{taskID, unitID}]
		if !ok {
			return entity.BuildIssue{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()

	opens.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, taskID, unitID string) (entity.OpenIssue, error) {
		value, ok := f.opens[issueKey{taskID, unitID}]
		if !ok {
			return entity.OpenIssue{}, storage.ErrNotFound
		}
		return value, nil
	}).AnyTimes()

	opens.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.OpenIssue) error {
		if err := f.fail("opens/create"); err != nil {
			return err
		}
		key := issueKey{issue.TaskID, issue.UnitID}
		if _, ok := f.opens[key]; ok {
			return storage.ErrAlreadyExists
		}
		f.opens[key] = issue
		return nil
	}).AnyTimes()

	opens.EXPECT().Delete(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.OpenIssue) error {
		if err := f.fail("opens/delete"); err != nil {
			return err
		}
		key := issueKey{issue.TaskID, issue.UnitID}
		if f.opens[key].FirstSeenBuildID == issue.FirstSeenBuildID {
			delete(f.opens, key)
		}
		return nil
	}).AnyTimes()

	opens.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, bounds storage.OpenIssueRange) ([]entity.OpenIssue, error) {
		if err := f.fail("opens/list"); err != nil {
			return nil, err
		}
		keys := make([]issueKey, 0)
		for key := range f.opens {
			if bounds.After.TaskID == "" || key.task > bounds.After.TaskID || (key.task == bounds.After.TaskID && key.unit > bounds.After.UnitID) {
				keys = append(keys, key)
			}
		}
		sortKeys(keys)
		if len(keys) > bounds.Limit {
			keys = keys[:bounds.Limit]
		}
		result := make([]entity.OpenIssue, 0, len(keys))
		for _, key := range keys {
			result = append(result, f.opens[key])
		}
		return result, nil
	}).AnyTimes()

	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.IssueLog) error {
		if err := f.fail("logs/" + string(log.Event)); err != nil {
			return err
		}
		key := logKey{issueKey{log.TaskID, log.UnitID}, log.BuildID, log.Event}
		if _, ok := f.logs[key]; ok {
			return storage.ErrAlreadyExists
		}
		f.logs[key] = log
		return nil
	}).AnyTimes()

	progress.EXPECT().Get(gomock.Any()).DoAndReturn(func(context.Context) (entity.IssueTrackingState, error) {
		if err := f.fail("progress/get"); err != nil {
			return entity.IssueTrackingState{}, err
		}
		if !f.hasProgress {
			return entity.IssueTrackingState{}, storage.ErrNotFound
		}
		return f.progress, nil
	}).AnyTimes()

	progress.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, state entity.IssueTrackingState) error {
		if err := f.fail("progress/create"); err != nil {
			return err
		}
		if f.createRace {
			f.createRace = false
			f.hasProgress = true
			f.progress = entity.IssueTrackingState{RequestID: "11", BuildID: "build/11", Phase: entity.IssueTrackingPhaseRefreshed, Version: 1}
			return storage.ErrAlreadyExists
		}
		if f.hasProgress {
			return storage.ErrAlreadyExists
		}
		f.progress = state
		f.hasProgress = true
		return nil
	}).AnyTimes()

	progress.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, state entity.IssueTrackingState, oldVersion, newVersion int32) error {
		if state.IsPending() {
			if err := f.fail("progress/start"); err != nil {
				return err
			}
		}
		if f.updateRace && state.IsPending() {
			f.updateRace = false
			f.progress = entity.IssueTrackingState{RequestID: "11", BuildID: "build/11", Phase: entity.IssueTrackingPhaseRefreshed, Version: oldVersion + 1}
			return storage.ErrVersionMismatch
		}
		if !f.hasProgress || f.progress.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		if !state.IsPending() {
			if err := f.fail("progress/complete"); err != nil {
				return err
			}
		}
		state.Version = newVersion
		f.progress = state
		return nil
	}).AnyTimes()

	f.controller = NewController(zap.NewNop().Sugar(), tally.NoopScope, stores, stovepipemq.TopicKeyIssueTrack, "test-issuetrack")
	f.controller.nowMs = func() int64 { return 1000 }
	return f
}

func sortKeys(keys []issueKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].task != keys[j].task {
			return keys[i].task < keys[j].task
		}
		return keys[i].unit < keys[j].unit
	})
}

func (f *fixture) fail(operation string) error {
	if f.failures[operation] > 0 {
		f.failures[operation]--
		return assert.AnError
	}
	return nil
}

func (f *fixture) source(buildID, requestID string, status entity.BuildStatus) {
	f.builds[buildID] = entity.Build{ID: buildID, RequestID: requestID, Status: status, Version: 1}
	request := entity.Request{ID: requestID, Queue: testQueue, State: entity.RequestStateProcessing}
	if status.IsTerminal() {
		request.TerminalBuildID = buildID
		switch status {
		case entity.BuildStatusSucceeded:
			request.State = entity.RequestStateSucceeded
		case entity.BuildStatusCancelled:
			request.State = entity.RequestStateCancelled
		default:
			request.State = entity.RequestStateFailed
		}
	}
	f.requests[requestID] = request
}

func (f *fixture) observe(buildID, task, unit string) {
	if f.observations[buildID] == nil {
		f.observations[buildID] = map[issueKey]entity.BuildIssue{}
	}
	f.observations[buildID][issueKey{task, unit}] = entity.BuildIssue{TaskID: task, UnitID: unit}
}

func (f *fixture) seed(task, unit, buildID string) {
	f.opens[issueKey{task, unit}] = entity.OpenIssue{TaskID: task, UnitID: unit, FirstSeenBuildID: buildID}
	f.logs[logKey{issueKey{task, unit}, buildID, entity.IssueEventOpened}] = entity.IssueLog{TaskID: task, UnitID: unit, BuildID: buildID, Event: entity.IssueEventOpened, TimestampMs: 1}
}

func (f *fixture) message(buildID string, finalize bool) entityqueue.Message {
	f.t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.IssueTrack{BuildId: buildID, QueueName: testQueue, Finalize: finalize})
	require.NoError(f.t, err)
	msg := entityqueue.NewMessage(publish.IntentID(buildID, "issuetrack"), payload, testQueue, nil)
	msg.Tenant = testQueue
	return msg
}

func (f *fixture) process(buildID string, finalize bool) error {
	f.t.Helper()
	return f.deliver(context.Background(), f.message(buildID, finalize))
}

func (f *fixture) deliver(ctx context.Context, msg entityqueue.Message) error {
	f.t.Helper()
	delivery := consumermock.NewMockDelivery(f.ctrl)
	delivery.EXPECT().Message().Return(msg)
	return f.controller.Process(ctx, delivery)
}

func TestRefreshOpensIssuesWithoutResolving(t *testing.T) {
	f := newFixture(t)
	f.source("build/10", "10", entity.BuildStatusRunning)
	f.seed("old", "unit", "build/1")
	f.observe("build/10", "task", "")

	require.NoError(t, f.process("build/10", false))
	require.Len(t, f.opens, 2)
	require.Equal(t, "build/10", f.opens[issueKey{"task", ""}].FirstSeenBuildID)
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
	require.Equal(t, int32(2), f.progress.Version)
	require.Len(t, f.logs, 2)

	f.controller.nowMs = func() int64 { return 2000 }
	f.observe("build/10", "task", "next")

	require.NoError(t, f.process("build/10", false))
	require.Len(t, f.opens, 3)
	require.Len(t, f.logs, 3)
	require.Equal(t, int64(1000), f.logs[logKey{issueKey{"task", ""}, "build/10", entity.IssueEventOpened}].TimestampMs)
}

func TestFinalAnalysisKeepsOpensAndResolves(t *testing.T) {
	f := newFixture(t)
	f.source("build/10", "10", entity.BuildStatusFailed)
	f.seed("a", "unit", "build/1")
	f.seed("b", "unit", "build/1")
	f.observe("build/10", "b", "unit")
	f.observe("build/10", "c", "unit")

	require.NoError(t, f.process("build/10", true))
	require.Len(t, f.opens, 2)
	require.NotContains(t, f.opens, issueKey{"a", "unit"})
	require.Equal(t, "build/1", f.opens[issueKey{"b", "unit"}].FirstSeenBuildID)
	require.Equal(t, "build/10", f.opens[issueKey{"c", "unit"}].FirstSeenBuildID)
	require.Contains(t, f.logs, logKey{issueKey{"a", "unit"}, "build/10", entity.IssueEventResolved})
	require.Len(t, f.logs, 4)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)

	f.observe("build/10", "late", "unit")

	require.NoError(t, f.process("build/10", false))

	require.NoError(t, f.process("build/10", true))
	require.Len(t, f.opens, 2)
	require.Len(t, f.logs, 4)
}

func TestEmptyFinalResultResolvesAcrossBatches(t *testing.T) {
	f := newFixture(t)
	f.source("new", "10", entity.BuildStatusSucceeded)
	for i := 0; i < _issueBatchSize+3; i++ {
		f.seed("task", fmt.Sprintf("unit/%03d", i), "old")
	}

	require.NoError(t, f.process("new", true))
	require.Empty(t, f.opens)
	require.Len(t, f.logs, 2*(_issueBatchSize+3))
	require.True(t, f.hasProgress)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	f.source("older", "9", entity.BuildStatusRunning)
	f.observe("older", "task", "late")

	require.NoError(t, f.process("older", false))
	require.Empty(t, f.opens)
}

func TestRefreshTraversesAllBatches(t *testing.T) {
	f := newFixture(t)
	f.source("build/10", "10", entity.BuildStatusRunning)
	for i := 0; i < _issueBatchSize+3; i++ {
		f.observe("build/10", "task", fmt.Sprintf("unit/%03d", i))
	}

	require.NoError(t, f.process("build/10", false))
	require.Len(t, f.opens, _issueBatchSize+3)
	require.Len(t, f.logs, _issueBatchSize+3)
}

func TestCancelledFinalDoesNotResolve(t *testing.T) {
	f := newFixture(t)
	f.source("cancelled", "10", entity.BuildStatusCancelled)
	f.seed("old", "unit", "build/1")
	f.observe("cancelled", "confirmed", "unit")

	require.NoError(t, f.process("cancelled", true))
	require.Len(t, f.opens, 2)
	require.Len(t, f.logs, 2)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
}

func TestRequestOrderNotBuildIDOrder(t *testing.T) {
	f := newFixture(t)
	f.source("aaa-new", "10", entity.BuildStatusRunning)
	f.source("zzz-old", "9", entity.BuildStatusSucceeded)
	f.observe("aaa-new", "new", "unit")
	f.observe("zzz-old", "old", "unit")

	require.NoError(t, f.process("aaa-new", false))
	before := f.progress

	require.NoError(t, f.process("zzz-old", false))

	require.NoError(t, f.process("zzz-old", true))
	require.Equal(t, before, f.progress)
	require.Len(t, f.opens, 1)
	require.Len(t, f.logs, 1)
	require.Contains(t, f.opens, issueKey{"new", "unit"})
}

func TestNewerBuildCanReopenResolvedIssue(t *testing.T) {
	f := newFixture(t)
	f.seed("task", "unit", "old")
	f.source("build/10", "10", entity.BuildStatusSucceeded)

	require.NoError(t, f.process("build/10", true))
	f.source("build/11", "11", entity.BuildStatusRunning)
	f.observe("build/11", "task", "unit")

	require.NoError(t, f.process("build/11", false))
	require.Equal(t, "build/11", f.opens[issueKey{"task", "unit"}].FirstSeenBuildID)
	require.Len(t, f.logs, 3)
}

func TestRetriesFinishAcceptedWork(t *testing.T) {
	for _, operation := range []string{"logs/opened", "progress/complete"} {
		t.Run("opening/"+operation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/10", "10", entity.BuildStatusRunning)
			f.observe("build/10", "task", "unit")
			f.failures[operation] = 1
			require.ErrorIs(t, f.process("build/10", false), assert.AnError)
			require.Equal(t, entity.IssueTrackingPhaseRefreshing, f.progress.Phase)

			require.NoError(t, f.process("build/10", false))
			require.Len(t, f.opens, 1)
			require.Len(t, f.logs, 1)
			require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
		})
	}
	for _, operation := range []string{"logs/resolved", "opens/delete", "progress/complete"} {
		t.Run("resolution/"+operation, func(t *testing.T) {
			f := newFixture(t)
			f.seed("task", "unit", "old")
			f.source("build/10", "10", entity.BuildStatusSucceeded)
			f.failures[operation] = 1
			require.ErrorIs(t, f.process("build/10", true), assert.AnError)
			require.Equal(t, entity.IssueTrackingPhaseFinalizing, f.progress.Phase)
			if operation == "logs/resolved" {
				require.Len(t, f.opens, 1)
			}
			f.controller.nowMs = func() int64 { return 2000 }

			require.NoError(t, f.process("build/10", true))
			require.Empty(t, f.opens)
			require.Len(t, f.logs, 2)
			require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
		})
	}
}

func TestNewDeliveryResumesPendingResolutionBeforeReopening(t *testing.T) {
	f := newFixture(t)
	f.seed("task", "unit", "old")
	f.source("build/10", "10", entity.BuildStatusSucceeded)
	f.failures["opens/delete"] = 1
	require.ErrorIs(t, f.process("build/10", true), assert.AnError)
	f.source("build/11", "11", entity.BuildStatusRunning)
	f.observe("build/11", "task", "unit")

	require.NoError(t, f.process("build/11", false))
	require.Equal(t, "build/11", f.opens[issueKey{"task", "unit"}].FirstSeenBuildID)
	require.Len(t, f.logs, 3)
	require.Equal(t, "11", f.progress.RequestID)
	require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
}

func TestNewDeliveryResumesPendingRefresh(t *testing.T) {
	f := newFixture(t)
	f.source("old", "9", entity.BuildStatusRunning)
	f.observe("old", "old", "unit")
	f.failures["observations/list"] = 1
	require.ErrorIs(t, f.process("old", false), assert.AnError)
	f.source("new", "10", entity.BuildStatusRunning)
	f.observe("new", "new", "unit")

	require.NoError(t, f.process("new", false))
	require.Len(t, f.opens, 2)
	require.Len(t, f.logs, 2)
	require.Equal(t, "10", f.progress.RequestID)
}

func TestClaimConflictsRecheckSourceOrder(t *testing.T) {
	for _, kind := range []string{"create", "update"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/10", "10", entity.BuildStatusRunning)
			f.observe("build/10", "task", "unit")
			if kind == "create" {
				f.createRace = true
			} else {
				f.hasProgress = true
				f.progress = entity.IssueTrackingState{RequestID: "9", BuildID: "build/9", Phase: entity.IssueTrackingPhaseRefreshed, Version: 2}
				f.updateRace = true
			}

			require.NoError(t, f.process("build/10", false))
			require.Empty(t, f.opens)
			require.Empty(t, f.logs)
			require.Equal(t, "11", f.progress.RequestID)
		})
	}
}

func TestFinalRequiresTerminalWinningBuild(t *testing.T) {
	for _, kind := range []string{"nonterminal", "missing outcome", "different winning build"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/10", "10", entity.BuildStatusRunning)
			if kind != "nonterminal" {
				f.source("build/10", "10", entity.BuildStatusSucceeded)
				request := f.requests["10"]
				if kind == "missing outcome" {
					request.TerminalBuildID = ""
				} else {
					request.TerminalBuildID = "other"
				}
				f.requests["10"] = request
			}
			err := f.process("build/10", true)
			if kind == "different winning build" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.False(t, f.hasProgress)
			require.Empty(t, f.opens)
			require.Empty(t, f.logs)
		})
	}
}

func TestReadErrorsNeverImplyResolution(t *testing.T) {
	for _, operation := range []string{"progress/get", "observations/list", "observations/get", "opens/list"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/10", "10", entity.BuildStatusSucceeded)
			f.seed("task", "unit", "old")
			f.failures[operation] = 1
			require.ErrorIs(t, f.process("build/10", true), assert.AnError)
			require.Len(t, f.opens, 1)
			require.Len(t, f.logs, 1)
		})
	}
}

func TestRejectsInvalidNotifications(t *testing.T) {
	for _, kind := range []string{"malformed", "missing build", "missing queue", "wrong tenant", "wrong partition", "missing source", "wrong request queue", "invalid request ID"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/10", "10", entity.BuildStatusRunning)
			msg := f.message("build/10", false)
			switch kind {
			case "malformed":
				msg.Payload = []byte("{")
			case "missing build":
				msg = f.message("", false)
			case "missing queue":
				msg.Payload = []byte(`{"build_id":"build/10"}`)
			case "wrong tenant":
				msg.Tenant = "other"
			case "wrong partition":
				msg.PartitionKey = "build/10"
			case "missing source":
				delete(f.builds, "build/10")
			case "wrong request queue":
				request := f.requests["10"]
				request.Queue = "other"
				f.requests["10"] = request
			case "invalid request ID":
				build := f.builds["build/10"]
				build.RequestID = "request/10"
				f.builds["build/10"] = build
				f.requests["request/10"] = entity.Request{ID: "request/10", Queue: testQueue}
			}
			require.Error(t, f.deliver(context.Background(), msg))
			require.False(t, f.hasProgress)
			require.Empty(t, f.opens)
		})
	}
}

func TestCancelledContextDoesNotAcceptWork(t *testing.T) {
	f := newFixture(t)
	f.source("build/10", "10", entity.BuildStatusRunning)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, f.deliver(ctx, f.message("build/10", false)), context.Canceled)
	require.False(t, f.hasProgress)
}

func TestFinalNotificationFinishesPendingRefresh(t *testing.T) {
	f := newFixture(t)
	f.source("build/10", "10", entity.BuildStatusRunning)
	f.seed("old", "unit", "build/1")
	f.observe("build/10", "new", "unit")
	f.failures["logs/opened"] = 1
	require.ErrorIs(t, f.process("build/10", false), assert.AnError)

	f.source("build/10", "10", entity.BuildStatusFailed)
	require.NoError(t, f.process("build/10", true))

	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
	require.Len(t, f.opens, 1)
	require.Contains(t, f.opens, issueKey{"new", "unit"})
	require.Contains(t, f.logs, logKey{issueKey{"old", "unit"}, "build/10", entity.IssueEventResolved})
}

func TestNewFinalFinishesPendingOlderRefresh(t *testing.T) {
	f := newFixture(t)
	f.source("old", "9", entity.BuildStatusRunning)
	f.observe("old", "task", "unit")
	f.failures["opens/create"] = 1
	require.ErrorIs(t, f.process("old", false), assert.AnError)

	f.source("new", "10", entity.BuildStatusSucceeded)
	require.NoError(t, f.process("new", true))

	require.Empty(t, f.opens)
	require.Len(t, f.logs, 2)
	require.Equal(t, "10", f.progress.RequestID)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
}

func TestPendingFinalizationValidatesItsOwnBuildAndRequest(t *testing.T) {
	for _, invalidSource := range []string{"missing build", "missing request", "wrong request", "nonterminal build", "different winning build"} {
		t.Run(invalidSource, func(t *testing.T) {
			f := newFixture(t)
			f.source("pending", "10", entity.BuildStatusSucceeded)
			f.seed("old", "unit", "build/1")
			f.failures["logs/resolved"] = 1
			require.ErrorIs(t, f.process("pending", true), assert.AnError)
			pending := f.progress

			build := f.builds["pending"]
			request := f.requests["10"]
			switch invalidSource {
			case "missing build":
				delete(f.builds, "pending")
			case "missing request":
				delete(f.requests, "10")
			case "wrong request":
				request.ID = "9"
				f.requests["10"] = request
			case "nonterminal build":
				build.Status = entity.BuildStatusRunning
				f.builds["pending"] = build
			case "different winning build":
				request.TerminalBuildID = "other"
				f.requests["10"] = request
			}

			f.source("new", "11", entity.BuildStatusRunning)
			f.observe("new", "new", "unit")

			require.Error(t, f.process("new", false))
			require.Equal(t, pending, f.progress)
			require.Len(t, f.opens, 1)
			require.Contains(t, f.opens, issueKey{"old", "unit"})
			require.Len(t, f.logs, 1)
		})
	}
}

func TestRefreshNotificationCompletesPendingFinalization(t *testing.T) {
	f := newFixture(t)
	f.source("build/10", "10", entity.BuildStatusSucceeded)
	f.seed("old", "unit", "build/1")
	f.failures["opens/delete"] = 1
	require.ErrorIs(t, f.process("build/10", true), assert.AnError)

	require.NoError(t, f.process("build/10", false))

	require.Empty(t, f.opens)
	require.Len(t, f.logs, 2)
	require.Equal(t, entity.IssueTrackingPhaseFinalized, f.progress.Phase)
}

func TestDecideReconciliationForSameBuild(t *testing.T) {
	tests := []struct {
		phase    entity.IssueTrackingPhase
		finalize bool
		want     reconciliationAction
	}{
		{entity.IssueTrackingPhaseUnknown, false, _startRefresh},
		{entity.IssueTrackingPhaseUnknown, true, _startFinalization},
		{entity.IssueTrackingPhaseRefreshing, false, _resumeAndFinishNotification},
		{entity.IssueTrackingPhaseRefreshing, true, _resumeAndRecheckNotification},
		{entity.IssueTrackingPhaseRefreshed, false, _startRefresh},
		{entity.IssueTrackingPhaseRefreshed, true, _startFinalization},
		{entity.IssueTrackingPhaseFinalizing, false, _resumeAndFinishNotification},
		{entity.IssueTrackingPhaseFinalizing, true, _resumeAndFinishNotification},
		{entity.IssueTrackingPhaseFinalized, false, _ignoreNotification},
		{entity.IssueTrackingPhaseFinalized, true, _ignoreNotification},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/finalize=%t", tt.phase, tt.finalize), func(t *testing.T) {
			build := entity.Build{ID: "build/10", RequestID: "10", Status: entity.BuildStatusFailed}
			request := entity.Request{ID: "10", Queue: testQueue, TerminalBuildID: build.ID}
			var progress entity.IssueTrackingState
			if tt.phase != entity.IssueTrackingPhaseUnknown {
				progress = entity.IssueTrackingState{RequestID: request.ID, BuildID: build.ID, Phase: tt.phase, Version: 1}
			}

			action, err := decideReconciliation(progress, build, request, tt.finalize)

			require.NoError(t, err)
			require.Equal(t, tt.want, action)
		})
	}
}

func TestDecideReconciliationOrdersPendingAndIncomingWork(t *testing.T) {
	tests := []struct {
		name        string
		phase       entity.IssueTrackingPhase
		requestID   string
		buildID     string
		nonterminal bool
		noOutcome   bool
		want        reconciliationAction
		wantErr     bool
	}{
		{name: "resume older refresh", phase: entity.IssueTrackingPhaseRefreshing, requestID: "9", want: _resumeAndRecheckNotification},
		{name: "resume newer finalization", phase: entity.IssueTrackingPhaseFinalizing, requestID: "11", want: _resumeAndRecheckNotification},
		{name: "resume another build of same request", phase: entity.IssueTrackingPhaseRefreshing, requestID: "10", buildID: "other", want: _resumeAndRecheckNotification},
		{name: "ignore older final", phase: entity.IssueTrackingPhaseRefreshed, requestID: "11", nonterminal: true, want: _ignoreNotification},
		{name: "ignore already finalized", phase: entity.IssueTrackingPhaseFinalized, requestID: "10", nonterminal: true, want: _ignoreNotification},
		{name: "accept after older refresh", phase: entity.IssueTrackingPhaseRefreshed, requestID: "9", want: _startFinalization},
		{name: "accept after older final", phase: entity.IssueTrackingPhaseFinalized, requestID: "9", want: _startFinalization},
		{name: "finish pending before validating final", phase: entity.IssueTrackingPhaseRefreshing, requestID: "10", nonterminal: true, want: _resumeAndRecheckNotification},
		{name: "resume final based on saved work", phase: entity.IssueTrackingPhaseFinalizing, requestID: "10", nonterminal: true, want: _resumeAndFinishNotification},
		{name: "reject nonterminal final", nonterminal: true, wantErr: true},
		{name: "reject final without outcome", noOutcome: true, wantErr: true},
		{name: "reject invalid ordering ID", phase: entity.IssueTrackingPhaseRefreshed, requestID: "invalid", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			build := entity.Build{ID: "build/10", RequestID: "10", Status: entity.BuildStatusFailed}
			request := entity.Request{ID: "10", Queue: testQueue, TerminalBuildID: build.ID}
			if tt.nonterminal {
				build.Status = entity.BuildStatusRunning
			}
			if tt.noOutcome {
				request.TerminalBuildID = ""
			}
			var progress entity.IssueTrackingState
			if tt.phase != entity.IssueTrackingPhaseUnknown {
				buildID := tt.buildID
				if buildID == "" {
					buildID = build.ID
				}
				progress = entity.IssueTrackingState{RequestID: tt.requestID, BuildID: buildID, Phase: tt.phase, Version: 1}
			}

			action, err := decideReconciliation(progress, build, request, true)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, action)
			}
		})
	}
}

func TestFailedAcceptanceDoesNotApplyIssues(t *testing.T) {
	for _, operation := range []string{"progress/create", "progress/start"} {
		t.Run(operation, func(t *testing.T) {
			f := newFixture(t)
			f.source("build/10", "10", entity.BuildStatusRunning)
			f.seed("old", "unit", "build/1")
			f.observe("build/10", "new", "unit")
			if operation == "progress/start" {
				f.hasProgress = true
				f.progress = entity.IssueTrackingState{RequestID: "9", BuildID: "build/9", Phase: entity.IssueTrackingPhaseRefreshed, Version: 2}
			}
			before := f.progress
			f.failures[operation] = 1

			require.ErrorIs(t, f.process("build/10", false), assert.AnError)
			require.Equal(t, before, f.progress)
			require.Len(t, f.opens, 1)
			require.Len(t, f.logs, 1)

			require.NoError(t, f.process("build/10", false))
			require.Len(t, f.opens, 2)
			require.Len(t, f.logs, 2)
			require.Equal(t, entity.IssueTrackingPhaseRefreshed, f.progress.Phase)
		})
	}
}
