// Copyright (c) 2025 Uber Technologies, Inc.
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

package fix

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/stovepipe/core/fixrequest"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	fixermock "github.com/uber/submitqueue/stovepipe/extension/fixer/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type fixture struct {
	ctrl             *gomock.Controller
	controller       *Controller
	registry         consumer.TopicRegistry
	params           Params
	build            entity.Build
	opening          entity.OpenIssue
	fixes            map[string]entity.Fix
	messages         map[string]entityqueue.Message
	executor         *fixermock.MockRepairExecutor
	publicationError error
	createConflict   bool
	historyError     error
	historyEvent     entity.RequestEvent
	logs             map[string]entity.RequestLog
	claimConflict    bool
	resultError      error
	executorError    error
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &fixture{ctrl: ctrl, logs: make(map[string]entity.RequestLog), build: entity.Build{ID: "build-1", RequestID: "1", Status: entity.BuildStatusRunning, Version: 1}, opening: entity.OpenIssue{TaskID: "compile", UnitID: "unit-a", FirstSeenBuildID: "build-1"}, fixes: make(map[string]entity.Fix), messages: make(map[string]entityqueue.Message), executor: fixermock.NewMockRepairExecutor(ctrl)}
	stores := storagemock.NewMockFactory(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	builds := storagemock.NewMockBuildStore(ctrl)
	requests := storagemock.NewMockRequestStore(ctrl)
	issues := storagemock.NewMockBuildIssueStore(ctrl)
	open := storagemock.NewMockOpenIssueStore(ctrl)
	fixes := storagemock.NewMockFixStore(ctrl)
	stores.EXPECT().For(storage.Config{QueueName: "q"}).Return(store, nil).AnyTimes()
	store.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
	store.EXPECT().GetBuildIssueStore().Return(issues).AnyTimes()
	store.EXPECT().GetOpenIssueStore().Return(open).AnyTimes()
	store.EXPECT().GetFixStore().Return(fixes).AnyTimes()
	builds.EXPECT().Get(gomock.Any(), "build-1").DoAndReturn(func(context.Context, string) (entity.Build, error) { return f.build, nil }).AnyTimes()
	requests.EXPECT().Get(gomock.Any(), "1").Return(entity.Request{ID: "1", Queue: "q"}, nil).AnyTimes()
	issues.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, task, unit string) (entity.BuildIssue, error) {
		return entity.BuildIssue{TaskID: task, UnitID: unit}, nil
	}).AnyTimes()
	open.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, string, string) (entity.OpenIssue, error) {
		if f.opening.TaskID == "" {
			return entity.OpenIssue{}, storage.ErrNotFound
		}
		return f.opening, nil
	}).AnyTimes()
	fixes.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, row entity.Fix) error {
		if f.createConflict {
			f.createConflict = false
			row.Issues[0].UnitID = "unit-b"
			f.opening.UnitID = "unit-b"
			f.fixes[row.ID] = row
			return storage.ErrAlreadyExists
		}
		if _, ok := f.fixes[row.ID]; ok {
			return storage.ErrAlreadyExists
		}
		f.fixes[row.ID] = row
		return nil
	}).AnyTimes()
	fixes.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.Fix, error) {
		row, ok := f.fixes[id]
		if !ok {
			return row, storage.ErrNotFound
		}
		return row, nil
	}).AnyTimes()
	fixes.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, row entity.Fix, old, new int32) error {
		previous, ok := f.fixes[row.ID]
		if !ok || previous.Version != old {
			return storage.ErrVersionMismatch
		}
		if f.claimConflict && row.State == entity.FixSubmitting {
			f.claimConflict = false
			previous.State = entity.FixSubmitting
			previous.Version = new
			f.fixes[row.ID] = previous
			return storage.ErrVersionMismatch
		}
		if f.resultError != nil && (row.State == entity.FixSubmitted || row.State == entity.FixSubmissionUnknown) {
			return f.resultError
		}
		row.Version = new
		f.fixes[row.ID] = row
		return nil
	}).AnyTimes()
	logs := storagemock.NewMockRequestLogStore(ctrl)
	store.EXPECT().GetRequestLogStore().Return(logs).AnyTimes()
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.RequestLog) error {
		if f.historyError != nil && (f.historyEvent == "" || f.historyEvent == log.Event) {
			return f.historyError
		}
		if _, ok := f.logs[log.ID]; ok {
			return storage.ErrAlreadyExists
		}
		f.logs[log.ID] = log
		return nil
	}).AnyTimes()
	logs.EXPECT().Get(gomock.Any(), "1", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, id string) (entity.RequestLog, error) { return f.logs[id], nil }).AnyTimes()
	executors := fixermock.NewMockFactory(ctrl)
	executors.EXPECT().For(fixer.Config{QueueName: "q"}).DoAndReturn(func(fixer.Config) (fixer.RepairExecutor, error) { return f.executor, f.executorError }).AnyTimes()
	queue := mqmock.NewMockQueue(ctrl)
	publisher := mqmock.NewMockPublisher(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, topic string, msg entityqueue.Message) error {
		if f.publicationError != nil {
			return f.publicationError
		}
		f.messages[topic+":"+msg.ID] = msg
		return nil
	}).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: mq.TopicKeyFix, Name: "fix", Queue: queue}})
	require.NoError(t, err)
	f.registry = registry
	f.params = Params{Stores: stores, Materializer: requestlog.NewMaterializer(tally.NoopScope), Executors: executors, TopicKey: mq.TopicKeyFix, ConsumerGroup: "fix"}
	f.controller = NewController(f.params)
	return f
}

func (f *fixture) request(t *testing.T, unit, opening string) error {
	t.Helper()
	payload, err := mq.Marshal(&mq.FixRequest{QueueName: "q", BuildId: "build-1", TaskId: "compile", UnitId: unit, FirstSeenBuildId: opening})
	require.NoError(t, err)
	return f.process(t, payload)
}
func (f *fixture) process(t *testing.T, payload []byte) error {
	t.Helper()
	message := entityqueue.NewMessage("delivery", payload, "partition", nil)
	message.Tenant = "q"
	delivery := consumermock.NewMockDelivery(f.ctrl)
	delivery.EXPECT().Message().Return(message)
	return f.controller.Process(context.Background(), delivery)
}
func TestFirstTargetTriggersWhileBuildIsRunningAndLaterTargetsNoop(t *testing.T) {
	f := newFixture(t)
	f.executor.EXPECT().Submit(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req fixer.SubmitRequest) (string, error) {
		require.Equal(t, entity.BuildStatusRunning, f.build.Status)
		require.Equal(t, f.build.ID, req.BuildID)
		require.Equal(t, f.opening, req.Issue)
		require.Equal(t, entity.FixSubmitting, f.fixes[req.BuildID].State)
		require.NoError(t, f.request(t, "unit-a", "build-1"))
		return "trigger-42", nil
	})
	require.NoError(t, f.request(t, "unit-a", "build-1"))
	id := f.build.ID
	require.Equal(t, entity.FixSubmitted, f.fixes[id].State)
	require.Equal(t, "trigger-42", f.fixes[id].ExecutionRef)
	for _, unit := range []string{"unit-a", "unit-b", "unit-c"} {
		require.NoError(t, f.request(t, unit, "build-1"))
	}
	require.Len(t, f.fixes, 1)
	require.Empty(t, f.messages)
}

func TestTriggerErrorAndEmptyReplyAreRecordedWithoutResubmission(t *testing.T) {
	for _, tt := range []struct {
		name, reference string
		err             error
	}{
		{name: "lost reply", reference: "ignored-on-error", err: errors.New("reply lost")},
		{name: "empty reference"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.executor.EXPECT().Submit(gomock.Any(), gomock.Any()).Return(tt.reference, tt.err)
			require.NoError(t, f.request(t, "unit-a", "build-1"))
			fix := f.fixes[f.build.ID]
			require.Equal(t, entity.FixSubmissionUnknown, fix.State)
			require.Empty(t, fix.ExecutionRef)
			require.NotEmpty(t, fix.Metadata["submission.error"])
			require.NoError(t, f.request(t, "unit-a", "build-1"))
			require.NoError(t, f.request(t, "unit-b", "build-1"))
			require.Len(t, f.fixes, 1)
		})
	}
}

func TestClaimConflictNeverDispatchesAgain(t *testing.T) {
	f := newFixture(t)
	f.claimConflict = true
	require.ErrorIs(t, f.request(t, "unit-a", "build-1"), storage.ErrVersionMismatch)
	require.Equal(t, entity.FixSubmitting, f.fixes[f.build.ID].State)
	require.NoError(t, f.request(t, "unit-a", "build-1"))
	require.NoError(t, f.request(t, "unit-b", "build-1"))
}

func TestResultPersistenceFailureNeverRepeatsTrigger(t *testing.T) {
	f := newFixture(t)
	f.resultError = errors.New("database unavailable")
	f.executor.EXPECT().Submit(gomock.Any(), gomock.Any()).Return("trigger", nil)
	require.ErrorIs(t, f.request(t, "unit-a", "build-1"), f.resultError)
	require.Equal(t, entity.FixSubmitting, f.fixes[f.build.ID].State)
	require.NoError(t, f.request(t, "unit-a", "build-1"))
	require.NoError(t, f.request(t, "unit-b", "build-1"))
}

func TestCreateRaceUsesPersistedSelection(t *testing.T) {
	f := newFixture(t)
	f.createConflict = true
	f.executor.EXPECT().Submit(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req fixer.SubmitRequest) (string, error) {
		require.Equal(t, "unit-b", req.Issue.UnitID)
		return "trigger", nil
	})
	require.NoError(t, f.request(t, "unit-a", "build-1"))
	require.Len(t, f.fixes, 1)
	require.Len(t, f.logs, 2)
	for _, log := range f.logs {
		require.Equal(t, "unit-b", log.Metadata["unit_id"])
	}
}

func TestTaskWideFailureAndStaleOpeningDoNotSelectTarget(t *testing.T) {
	f := newFixture(t)
	require.NoError(t, f.request(t, "", "build-1"))
	f.opening.FirstSeenBuildID = "new-opening"
	require.NoError(t, f.request(t, "unit-a", "build-1"))
	require.Empty(t, f.fixes)
}

func TestQueuedRepairWhoseOpeningRecoveredOrReopenedIsSkipped(t *testing.T) {
	for _, opening := range []entity.OpenIssue{{}, {TaskID: "compile", UnitID: "unit-a", FirstSeenBuildID: "new-opening"}} {
		t.Run(opening.FirstSeenBuildID, func(t *testing.T) {
			f := newFixture(t)
			f.fixes[f.build.ID] = entity.Fix{ID: f.build.ID, Queue: "q", BuildID: f.build.ID, State: entity.FixQueued, Version: 1, Issues: []entity.OpenIssue{f.opening}}
			f.opening = opening
			require.NoError(t, f.request(t, "unit-b", "build-1"))
			require.Equal(t, entity.FixSkipped, f.fixes[f.build.ID].State)
			require.NoError(t, f.request(t, "unit-c", "build-1"))
			require.Empty(t, f.logs)
		})
	}
}

func TestLaterBuildCanRepairAnOlderOpening(t *testing.T) {
	f := newFixture(t)
	f.opening.FirstSeenBuildID = "previous-build"
	f.executor.EXPECT().Submit(gomock.Any(), fixer.SubmitRequest{BuildID: "build-1", Issue: f.opening}).Return("trigger", nil)
	require.NoError(t, f.request(t, "unit-a", "previous-build"))
	require.Equal(t, "previous-build", f.fixes[f.build.ID].Issues[0].FirstSeenBuildID)
}

func TestIssueHandoffReplaysAndPreservesOpaqueIdentity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ref := entity.OpenIssue{TaskID: "task/one", UnitID: "Unit A ", FirstSeenBuildID: "build/1"}
	f.publicationError = errors.New("publish failed")
	require.Error(t, fixrequest.Publish(ctx, f.registry, "q", "build-1", ref))
	f.publicationError = nil
	require.NoError(t, fixrequest.Publish(ctx, f.registry, "q", "build-1", ref))
	require.NoError(t, fixrequest.Publish(ctx, f.registry, "q", "build-1", ref))
	require.Len(t, f.messages, 1)
	for _, message := range f.messages {
		request := &mq.FixRequest{}
		require.NoError(t, mq.Unmarshal(message.Payload, request))
		require.Equal(t, ref.UnitID, request.GetUnitId())
		require.Equal(t, "build-1", message.PartitionKey)
	}
	other := ref
	other.UnitID = fmt.Sprintf("%s/another", ref.UnitID)
	require.NoError(t, fixrequest.Publish(ctx, f.registry, "q", "build-1", other))
	require.Len(t, f.messages, 2)
}

func TestPreparationFailureCanResumeWithoutChangingSelectedTarget(t *testing.T) {
	f := newFixture(t)
	f.executorError = errors.New("configuration unavailable")
	require.ErrorIs(t, f.request(t, "unit-a", "build-1"), f.executorError)
	id := f.build.ID
	require.Equal(t, entity.FixQueued, f.fixes[id].State)
	require.ErrorIs(t, f.request(t, "unit-b", "build-1"), f.executorError)
	f.executorError = nil
	f.executor.EXPECT().Submit(gomock.Any(), gomock.Any()).Return("trigger", nil)
	require.NoError(t, f.request(t, "unit-a", "build-1"))
	require.Equal(t, id, f.build.ID)
	require.Len(t, f.fixes, 1)
}

func TestHistoryFailureReplaysSavedResultWithoutAnotherSubmission(t *testing.T) {
	for _, event := range []entity.RequestEvent{entity.RequestEventFixSubmissionStarted, entity.RequestEventFixSubmissionRecorded} {
		t.Run(string(event), func(t *testing.T) {
			f := newFixture(t)
			f.historyError = errors.New("history unavailable")
			f.historyEvent = event
			f.executor.EXPECT().Submit(gomock.Any(), gomock.Any()).Return("trigger", nil)
			require.ErrorIs(t, f.request(t, "unit-a", "build-1"), f.historyError)
			require.Equal(t, entity.FixSubmitted, f.fixes[f.build.ID].State)
			f.historyError = nil
			require.NoError(t, f.request(t, "unit-b", "build-1"))
			require.NoError(t, f.request(t, "unit-c", "build-1"))
			require.Len(t, f.logs, 2)
			for _, log := range f.logs {
				require.Equal(t, "1", log.RequestID)
				require.Equal(t, "unit-a", log.Metadata["unit_id"])
				if log.Event == entity.RequestEventFixSubmissionRecorded {
					require.Equal(t, "trigger", log.Metadata["execution_ref"])
				}
			}
		})
	}
}
