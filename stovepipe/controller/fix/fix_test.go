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

package fix

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	fixermock "github.com/uber/submitqueue/stovepipe/extension/fixer/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const (
	testQueue   = "validation/main"
	testBuildID = "Build/42 "
)

type controllerMocks struct {
	ctrl         *gomock.Controller
	stores       *storagemock.MockFactory
	store        *storagemock.MockStorage
	issues       *storagemock.MockIssueStateStore
	observations *storagemock.MockBuildIssueStore
	executors    *fixermock.MockFactory
	executor     *fixermock.MockRepairExecutor
	logs         *observer.ObservedLogs
}

func newTestController(t *testing.T) (*Controller, controllerMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	core, logs := observer.New(zap.InfoLevel)
	m := controllerMocks{
		ctrl:         ctrl,
		stores:       storagemock.NewMockFactory(ctrl),
		store:        storagemock.NewMockStorage(ctrl),
		issues:       storagemock.NewMockIssueStateStore(ctrl),
		observations: storagemock.NewMockBuildIssueStore(ctrl),
		executors:    fixermock.NewMockFactory(ctrl),
		executor:     fixermock.NewMockRepairExecutor(ctrl),
		logs:         logs,
	}
	m.store.EXPECT().GetIssueStateStore().Return(m.issues).AnyTimes()
	m.store.EXPECT().GetBuildIssueStore().Return(m.observations).AnyTimes()
	return NewController(zap.New(core).Sugar(), tally.NoopScope, m.stores, m.executors,
		consumer.TopicKey("fix"), "stovepipe-fix"), m
}

func testIssue() entity.OpenIssue {
	return entity.OpenIssue{TaskID: "test", UnitID: "unit/1 ", FirstSeenBuildID: testBuildID}
}

func storedIssue(issue entity.OpenIssue) entity.IssueState {
	return entity.IssueState{TaskID: issue.TaskID, UnitID: issue.UnitID, FirstSeenBuildID: issue.FirstSeenBuildID, Status: entity.IssueStatusOpen, Version: 1, LastProcessedBuildID: issue.FirstSeenBuildID, LastChangedBuildID: issue.FirstSeenBuildID, ChangedAtMs: 1000}
}

func issueIdentity(issue entity.OpenIssue) entity.BuildIssue {
	return entity.BuildIssue{TaskID: issue.TaskID, UnitID: issue.UnitID}
}

func issueWithFixID(issue entity.OpenIssue, fixID string) entity.BuildIssue {
	observation := issueIdentity(issue)
	observation.FixID = fixID
	return observation
}

func afterIssue(issue entity.BuildIssue) storage.BuildIssueRange {
	return storage.BuildIssueRange{
		After: storage.BuildIssueCursor{TaskID: issue.TaskID, UnitID: issue.UnitID},
		Limit: buildIssuePageSize,
	}
}

func (m controllerMocks) expectIssue(issue entity.OpenIssue) {
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).
		Return([]entity.BuildIssue{issueIdentity(issue)}, nil)
	m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(storedIssue(issue), nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issueIdentity(issue))).Return(nil, nil)
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return(m.executor, nil)
}

func TestProcessBuildSubmitsOneIssueAndAssociatesFixAfterCall(t *testing.T) {
	controller, m := newTestController(t)
	issue := testIssue()
	observation := issueIdentity(issue)
	observation.Version = 7
	another := entity.BuildIssue{TaskID: "test", UnitID: "unit/2"}
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).
		Return([]entity.BuildIssue{observation, another}, nil)
	m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(storedIssue(issue), nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(another)).Return(nil, nil)
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return(m.executor, nil)
	updated := observation
	updated.FixID = "fix/9"
	gomock.InOrder(
		m.executor.EXPECT().Submit(gomock.Any(), issue).Return(updated.FixID, nil),
		m.observations.EXPECT().Update(gomock.Any(), testBuildID, updated, int32(7), int32(8)).Return(nil),
	)
	require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
	entries := m.logs.All()
	require.Len(t, entries, 1)
	assert.Equal(t, testQueue, entries[0].ContextMap()["queue"])
	assert.Equal(t, testBuildID, entries[0].ContextMap()["build_id"])
	assert.Equal(t, issue.TaskID, entries[0].ContextMap()["task_id"])
	assert.Equal(t, issue.UnitID, entries[0].ContextMap()["unit_id"])
	assert.Equal(t, updated.FixID, entries[0].ContextMap()["fix_id"])
}

func TestProcessBuildFindsCandidateOnLaterPage(t *testing.T) {
	controller, m := newTestController(t)
	issue := entity.OpenIssue{TaskID: "test", FirstSeenBuildID: testBuildID}
	older := entity.BuildIssue{TaskID: "build"}
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	gomock.InOrder(
		m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).
			Return([]entity.BuildIssue{older}, nil),
		m.issues.EXPECT().Get(gomock.Any(), older.TaskID, older.UnitID).
			Return(entity.IssueState{TaskID: older.TaskID, FirstSeenBuildID: "older-build", Status: entity.IssueStatusOpen, Version: 1, LastProcessedBuildID: "older-build", LastChangedBuildID: "older-build", ChangedAtMs: 1000}, nil),
		m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(older)).
			Return([]entity.BuildIssue{issueIdentity(issue)}, nil),
		m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(storedIssue(issue), nil),
		m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issueIdentity(issue))).Return(nil, nil),
	)
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return(m.executor, nil)
	m.executor.EXPECT().Submit(gomock.Any(), issue).Return("attempt", nil)
	m.observations.EXPECT().Update(gomock.Any(), testBuildID, issueWithFixID(issue, "attempt"), int32(0), int32(1)).Return(nil)

	require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
}

func TestProcessBuildChecksLaterPagesForRecordedFix(t *testing.T) {
	controller, m := newTestController(t)
	issue := testIssue()
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	gomock.InOrder(
		m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).
			Return([]entity.BuildIssue{issueIdentity(issue)}, nil),
		m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(storedIssue(issue), nil),
		m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issueIdentity(issue))).
			Return([]entity.BuildIssue{{TaskID: "resolved", FixID: "fix/1"}}, nil),
	)
	require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
}

func TestProcessBuildDoesNotResubmitAfterRecordedFixID(t *testing.T) {
	controller, m := newTestController(t)
	issue := testIssue()
	record := issueIdentity(issue)
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil).Times(2)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).
		DoAndReturn(func(context.Context, string, storage.BuildIssueRange) ([]entity.BuildIssue, error) {
			return []entity.BuildIssue{record}, nil
		}).Times(2)
	m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(storedIssue(issue), nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(record)).Return(nil, nil)
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return(m.executor, nil)
	m.executor.EXPECT().Submit(gomock.Any(), issue).Return("fix/1", nil)
	m.observations.EXPECT().Update(gomock.Any(), testBuildID, issueWithFixID(issue, "fix/1"), int32(0), int32(1)).
		DoAndReturn(func(_ context.Context, _ string, updated entity.BuildIssue, _, newVersion int32) error {
			record = updated
			record.Version = newVersion
			return nil
		})
	for i := 0; i < 2; i++ {
		require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
	}
}

func TestProcessBuildWithoutEligibleIssue(t *testing.T) {
	for _, firstSeenBuildID := range []string{"", "other-build", "build/42 ", "Build/42"} {
		t.Run(firstSeenBuildID, func(t *testing.T) {
			controller, m := newTestController(t)
			issue := entity.BuildIssue{TaskID: "build"}
			m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
			m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).
				Return([]entity.BuildIssue{issue}, nil)
			if firstSeenBuildID == "" {
				m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(entity.IssueState{}, storage.ErrNotFound)
			} else {
				m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).
					Return(entity.IssueState{TaskID: issue.TaskID, FirstSeenBuildID: firstSeenBuildID, Status: entity.IssueStatusOpen, Version: 1, LastProcessedBuildID: firstSeenBuildID, LastChangedBuildID: firstSeenBuildID, ChangedAtMs: 1000}, nil)
			}
			m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issue)).Return(nil, nil)

			require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
		})
	}
}

func TestProcessBuildWithoutObservations(t *testing.T) {
	controller, m := newTestController(t)
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).Return(nil, nil)
	require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
}

func TestProcessBuildLeavesSubmissionWithoutAnIDEligibleForRetry(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "empty fix ID"},
		{name: "provider error", err: assert.AnError},
		{name: "retryable provider error", err: errs.NewRetryableError(assert.AnError)},
		{name: "cancelled call", err: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller, m := newTestController(t)
			issue := testIssue()
			m.expectIssue(issue)
			m.executor.EXPECT().Submit(gomock.Any(), issue).Return("", test.err)
			err := controller.processBuild(context.Background(), testQueue, testBuildID)
			require.Error(t, err)
			if test.err != nil {
				require.ErrorIs(t, err, test.err)
				assert.Equal(t, errs.IsRetryable(test.err), errs.IsRetryable(err))
			}
			require.Len(t, m.logs.All(), 1)
			assert.Equal(t, zap.ErrorLevel, m.logs.All()[0].Level)
		})
	}
}

func TestProcessBuildRetainsFixIDReturnedWithAnError(t *testing.T) {
	controller, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.executor.EXPECT().Submit(gomock.Any(), issue).Return("fix/1", assert.AnError)
	m.observations.EXPECT().Update(gomock.Any(), testBuildID, issueWithFixID(issue, "fix/1"), int32(0), int32(1)).Return(nil)
	require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
}

func TestProcessBuildHandlesAssociationPersistenceResults(t *testing.T) {
	for _, test := range []struct {
		name      string
		updateErr error
		current   entity.BuildIssue
		readErr   error
		wantErr   error
	}{
		{name: "write error", updateErr: assert.AnError, wantErr: assert.AnError},
		{name: "concurrent association", updateErr: storage.ErrVersionMismatch, current: entity.BuildIssue{FixID: "another-fix"}},
		{name: "concurrent update without an association", updateErr: storage.ErrVersionMismatch, wantErr: storage.ErrVersionMismatch},
		{name: "reload error", updateErr: storage.ErrVersionMismatch, readErr: assert.AnError, wantErr: assert.AnError},
	} {
		t.Run(test.name, func(t *testing.T) {
			controller, m := newTestController(t)
			issue := testIssue()
			m.expectIssue(issue)
			m.executor.EXPECT().Submit(gomock.Any(), issue).Return("fix/1", nil)
			m.observations.EXPECT().Update(gomock.Any(), testBuildID, issueWithFixID(issue, "fix/1"), int32(0), int32(1)).Return(test.updateErr)
			if test.updateErr == storage.ErrVersionMismatch {
				m.observations.EXPECT().Get(gomock.Any(), testBuildID, issue.TaskID, issue.UnitID).Return(test.current, test.readErr)
			}
			err := controller.processBuild(context.Background(), testQueue, testBuildID)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "another-fix", m.logs.All()[1].ContextMap()["fix_id"])
			}
		})
	}
}

func TestProcessBuildReturnsErrorsBeforeSubmission(t *testing.T) {
	for _, step := range []string{"storage factory", "issue listing", "open issue lookup", "later page", "executor factory"} {
		t.Run(step, func(t *testing.T) {
			controller, m := newTestController(t)
			issue := testIssue()
			configureFailure := func() {
				if step == "storage factory" {
					m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, assert.AnError)
					return
				}
				m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				first := m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize})
				if step == "issue listing" {
					first.Return(nil, assert.AnError)
					return
				}
				first.Return([]entity.BuildIssue{issueIdentity(issue)}, nil)
				lookup := m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID)
				if step == "open issue lookup" {
					lookup.Return(entity.IssueState{}, assert.AnError)
					return
				}
				lookup.Return(storedIssue(issue), nil)
				last := m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issueIdentity(issue)))
				if step == "later page" {
					last.Return(nil, assert.AnError)
					return
				}
				last.Return(nil, nil)
				m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return(nil, assert.AnError)
			}
			configureFailure()
			require.ErrorIs(t, controller.processBuild(context.Background(), testQueue, testBuildID), assert.AnError)
		})
	}
}

func TestProcessBuildSkipsClosedIssue(t *testing.T) {
	controller, m := newTestController(t)
	issue := testIssue()
	closed := storedIssue(issue)
	closed.Status = entity.IssueStatusClosed
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).Return([]entity.BuildIssue{issueIdentity(issue)}, nil)
	m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(closed, nil)
	m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issueIdentity(issue))).Return(nil, nil)
	require.NoError(t, controller.processBuild(context.Background(), testQueue, testBuildID))
}
