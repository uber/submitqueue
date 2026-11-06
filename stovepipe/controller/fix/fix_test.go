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
	"fmt"
	"maps"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"

	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	fixermock "github.com/uber/submitqueue/stovepipe/extension/fixer/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue   = "validation/main"
	testBuildID = "Build/42 "
)

type controllerMocks struct {
	history        map[string]entity.RequestLog
	historyFailure *error
	materializer   requestlog.Materializer
	builds         *storagemock.MockBuildStore
	requests       *storagemock.MockRequestStore
	ctrl           *gomock.Controller
	stores         *storagemock.MockFactory
	store          *storagemock.MockStorage
	issues         *storagemock.MockIssueStateStore
	observations   *storagemock.MockBuildIssueStore
	executors      *fixermock.MockFactory
	executor       *fixermock.MockRepairExecutor
	repairs        map[entity.OpenIssue]map[string]entity.IssueRepair
	results        *storagemock.MockIssueRepairStore
	repairFailure  *error
	updateFailure  *error
}

func newTestController(t *testing.T) (*Controller, controllerMocks) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := controllerMocks{
		ctrl:         ctrl,
		stores:       storagemock.NewMockFactory(ctrl),
		store:        storagemock.NewMockStorage(ctrl),
		issues:       storagemock.NewMockIssueStateStore(ctrl),
		observations: storagemock.NewMockBuildIssueStore(ctrl),
		executors:    fixermock.NewMockFactory(ctrl),
		executor:     fixermock.NewMockRepairExecutor(ctrl),
	}
	m.store.EXPECT().GetIssueStateStore().Return(m.issues).AnyTimes()
	m.store.EXPECT().GetBuildIssueStore().Return(m.observations).AnyTimes()

	m.repairs = map[entity.OpenIssue]map[string]entity.IssueRepair{}
	m.repairFailure = new(error)
	m.results = storagemock.NewMockIssueRepairStore(ctrl)
	m.store.EXPECT().GetIssueRepairStore().Return(m.results).AnyTimes()
	m.results.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, issue entity.OpenIssue, id string) (entity.IssueRepair, error) {
		if result, ok := m.repairs[issue][id]; ok {
			result.Metadata = maps.Clone(result.Metadata)
			return result, nil
		}
		return entity.IssueRepair{}, storage.ErrNotFound
	}).AnyTimes()
	m.results.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, result entity.IssueRepair) error {
		if *m.repairFailure != nil {
			return *m.repairFailure
		}
		if _, ok := m.repairs[result.Issue][result.ExecutorID]; ok {
			return storage.ErrAlreadyExists
		}
		if m.repairs[result.Issue] == nil {
			m.repairs[result.Issue] = map[string]entity.IssueRepair{}
		}
		m.repairs[result.Issue][result.ExecutorID] = result
		return nil
	}).AnyTimes()
	m.updateFailure = new(error)
	m.results.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, replacement entity.IssueRepair, oldVersion, newVersion int32) error {
		if *m.updateFailure != nil {
			return *m.updateFailure
		}
		current, exists := m.repairs[replacement.Issue][replacement.ExecutorID]
		if !exists || current.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		replacement.Version = newVersion
		replacement.Metadata = maps.Clone(replacement.Metadata)
		m.repairs[replacement.Issue][replacement.ExecutorID] = replacement
		return nil
	}).AnyTimes()

	m.history = map[string]entity.RequestLog{}
	m.historyFailure = new(error)
	m.materializer = requestlog.NewMaterializer(tally.NoopScope)
	history := storagemock.NewMockRequestLogStore(ctrl)
	m.store.EXPECT().GetRequestLogStore().Return(history).AnyTimes()
	history.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.RequestLog) error {
		if *m.historyFailure != nil {
			return *m.historyFailure
		}
		if _, exists := m.history[log.ID]; exists {
			return storage.ErrAlreadyExists
		}
		m.history[log.ID] = log
		return nil
	}).AnyTimes()
	history.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, requestID, logID string) (entity.RequestLog, error) {
		log, exists := m.history[logID]
		if !exists || log.RequestID != requestID {
			return entity.RequestLog{}, storage.ErrNotFound
		}
		return log, nil
	}).AnyTimes()

	m.builds = storagemock.NewMockBuildStore(ctrl)
	m.requests = storagemock.NewMockRequestStore(ctrl)
	m.store.EXPECT().GetBuildStore().Return(m.builds).AnyTimes()
	m.store.EXPECT().GetRequestStore().Return(m.requests).AnyTimes()
	m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID, RequestID: "1"}, nil).AnyTimes()
	m.requests.EXPECT().Get(gomock.Any(), "1").Return(entity.Request{ID: "1", Queue: testQueue, State: entity.RequestStateFailed}, nil).AnyTimes()
	return NewController(zap.NewNop().Sugar(), tally.NoopScope, m.stores, m.materializer, m.executors,
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

func afterIssue(issue entity.BuildIssue) storage.BuildIssueRange {
	return storage.BuildIssueRange{
		After: storage.BuildIssueCursor{TaskID: issue.TaskID, UnitID: issue.UnitID},
		Limit: buildIssuePageSize,
	}
}

func (m controllerMocks) expectIssues(items []entity.OpenIssue) {
	m.stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil).AnyTimes()
	observations := make([]entity.BuildIssue, 0, len(items))
	for _, issue := range items {
		observations = append(observations, issueIdentity(issue))
		m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(storedIssue(issue), nil).AnyTimes()
	}
	sort.Slice(observations, func(i, j int) bool {
		if observations[i].TaskID == observations[j].TaskID {
			return observations[i].UnitID < observations[j].UnitID
		}
		return observations[i].TaskID < observations[j].TaskID
	})
	m.observations.EXPECT().List(gomock.Any(), testBuildID, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, bounds storage.BuildIssueRange) ([]entity.BuildIssue, error) {
		var page []entity.BuildIssue
		for _, observation := range observations {
			if observation.TaskID > bounds.After.TaskID || observation.TaskID == bounds.After.TaskID && observation.UnitID > bounds.After.UnitID {
				page = append(page, observation)
				if len(page) == bounds.Limit {
					break
				}
			}
		}
		return page, nil
	}).AnyTimes()
}

func (m controllerMocks) expectIssue(issue entity.OpenIssue) {
	m.expectIssues([]entity.OpenIssue{issue})
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return([]fixer.Registration{{ID: "first", Executor: m.executor}}, nil).AnyTimes()
}

func accepted(issues []entity.OpenIssue, id string) []fixer.Submission {
	results := make([]fixer.Submission, 0, len(issues))
	for _, issue := range issues {
		results = append(results, fixer.Submission{Issue: issue, FixID: id})
	}
	return results
}

func TestProcessBuildBatchesAcrossPagesAndReplays(t *testing.T) {
	c, m := newTestController(t)
	issues := make([]entity.OpenIssue, 205)
	for i := range issues {
		issues[i] = entity.OpenIssue{TaskID: "tests", UnitID: fmt.Sprintf("unit/%03d", i), FirstSeenBuildID: testBuildID}
	}
	m.expectIssues(issues)
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return([]fixer.Registration{{ID: "first", Executor: m.executor}}, nil).Times(2)
	m.executor.EXPECT().Submit(gomock.Any(), issues).Return(accepted(issues, "shared-fix"), nil)
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.Len(t, m.repairs, 205)
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.Len(t, m.history, 205)
}

func TestIndependentExecutorsRetainPartialResults(t *testing.T) {
	c, m := newTestController(t)
	first := testIssue()
	second := first
	second.UnitID = "unit/2"
	items := []entity.OpenIssue{first, second}
	m.expectIssues(items)
	other := fixermock.NewMockRepairExecutor(m.ctrl)
	m.executors.EXPECT().For(fixer.Config{QueueName: testQueue}).Return([]fixer.Registration{{ID: "first", Executor: m.executor}, {ID: "other", Executor: other}}, nil).AnyTimes()
	m.executor.EXPECT().Submit(gomock.Any(), items).Return([]fixer.Submission{{Issue: first, FixID: "shared-fix"}}, assert.AnError)
	other.EXPECT().Submit(gomock.Any(), items).Return([]fixer.Submission{
		{Issue: first, SkipReason: "unsupported"},
		{Issue: second, FixID: "other-fix"},
	}, nil)
	require.ErrorIs(t, c.processBuild(context.Background(), testQueue, testBuildID), assert.AnError)
	m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{second}).Return(accepted([]entity.OpenIssue{second}, "shared-fix"), nil)
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.Len(t, m.repairs[first], 2)
	require.Len(t, m.repairs[second], 2)
	assert.Equal(t, "shared-fix", m.repairs[second]["first"].FixID)
}

func TestInvalidExecutorResultsLeaveItemsPending(t *testing.T) {
	for _, kind := range []string{"omitted", "foreign", "both dispositions", "empty disposition", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			c, m := newTestController(t)
			issue := testIssue()
			m.expectIssue(issue)
			var results []fixer.Submission
			switch kind {
			case "foreign":
				changed := issue
				changed.FirstSeenBuildID = "other"
				results = accepted([]entity.OpenIssue{changed}, "id")
			case "both dispositions":
				results = []fixer.Submission{{Issue: issue, FixID: "id", SkipReason: "skip"}}
			case "empty disposition":
				results = []fixer.Submission{{Issue: issue}}
			case "duplicate":
				results = append(accepted([]entity.OpenIssue{issue}, "id"), accepted([]entity.OpenIssue{issue}, "id")...)
			}
			m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).Return(results, nil)
			require.Error(t, c.processBuild(context.Background(), testQueue, testBuildID))
			if kind != "duplicate" {
				require.Empty(t, m.repairs[issue]["first"].FixID)
				require.Empty(t, m.repairs[issue]["first"].SkipReason)
			}
		})
	}
}

func TestClosedAndPreviousOpeningsAreNotSubmitted(t *testing.T) {
	for _, state := range []entity.IssueState{{Status: entity.IssueStatusClosed, FirstSeenBuildID: testBuildID}, {Status: entity.IssueStatusOpen, FirstSeenBuildID: "old-build"}, {}} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			c, m := newTestController(t)
			issue := testIssue()
			m.stores.EXPECT().For(gomock.Any()).Return(m.store, nil)
			m.executors.EXPECT().For(gomock.Any()).Return([]fixer.Registration{{ID: "first", Executor: m.executor}}, nil)
			m.observations.EXPECT().List(gomock.Any(), testBuildID, storage.BuildIssueRange{Limit: buildIssuePageSize}).Return([]entity.BuildIssue{issueIdentity(issue)}, nil)
			m.observations.EXPECT().List(gomock.Any(), testBuildID, afterIssue(issueIdentity(issue))).Return(nil, nil)
			m.issues.EXPECT().Get(gomock.Any(), issue.TaskID, issue.UnitID).Return(state, nil)
			require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
		})
	}
}

func TestRepairHistoryRetryDoesNotResubmit(t *testing.T) {
	for _, skipped := range []bool{false, true} {
		t.Run(fmt.Sprint(skipped), func(t *testing.T) {
			c, m := newTestController(t)
			issue := testIssue()
			m.expectIssue(issue)
			submission := fixer.Submission{Issue: issue, FixID: "fix"}
			if skipped {
				submission.FixID, submission.SkipReason = "", "unsupported"
			}
			m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).Return([]fixer.Submission{submission}, nil)
			*m.historyFailure = assert.AnError
			require.Error(t, c.processBuild(context.Background(), testQueue, testBuildID))
			*m.historyFailure = nil
			require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
			require.Len(t, m.history, 1)
		})
	}
}

func TestConcurrentDispositionIsNeverOverwritten(t *testing.T) {
	for _, tt := range []struct {
		name       string
		fixID      string
		skipReason string
		conflicts  bool
	}{
		{name: "same decision", fixID: "fix"},
		{name: "different fix", fixID: "already-recorded", conflicts: true},
		{name: "skip", skipReason: "unsupported", conflicts: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, m := newTestController(t)
			issue := testIssue()
			m.expectIssue(issue)
			metadata := map[string]string{"state": "retained"}
			m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).DoAndReturn(func(ctx context.Context, _ []entity.OpenIssue) ([]fixer.Submission, error) {
				current, err := m.results.Get(ctx, issue, "first")
				require.NoError(t, err)
				current.FixID, current.SkipReason, current.Metadata = tt.fixID, tt.skipReason, metadata
				require.NoError(t, m.results.Update(ctx, current, current.Version, current.Version+1))
				return []fixer.Submission{{Issue: issue, FixID: "fix"}}, nil
			})
			err := c.processBuild(context.Background(), testQueue, testBuildID)
			if tt.conflicts {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, metadata, m.repairs[issue]["first"].Metadata)
			require.Equal(t, tt.fixID, m.repairs[issue]["first"].FixID)
			require.Equal(t, tt.skipReason, m.repairs[issue]["first"].SkipReason)
			require.Len(t, m.history, 1)
		})
	}
}

func TestFixerMetadataPersistsAcrossSubmissionAndReplay(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	metadata := map[string]string{"state": `{"attempt":2,"workflow":"run/1"}`}
	m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).DoAndReturn(func(ctx context.Context, _ []entity.OpenIssue) ([]fixer.Submission, error) {
		current, err := m.results.Get(ctx, issue, "first")
		require.NoError(t, err)
		require.Empty(t, current.FixID)
		require.Empty(t, current.SkipReason)
		current.Metadata = metadata
		require.NoError(t, m.results.Update(ctx, current, current.Version, current.Version+1))
		return accepted([]entity.OpenIssue{issue}, "fix"), nil
	})
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.Equal(t, "fix", m.repairs[issue]["first"].FixID)
	require.Equal(t, metadata, m.repairs[issue]["first"].Metadata)
	require.Len(t, m.history, 1)
}

func TestConcurrentMetadataUpdateRetriesReceipt(t *testing.T) {
	ctrl := gomock.NewController(t)
	repairs := storagemock.NewMockIssueRepairStore(ctrl)
	issue := testIssue()
	pending := entity.IssueRepair{Issue: issue, ExecutorID: "first", Version: 1}
	concurrent := pending
	concurrent.Version = 2
	concurrent.Metadata = map[string]string{"state": "concurrent edit"}
	replacement := concurrent
	replacement.FixID = "fix"
	gomock.InOrder(
		repairs.EXPECT().Get(gomock.Any(), issue, "first").Return(pending, nil),
		repairs.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(storage.ErrVersionMismatch),
		repairs.EXPECT().Get(gomock.Any(), issue, "first").Return(concurrent, nil),
		repairs.EXPECT().Update(gomock.Any(), replacement, int32(2), int32(3)).Return(nil),
	)
	actual, err := persistRepairSubmission(context.Background(), repairs, "first", fixer.Submission{Issue: issue, FixID: "fix"})
	require.NoError(t, err)
	require.Equal(t, "fix", actual.FixID)
	require.Equal(t, concurrent.Metadata, actual.Metadata)
	require.Equal(t, int32(3), actual.Version)
}

func TestPartialExecutorFailuresRetainRetryClassification(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).Return(nil, context.Canceled)
	err := c.processBuild(context.Background(), testQueue, testBuildID)
	processed := errs.NewClassifierProcessor(genericerrs.Classifier).Process(err)
	require.True(t, errs.IsRetryable(processed))
}

func TestFailedDispositionWriteRetriesOnlyUnrecordedItems(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).Return(accepted([]entity.OpenIssue{issue}, "fix"), nil).Times(2)
	*m.updateFailure = assert.AnError
	require.ErrorIs(t, c.processBuild(context.Background(), testQueue, testBuildID), assert.AnError)
	require.Empty(t, m.repairs[issue]["first"].FixID)
	require.Empty(t, m.history)
	*m.updateFailure = nil
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.Equal(t, "fix", m.repairs[issue]["first"].FixID)
}

func TestFailedPendingWriteDoesNotCallExecutor(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	*m.repairFailure = assert.AnError
	require.ErrorIs(t, c.processBuild(context.Background(), testQueue, testBuildID), assert.AnError)
	require.Empty(t, m.repairs)
}

func TestExistingPendingRowIsResubmitted(t *testing.T) {
	c, m := newTestController(t)
	issue := testIssue()
	m.expectIssue(issue)
	m.repairs[issue] = map[string]entity.IssueRepair{"first": {Issue: issue, ExecutorID: "first", Version: 1, Metadata: map[string]string{"state": "before crash"}}}
	m.executor.EXPECT().Submit(gomock.Any(), []entity.OpenIssue{issue}).Return(accepted([]entity.OpenIssue{issue}, "fix"), nil)
	require.NoError(t, c.processBuild(context.Background(), testQueue, testBuildID))
	require.Equal(t, "fix", m.repairs[issue]["first"].FixID)
	require.Equal(t, map[string]string{"state": "before crash"}, m.repairs[issue]["first"].Metadata)
}
