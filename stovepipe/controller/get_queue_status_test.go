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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/uber/submitqueue/platform/errs"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	scmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type queueStatusFixture struct {
	mockController      *gomock.Controller
	c                   *getQueueStatusController
	store               *storagemock.MockQueueSummaryStore
	logStore            *storagemock.MockQueueLogStore
	scFactory           *scmock.MockFactory
	current             entity.QueueState
	logs                map[string]entity.QueueLog
	stateErr, logGetErr error
}

func newQueueStatusFixture(t *testing.T) *queueStatusFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &queueStatusFixture{mockController: ctrl, store: storagemock.NewMockQueueSummaryStore(ctrl),
		logStore: storagemock.NewMockQueueLogStore(ctrl), scFactory: scmock.NewMockFactory(ctrl), logs: make(map[string]entity.QueueLog)}
	factory := storagemock.NewMockFactory(ctrl)
	stores := storagemock.NewMockStorage(ctrl)
	factory.EXPECT().For(storage.Config{QueueName: "repo/main"}).Return(stores, nil).AnyTimes()
	stores.EXPECT().GetQueueSummaryStore().Return(f.store).AnyTimes()
	stores.EXPECT().GetQueueLogStore().Return(f.logStore).AnyTimes()
	f.c = NewGetQueueStatusController(tally.NoopScope, factory, f.scFactory).(*getQueueStatusController)
	f.addState(entity.QueueEnablementDisabled, "")
	f.store.EXPECT().Get(gomock.Any()).DoAndReturn(func(context.Context) (entity.QueueSummary, error) {
		if f.stateErr != nil {
			return entity.QueueSummary{}, f.stateErr
		}
		if f.current.Queue == "" {
			return entity.QueueSummary{}, storage.ErrNotFound
		}
		return entity.QueueSummary{Queue: f.current.Queue, State: f.current.State, QueueStateVersion: f.current.Version, EffectiveFromCommitURI: f.current.EffectiveFromCommitURI, StateTimestampMs: f.current.ChangedAtMs, Version: f.current.Version + 100}, nil
	}).AnyTimes()
	f.logStore.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.QueueLog, error) {
		if f.logGetErr != nil {
			return entity.QueueLog{}, f.logGetErr
		}
		log, ok := f.logs[id]
		if !ok {
			return entity.QueueLog{}, storage.ErrNotFound
		}
		return log, nil
	}).AnyTimes()
	return f
}
func (f *queueStatusFixture) addState(state entity.QueueEnablement, boundary string) {
	version := f.current.Version + 1
	f.current = entity.QueueState{Queue: "repo/main", State: state, EffectiveFromCommitURI: boundary, ChangedAtMs: version * 1000, Version: version}
	id := entity.QueueLogID(version)
	f.logs[id] = entity.QueueLog{ID: id, Queue: f.current.Queue, State: state, EffectiveFromCommitURI: boundary, TimestampMs: f.current.ChangedAtMs, QueueStateVersion: version}
}
func (f *queueStatusFixture) expectLinearSourceControl() {
	f.scFactory.EXPECT().For(sourcecontrol.Config{QueueName: "repo/main"}).
		Return(scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{"G", "F", "E", "D", "C", "B", "A", "before"}), nil)
}

func TestGetQueueStatusResolvesIntermediateCommitsAndBoundaries(t *testing.T) {
	for _, tt := range []struct {
		commit   string
		revision int64
		state    entity.QueueEnablement
	}{
		{"before", 1, entity.QueueEnablementDisabled},
		{"A", 2, entity.QueueEnablementEnabled}, {"C", 2, entity.QueueEnablementEnabled},
		{"D", 3, entity.QueueEnablementDisabled}, {"F", 3, entity.QueueEnablementDisabled},
		{"G", 4, entity.QueueEnablementEnabled},
	} {
		t.Run(tt.commit, func(t *testing.T) {
			f := newQueueStatusFixture(t)
			f.addState(entity.QueueEnablementEnabled, "A")
			f.addState(entity.QueueEnablementDisabled, "D")
			f.addState(entity.QueueEnablementEnabled, "G")
			// Ignore history newer than the summary.
			f.logs[entity.QueueLogID(5)] = entity.QueueLog{ID: entity.QueueLogID(5), Queue: "repo/main", QueueStateVersion: 5, TimestampMs: 5000, State: entity.QueueEnablementDisabled, EffectiveFromCommitURI: "G"}
			f.expectLinearSourceControl()
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: tt.commit})
			require.NoError(t, err)
			require.Equal(t, "repo/main", result.Queue)
			require.Equal(t, f.logs[entity.QueueLogID(tt.revision)].Snapshot(), result.State)
			require.Equal(t, tt.state, result.State.State)
		})
	}
}

func TestGetQueueStatusNeverEnabledAndSameBoundary(t *testing.T) {
	for _, sameBoundary := range []bool{false, true} {
		t.Run(map[bool]string{false: "never enabled", true: "latest revision at equal boundary"}[sameBoundary], func(t *testing.T) {
			f := newQueueStatusFixture(t)
			if sameBoundary {
				f.addState(entity.QueueEnablementEnabled, "A")
				f.addState(entity.QueueEnablementDisabled, "A")
			}
			f.expectLinearSourceControl()
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"})
			require.NoError(t, err)
			require.Equal(t, f.current, result.State)
			require.Equal(t, entity.QueueEnablementDisabled, result.State.State)
		})
	}
}

func TestGetQueueStatusCurrentOnlyNeedsNoSourceControl(t *testing.T) {
	f := newQueueStatusFixture(t)
	f.addState(entity.QueueEnablementEnabled, "A")
	result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main"})
	require.NoError(t, err)
	require.Equal(t, f.logs[entity.QueueLogID(2)].Snapshot(), result.State)
}

func TestGetQueueStatusPinsStateAcrossConcurrentUpdate(t *testing.T) {
	f := newQueueStatusFixture(t)
	f.addState(entity.QueueEnablementEnabled, "A")
	sc := scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{"G", "F", "D", "C", "A"})
	f.scFactory.EXPECT().For(gomock.Any()).DoAndReturn(func(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
		f.addState(entity.QueueEnablementDisabled, "D")
		return sc, nil
	})
	result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "F"})
	require.NoError(t, err)
	require.Equal(t, f.logs[entity.QueueLogID(2)].Snapshot(), result.State)
	require.Equal(t, int64(3), f.current.Version)
}

func TestGetQueueStatusFailuresNeverReturnDisabled(t *testing.T) {
	dependencyErr := errors.New("source control unavailable")
	for _, tt := range []struct {
		name  string
		setup func(*queueStatusFixture)
		want  error
	}{
		{"unregistered queue", func(f *queueStatusFixture) { f.current = entity.QueueState{} }, ErrQueueStateNotFound},
		{"conflicting current log", func(f *queueStatusFixture) {
			log := f.logs[entity.QueueLogID(1)]
			log.TimestampMs++
			f.logs[log.ID] = log
		}, queuelog.ErrInconsistentHistory},
		{"backward persisted boundary", func(f *queueStatusFixture) {
			f.addState(entity.QueueEnablementEnabled, "G")
			f.addState(entity.QueueEnablementDisabled, "D")
			f.expectLinearSourceControl()
		}, queuelog.ErrInconsistentHistory},
		{"redundant persisted state", func(f *queueStatusFixture) {
			f.addState(entity.QueueEnablementDisabled, "G")
			f.expectLinearSourceControl()
		}, queuelog.ErrInconsistentHistory},
		{"missing predecessor", func(f *queueStatusFixture) {
			f.addState(entity.QueueEnablementEnabled, "G")
			delete(f.logs, entity.QueueLogID(1))
			f.expectLinearSourceControl()
		}, queuelog.ErrInconsistentHistory},
		{"unknown commit", func(f *queueStatusFixture) { f.expectLinearSourceControl() }, ErrCommitStateUnresolved},
		{"source dependency", func(f *queueStatusFixture) { f.scFactory.EXPECT().For(gomock.Any()).Return(nil, dependencyErr) }, dependencyErr},
		{"rewritten policy lineage", func(f *queueStatusFixture) {
			f.addState(entity.QueueEnablementEnabled, "old-lineage")
			sc := scmock.NewMockSourceControl(f.mockController)
			sc.EXPECT().Latest(gomock.Any()).Return("new-tip", nil)
			sc.EXPECT().IsAncestor(gomock.Any(), "F", "new-tip").Return(true, nil)
			sc.EXPECT().IsAncestor(gomock.Any(), "old-lineage", "new-tip").Return(false, nil)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
		}, ErrCommitStateUnresolved},
		{"unrelated commit", func(f *queueStatusFixture) {
			sc := scmock.NewMockSourceControl(f.mockController)
			sc.EXPECT().Latest(gomock.Any()).Return("tip", nil)
			sc.EXPECT().IsAncestor(gomock.Any(), "F", "tip").Return(false, nil)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
		}, ErrCommitStateUnresolved},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newQueueStatusFixture(t)
			tt.setup(f)
			commit := "F"
			if tt.name == "backward persisted boundary" {
				commit = "B"
			}
			if tt.name == "unknown commit" {
				commit = "unknown"
			}
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: commit})
			require.ErrorIs(t, err, tt.want)
			require.Equal(t, entity.GetQueueStatusResult{}, result)
		})
	}
}

func TestGetQueueStatusRejectsIncomparableCommits(t *testing.T) {
	for _, tt := range []struct {
		name          string
		olderBoundary bool
	}{
		{"latest boundary", false},
		{"older boundary", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newQueueStatusFixture(t)
			f.addState(entity.QueueEnablementEnabled, "A")
			if tt.olderBoundary {
				f.addState(entity.QueueEnablementDisabled, "D")
			}
			sc := scmock.NewMockSourceControl(f.mockController)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
			calls := []any{
				sc.EXPECT().Latest(gomock.Any()).Return("M", nil),
				sc.EXPECT().IsAncestor(gomock.Any(), "C", "M").Return(true, nil),
				sc.EXPECT().IsAncestor(gomock.Any(), f.current.EffectiveFromCommitURI, "M").Return(true, nil),
			}
			if tt.olderBoundary {
				calls = append(calls,
					sc.EXPECT().IsAncestor(gomock.Any(), "D", "C").Return(false, nil),
					sc.EXPECT().IsAncestor(gomock.Any(), "C", "D").Return(true, nil),
					sc.EXPECT().IsAncestor(gomock.Any(), "A", "D").Return(true, nil),
				)
			}
			calls = append(calls,
				sc.EXPECT().IsAncestor(gomock.Any(), "A", "C").Return(false, nil),
				sc.EXPECT().IsAncestor(gomock.Any(), "C", "A").Return(false, nil),
			)
			gomock.InOrder(calls...)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"})
			require.ErrorIs(t, err, ErrCommitStateUnresolved)
			require.Equal(t, entity.GetQueueStatusResult{}, result)
		})
	}
}

func TestGetQueueStatusAncestryFailuresArePreserved(t *testing.T) {
	dependency := errors.New("comparison unavailable")
	for _, failAt := range []int{0, 1, 2, 3} {
		t.Run([]string{"commit membership", "policy lineage", "commit coverage", "reverse ancestry"}[failAt], func(t *testing.T) {
			f := newQueueStatusFixture(t)
			f.addState(entity.QueueEnablementEnabled, "A")
			sc := scmock.NewMockSourceControl(f.mockController)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
			sc.EXPECT().Latest(gomock.Any()).Return("tip", nil)
			calls := [][2]string{{"F", "tip"}, {"A", "tip"}, {"A", "F"}, {"F", "A"}}
			expected := make([]any, 0, failAt+1)
			for i := 0; i <= failAt; i++ {
				call := sc.EXPECT().IsAncestor(gomock.Any(), calls[i][0], calls[i][1])
				if i == failAt {
					call.Return(false, dependency)
				} else if i == 2 {
					call.Return(false, nil)
				} else {
					call.Return(true, nil)
				}
				expected = append(expected, call)
			}
			gomock.InOrder(expected...)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "F"})
			require.ErrorIs(t, err, dependency)
			require.NotErrorIs(t, err, ErrCommitStateUnresolved)
			require.Equal(t, entity.GetQueueStatusResult{}, result)
		})
	}
}

func TestGetQueueStatusValidationAndCancellation(t *testing.T) {
	for _, req := range []entity.GetQueueStatusRequest{{}, {Queue: strings.Repeat("x", 256)}, {Queue: "repo/main", ChangeURI: strings.Repeat("x", 256)}} {
		f := newQueueStatusFixture(t)
		_, err := f.c.GetQueueStatus(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
	for _, cancelErr := range []error{context.Canceled, context.DeadlineExceeded} {
		f := newQueueStatusFixture(t)
		f.scFactory.EXPECT().For(gomock.Any()).Return(nil, cancelErr)
		result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"})
		require.ErrorIs(t, err, cancelErr)
		require.Equal(t, entity.GetQueueStatusResult{}, result)
	}
}

func TestGetQueueStatusCurrentOnlyPreservesCancellation(t *testing.T) {
	f := newQueueStatusFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := f.c.GetQueueStatus(ctx, entity.GetQueueStatusRequest{Queue: "repo/main"})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, entity.GetQueueStatusResult{}, result)
}

func TestGetQueueStatusMissingCurrentHistoryDoesNotWrite(t *testing.T) {
	f := newQueueStatusFixture(t)
	f.addState(entity.QueueEnablementEnabled, "A")
	delete(f.logs, entity.QueueLogID(f.current.Version))
	result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main"})
	require.ErrorIs(t, err, queuelog.ErrInconsistentHistory)
	require.Equal(t, entity.GetQueueStatusResult{}, result)
	require.Len(t, f.logs, 1)
}

func TestGetQueueStatusStorageFailures(t *testing.T) {
	failure := errors.New("storage unavailable")
	for _, tc := range []struct {
		name  string
		setup func(*queueStatusFixture)
	}{
		{"snapshot read", func(f *queueStatusFixture) { f.stateErr = failure }},
		{"history reconciliation", func(f *queueStatusFixture) { f.logGetErr = failure }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newQueueStatusFixture(t)
			tc.setup(f)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main"})
			require.ErrorIs(t, err, failure)
			require.Equal(t, entity.GetQueueStatusResult{}, result)
		})
	}
}

func TestInvalidRequestClassificationMatchesDomainCause(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		invalid bool
	}{
		{"invalid selector", ErrInvalidRequest, true},
		{"wrapped invalid selector", fmt.Errorf("selector: %w", ErrInvalidRequest), true},
		{"unregistered queue", errs.NewUserError(ErrQueueStateNotFound), false},
		{"unresolved commit", errs.NewUserError(ErrCommitStateUnresolved), false},
		{"other user error", errs.NewUserError(errors.New("other")), false},
		{"no error", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.invalid, IsInvalidRequest(tc.err)) })
	}
}
