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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/queuepolicy"
	"github.com/uber/submitqueue/stovepipe/entity"
	executionmock "github.com/uber/submitqueue/stovepipe/extension/queueexecution/mock"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	scmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type queueStatusFixture struct {
	mockController *gomock.Controller
	c              *getQueueStatusController
	store          *storagemock.MockQueuePolicyStore
	scFactory      *scmock.MockFactory
	execution      *executionmock.MockReader
	transitions    map[string]entity.QueuePolicyTransition
	head           entity.QueuePolicyHead
}

func newQueueStatusFixture(t *testing.T) *queueStatusFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &queueStatusFixture{mockController: ctrl, store: storagemock.NewMockQueuePolicyStore(ctrl), scFactory: scmock.NewMockFactory(ctrl), execution: executionmock.NewMockReader(ctrl), transitions: make(map[string]entity.QueuePolicyTransition)}
	factory := storagemock.NewMockFactory(ctrl)
	stores := storagemock.NewMockStorage(ctrl)
	factory.EXPECT().For(storage.Config{QueueName: "repo/main"}).Return(stores, nil).AnyTimes()
	stores.EXPECT().GetQueuePolicyStore().Return(f.store).AnyTimes()
	f.c = NewGetQueueStatusController(zap.NewNop().Sugar(), tally.NoopScope, factory, f.scFactory, f.execution).(*getQueueStatusController)
	f.c.now = func() time.Time { return time.UnixMilli(4000) }
	f.addPolicy("initial", entity.QueuePolicyStateDisabled, "")
	f.store.EXPECT().GetCurrent(gomock.Any()).DoAndReturn(func(context.Context) (entity.QueuePolicyHead, error) {
		if f.head.Queue == "" {
			return f.head, storage.ErrNotFound
		}
		return f.head, nil
	}).AnyTimes()
	f.store.EXPECT().GetTransition(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.QueuePolicyTransition, error) {
		p, ok := f.transitions[id]
		if !ok {
			return p, storage.ErrNotFound
		}
		return p, nil
	}).AnyTimes()
	return f
}
func (f *queueStatusFixture) addPolicy(id string, state entity.QueuePolicyState, boundary string) {
	revision := f.head.Revision + 1
	p := entity.QueuePolicyTransition{ID: id, Queue: "repo/main", PreviousID: f.head.TransitionID, Policy: entity.QueuePolicy{Revision: revision, State: state, EffectiveFromCommitURI: boundary, ChangedAtMs: revision * 1000}}
	f.transitions[id] = p
	f.head = entity.QueuePolicyHead{Queue: "repo/main", TransitionID: id, Revision: revision, Version: revision}
}
func (f *queueStatusFixture) expectLinearSourceControl() {
	f.scFactory.EXPECT().For(sourcecontrol.Config{QueueName: "repo/main"}).Return(scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{"G", "F", "E", "D", "C", "B", "A", "before"}), nil)
}

func TestGetQueueStatusResolvesIntermediateCommitsAndBoundaries(t *testing.T) {
	for _, tt := range []struct {
		commit   string
		revision int64
		state    entity.QueuePolicyState
	}{
		{"before", 1, entity.QueuePolicyStateDisabled},
		{"A", 2, entity.QueuePolicyStateEnabled}, {"C", 2, entity.QueuePolicyStateEnabled},
		{"D", 3, entity.QueuePolicyStateDisabled}, {"F", 3, entity.QueuePolicyStateDisabled},
		{"G", 4, entity.QueuePolicyStateEnabled},
	} {
		t.Run(tt.commit, func(t *testing.T) {
			f := newQueueStatusFixture(t)
			f.addPolicy("on", entity.QueuePolicyStateEnabled, "A")
			f.addPolicy("off", entity.QueuePolicyStateDisabled, "D")
			f.addPolicy("on-again", entity.QueuePolicyStateEnabled, "G")
			// An uncommitted proposal must not affect policy lookup.
			f.transitions["orphan"] = entity.QueuePolicyTransition{ID: "orphan", Policy: entity.QueuePolicy{Revision: 5, State: entity.QueuePolicyStateDisabled, EffectiveFromCommitURI: "G"}}
			f.expectLinearSourceControl()
			f.execution.EXPECT().GetQueueExecutionState(gomock.Any(), "repo/main").Return(entity.QueueExecutionStatePaused, nil)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: tt.commit})
			require.NoError(t, err)
			require.True(t, result.HasPolicyForCommit)
			require.Equal(t, f.transitions["on-again"].Policy, result.CurrentPolicy)
			require.Equal(t, tt.revision, result.PolicyForCommit.Revision)
			require.Equal(t, tt.state, result.PolicyForCommit.State)
			require.Equal(t, entity.QueueExecutionStatus{State: entity.QueueExecutionStatePaused, ObservedAtMs: 4000}, result.Execution)
		})
	}
}

func TestGetQueueStatusNeverEnabledAndSameBoundary(t *testing.T) {
	for _, sameBoundary := range []bool{false, true} {
		t.Run(map[bool]string{false: "never enabled", true: "latest revision at equal boundary"}[sameBoundary], func(t *testing.T) {
			f := newQueueStatusFixture(t)
			if sameBoundary {
				f.addPolicy("on", entity.QueuePolicyStateEnabled, "A")
				f.addPolicy("off", entity.QueuePolicyStateDisabled, "A")
			}
			f.expectLinearSourceControl()
			f.execution.EXPECT().GetQueueExecutionState(gomock.Any(), "repo/main").Return(entity.QueueExecutionStateRunning, nil)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"})
			require.NoError(t, err)
			require.Equal(t, result.CurrentPolicy, result.PolicyForCommit)
			require.Equal(t, entity.QueuePolicyStateDisabled, result.PolicyForCommit.State)
		})
	}
}

func TestGetQueueStatusCurrentOnlyNeedsNoSourceControl(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state entity.QueueExecutionState
		err   error
		want  entity.QueueExecutionState
	}{
		{"running", entity.QueueExecutionStateRunning, nil, entity.QueueExecutionStateRunning},
		{"paused", entity.QueueExecutionStatePaused, nil, entity.QueueExecutionStatePaused},
		{"pause dependency unavailable", entity.QueueExecutionStatePaused, errors.New("flipr unavailable"), entity.QueueExecutionStateUnknown},
		{"unknown execution value", "future-state", nil, entity.QueueExecutionStateUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newQueueStatusFixture(t)
			f.addPolicy("on", entity.QueuePolicyStateEnabled, "A")
			f.execution.EXPECT().GetQueueExecutionState(gomock.Any(), "repo/main").Return(tt.state, tt.err)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main"})
			require.NoError(t, err)
			require.False(t, result.HasPolicyForCommit)
			require.Equal(t, entity.QueuePolicyStateEnabled, result.CurrentPolicy.State)
			require.Equal(t, tt.want, result.Execution.State)
		})
	}
}

func TestGetQueueStatusPinsPolicyAcrossConcurrentUpdate(t *testing.T) {
	f := newQueueStatusFixture(t)
	f.addPolicy("on", entity.QueuePolicyStateEnabled, "A")
	sc := scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{"G", "F", "D", "C", "A"})
	f.scFactory.EXPECT().For(gomock.Any()).DoAndReturn(func(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
		f.addPolicy("off", entity.QueuePolicyStateDisabled, "D")
		return sc, nil
	})
	f.execution.EXPECT().GetQueueExecutionState(gomock.Any(), "repo/main").Return(entity.QueueExecutionStateRunning, nil)
	result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "F"})
	require.NoError(t, err)
	require.Equal(t, int64(2), result.CurrentPolicy.Revision)
	require.Equal(t, result.CurrentPolicy, result.PolicyForCommit)
	require.Equal(t, int64(3), f.head.Revision)
}

func TestGetQueueStatusFailuresNeverReturnDisabled(t *testing.T) {
	dependencyErr := errors.New("code-gateway unavailable")
	for _, tt := range []struct {
		name  string
		setup func(*queueStatusFixture)
		want  error
	}{
		{"unregistered queue", func(f *queueStatusFixture) { f.head = entity.QueuePolicyHead{} }, ErrQueuePolicyNotFound},
		{"missing committed transition", func(f *queueStatusFixture) { delete(f.transitions, "initial") }, queuepolicy.ErrInconsistentHistory},
		{"backward persisted boundary", func(f *queueStatusFixture) {
			f.addPolicy("on", entity.QueuePolicyStateEnabled, "G")
			f.addPolicy("off", entity.QueuePolicyStateDisabled, "D")
			f.expectLinearSourceControl()
		}, queuepolicy.ErrInconsistentHistory},
		{"redundant persisted state", func(f *queueStatusFixture) {
			f.addPolicy("off", entity.QueuePolicyStateDisabled, "G")
			f.expectLinearSourceControl()
		}, queuepolicy.ErrInconsistentHistory},
		{"missing predecessor", func(f *queueStatusFixture) {
			f.addPolicy("on", entity.QueuePolicyStateEnabled, "G")
			delete(f.transitions, "initial")
			f.expectLinearSourceControl()
		}, queuepolicy.ErrInconsistentHistory},
		{"unknown commit", func(f *queueStatusFixture) { f.expectLinearSourceControl() }, ErrCommitPolicyUnresolved},
		{"source dependency", func(f *queueStatusFixture) { f.scFactory.EXPECT().For(gomock.Any()).Return(nil, dependencyErr) }, dependencyErr},
		{"rewritten policy lineage", func(f *queueStatusFixture) {
			f.addPolicy("on", entity.QueuePolicyStateEnabled, "old-lineage")
			sc := scmock.NewMockSourceControl(f.mockController)
			sc.EXPECT().Latest(gomock.Any()).Return("new-tip", nil)
			sc.EXPECT().IsAncestor(gomock.Any(), "F", "new-tip").Return(true, nil)
			sc.EXPECT().IsAncestor(gomock.Any(), "old-lineage", "new-tip").Return(false, nil)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
		}, ErrCommitPolicyUnresolved},
		{"unrelated commit", func(f *queueStatusFixture) {
			sc := scmock.NewMockSourceControl(f.mockController)
			sc.EXPECT().Latest(gomock.Any()).Return("tip", nil)
			sc.EXPECT().IsAncestor(gomock.Any(), "F", "tip").Return(false, nil)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
		}, ErrCommitPolicyUnresolved},
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

func TestGetQueueStatusAncestryFailuresArePreserved(t *testing.T) {
	dependency := errors.New("comparison unavailable")
	for _, failAt := range []int{0, 1, 2} {
		t.Run([]string{"commit membership", "policy lineage", "commit coverage"}[failAt], func(t *testing.T) {
			f := newQueueStatusFixture(t)
			f.addPolicy("on", entity.QueuePolicyStateEnabled, "A")
			sc := scmock.NewMockSourceControl(f.mockController)
			f.scFactory.EXPECT().For(gomock.Any()).Return(sc, nil)
			sc.EXPECT().Latest(gomock.Any()).Return("tip", nil)
			calls := [][2]string{{"F", "tip"}, {"A", "tip"}, {"A", "F"}}
			expected := make([]any, 0, failAt+1)
			for i := 0; i <= failAt; i++ {
				call := sc.EXPECT().IsAncestor(gomock.Any(), calls[i][0], calls[i][1])
				if i == failAt {
					call.Return(false, dependency)
				} else {
					call.Return(true, nil)
				}
				expected = append(expected, call)
			}
			gomock.InOrder(expected...)
			result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "F"})
			require.ErrorIs(t, err, dependency)
			require.NotErrorIs(t, err, ErrCommitPolicyUnresolved)
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
		f.execution.EXPECT().GetQueueExecutionState(gomock.Any(), "repo/main").Return(entity.QueueExecutionStateUnknown, cancelErr)
		result, err := f.c.GetQueueStatus(context.Background(), entity.GetQueueStatusRequest{Queue: "repo/main"})
		require.ErrorIs(t, err, cancelErr)
		require.Equal(t, entity.GetQueueStatusResult{}, result)
	}
}
