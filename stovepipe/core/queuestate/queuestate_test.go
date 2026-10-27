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

package queuestate

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type stateFixture struct {
	manager                                               *transitionController
	state                                                 entity.QueueState
	queue                                                 entity.Queue
	summary                                               entity.QueueSummary
	summaryCreateErr, summaryUpdateErr                    error
	logs                                                  map[string]entity.QueueLog
	source                                                *stateSourceFactory
	clock                                                 int64
	getErr, createErr, updateErr, logCreateErr, logGetErr error
	commitOnError                                         bool
	beforeUpdate                                          func()
}
type stateStorage struct {
	storage.Storage
	f *stateFixture
}

func (s stateStorage) GetQueueStore() storage.QueueStore { return memoryStateStore{s.f} }
func (s stateStorage) GetQueueSummaryStore() storage.QueueSummaryStore {
	return memorySummaryStore{s.f}
}
func (s stateStorage) GetQueueLogStore() storage.QueueLogStore {
	return memoryStateLogStore{s.f}
}

type stateStorageFactory struct{ stores storage.Storage }

func (f stateStorageFactory) For(storage.Config) (storage.Storage, error) { return f.stores, nil }

type stateSourceFactory struct {
	sc    sourcecontrol.SourceControl
	calls int
	err   error
}

func (f *stateSourceFactory) For(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	f.calls++
	return f.sc, f.err
}

type fixedLatestSource struct {
	sourcecontrol.SourceControl
	tip string
}

func (s fixedLatestSource) Latest(context.Context) (string, error) { return s.tip, nil }

type memoryStateStore struct{ f *stateFixture }

func (s memoryStateStore) Get(_ context.Context, name string) (entity.Queue, error) {
	if s.f.getErr != nil {
		return entity.Queue{}, s.f.getErr
	}
	if s.f.queue.Version == 0 {
		return entity.Queue{}, storage.ErrNotFound
	}
	queue := s.f.queue
	if s.f.state.Queue != "" {
		queue.Name = s.f.state.Queue
	}
	queue.Enablement, queue.EnablementVersion = s.f.state.State, s.f.state.Version
	queue.EffectiveFromCommitURI, queue.EnablementChangedAtMs = s.f.state.EffectiveFromCommitURI, s.f.state.ChangedAtMs
	return queue, nil
}
func (s memoryStateStore) Create(_ context.Context, queue entity.Queue) error {
	if s.f.createErr != nil {
		return s.f.createErr
	}
	if s.f.queue.Version != 0 {
		return storage.ErrAlreadyExists
	}
	s.f.queue, s.f.state = queue, queue.EnablementState()
	return nil
}
func (s memoryStateStore) Update(_ context.Context, queue entity.Queue, oldVersion, newVersion int32) error {
	if hook := s.f.beforeUpdate; hook != nil {
		s.f.beforeUpdate = nil
		hook()
	}
	if s.f.queue.Version != oldVersion {
		return storage.ErrVersionMismatch
	}
	if s.f.updateErr != nil && !s.f.commitOnError {
		return s.f.updateErr
	}
	queue.Version = newVersion
	s.f.queue, s.f.state = queue, queue.EnablementState()
	return s.f.updateErr
}

type memorySummaryStore struct{ f *stateFixture }

func (s memorySummaryStore) Get(context.Context) (entity.QueueSummary, error) {
	if s.f.summary.Version == 0 {
		return entity.QueueSummary{}, storage.ErrNotFound
	}
	return s.f.summary, nil
}
func (s memorySummaryStore) Create(_ context.Context, summary entity.QueueSummary) error {
	if s.f.summaryCreateErr != nil {
		return s.f.summaryCreateErr
	}
	if s.f.summary.Version != 0 {
		return storage.ErrAlreadyExists
	}
	s.f.summary = summary
	return nil
}
func (s memorySummaryStore) Update(_ context.Context, summary entity.QueueSummary, oldVersion, newVersion int64) error {
	if s.f.summaryUpdateErr != nil {
		return s.f.summaryUpdateErr
	}
	if s.f.summary.Version != oldVersion {
		return storage.ErrVersionMismatch
	}
	summary.Version = newVersion
	s.f.summary = summary
	return nil
}

type memoryStateLogStore struct{ f *stateFixture }

func (s memoryStateLogStore) Get(_ context.Context, id string) (entity.QueueLog, error) {
	if s.f.logGetErr != nil {
		return entity.QueueLog{}, s.f.logGetErr
	}
	log, ok := s.f.logs[id]
	if !ok {
		return entity.QueueLog{}, storage.ErrNotFound
	}
	return log, nil
}
func (s memoryStateLogStore) Create(_ context.Context, log entity.QueueLog) error {
	if s.f.logCreateErr != nil {
		return s.f.logCreateErr
	}
	if _, ok := s.f.logs[log.ID]; ok {
		return storage.ErrAlreadyExists
	}
	s.f.logs[log.ID] = log
	return nil
}
func newStateFixture() *stateFixture {
	f := &stateFixture{logs: make(map[string]entity.QueueLog), source: &stateSourceFactory{}, clock: 1000}
	f.manager = NewTransitionController(stateStorageFactory{stateStorage{f: f}}, f.source, queuelog.NewMaterializer(tally.NoopScope)).(*transitionController)
	f.manager.now = func() time.Time { return time.UnixMilli(f.clock) }
	f.setTip("A")
	return f
}
func (f *stateFixture) setTip(tip string) {
	history := []string{"G", "F", "E", "D", "C", "B", "A", "before"}
	for i, uri := range history {
		if uri == tip {
			f.source.sc = scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, history[i:])
			return
		}
	}
	f.source.sc = scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{tip})
}
func (f *stateFixture) applyState(t *testing.T, state entity.QueueEnablement, tip string) (PreparedChange, entity.QueueState) {
	t.Helper()
	f.setTip(tip)
	change, err := f.manager.Prepare(context.Background(), "repo/main", state, tip)
	require.NoError(t, err)
	applied, err := f.manager.Apply(context.Background(), change)
	require.NoError(t, err)
	return change, applied
}
func initializeFixture(t *testing.T, f *stateFixture) entity.QueueState {
	t.Helper()
	state, err := f.manager.Initialize(context.Background(), "repo/main")
	require.NoError(t, err)
	return state
}

func TestStateInitializationPreservesStateAndRepairsHistory(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "history write interrupted"}[partial], func(t *testing.T) {
			f := newStateFixture()
			if partial {
				f.logCreateErr = errors.New("history unavailable")
				_, err := f.manager.Initialize(context.Background(), "repo/main")
				require.ErrorIs(t, err, f.logCreateErr)
				require.Equal(t, int64(1), f.state.Version)
				require.Empty(t, f.logs)
				f.logCreateErr = nil
				f.clock = 9000
			}
			initial := initializeFixture(t, f)
			require.Equal(t, int64(1000), initial.ChangedAtMs)
			require.Len(t, f.logs, 1)
			_, enabled := f.applyState(t, entity.QueueEnablementEnabled, "A")
			require.Equal(t, enabled, initializeFixture(t, f))
		})
	}
}

func TestStateCommittedHistoryAndPreparedReplay(t *testing.T) {
	f := newStateFixture()
	initializeFixture(t, f)
	first, enabled := f.applyState(t, entity.QueueEnablementEnabled, "A")
	_, disabled := f.applyState(t, entity.QueueEnablementDisabled, "D")
	_, reenabled := f.applyState(t, entity.QueueEnablementEnabled, "G")
	require.Equal(t, int64(2), enabled.Version)
	require.Equal(t, int64(3), disabled.Version)
	require.Equal(t, int64(4), reenabled.Version)
	replay, err := f.manager.Apply(context.Background(), first)
	require.NoError(t, err)
	require.Equal(t, enabled, replay)
	require.Equal(t, reenabled, f.state)
	require.Len(t, f.logs, 4)
}

func TestStateNoOpPreservesBoundaryAndReplays(t *testing.T) {
	f := newStateFixture()
	initial := initializeFixture(t, f)
	noop, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementDisabled, "A")
	require.NoError(t, err)
	require.Zero(t, f.source.calls)
	state, err := f.manager.Apply(context.Background(), noop)
	require.NoError(t, err)
	require.Equal(t, initial, state)
	f.applyState(t, entity.QueueEnablementEnabled, "A")
	replay, err := f.manager.Apply(context.Background(), noop)
	require.NoError(t, err)
	require.Equal(t, initial, replay)
	require.Len(t, f.logs, 2)
}

func TestStatePartialLogWriteBlocksNextTransition(t *testing.T) {
	f := newStateFixture()
	initializeFixture(t, f)
	change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	f.beforeUpdate = func() { f.logCreateErr = errors.New("history unavailable") }
	_, err = f.manager.Apply(context.Background(), change)
	require.ErrorIs(t, err, f.logCreateErr)
	require.Equal(t, int64(2), f.state.Version)
	require.Len(t, f.logs, 1)
	_, err = f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementDisabled, "A")
	require.ErrorIs(t, err, f.logCreateErr)
	require.Equal(t, int64(2), f.state.Version)
	f.logCreateErr = nil
	f.clock = 9000
	noop, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	_, err = f.manager.Apply(context.Background(), noop)
	require.NoError(t, err)
	replay, err := f.manager.Apply(context.Background(), change)
	require.NoError(t, err)
	require.Equal(t, int64(1000), replay.ChangedAtMs)
	require.Len(t, f.logs, 2)
	f.applyState(t, entity.QueueEnablementDisabled, "D")
	require.Len(t, f.logs, 3)
}

func TestStateUncertainWriteReconciliation(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed before commit", true: "committed before connection failed"}[committed], func(t *testing.T) {
			f := newStateFixture()
			initializeFixture(t, f)
			change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
			require.NoError(t, err)
			f.updateErr = errors.New("connection lost")
			f.commitOnError = committed
			state, err := f.manager.Apply(context.Background(), change)
			if committed {
				require.NoError(t, err)
				require.Equal(t, int64(2), state.Version)
			} else {
				require.ErrorIs(t, err, f.updateErr)
				require.Equal(t, int64(1), f.state.Version)
				require.Len(t, f.logs, 1)
			}
			f.updateErr = nil
			f.clock = 9999
			f.setTip("D")
			state, err = f.manager.Apply(context.Background(), change)
			require.NoError(t, err)
			require.Equal(t, "A", state.EffectiveFromCommitURI)
			require.Equal(t, int64(1000), state.ChangedAtMs)
			require.Len(t, f.logs, 2)
		})
	}
}

func TestStateCompetingPreparedChanges(t *testing.T) {
	f := newStateFixture()
	initializeFixture(t, f)
	first, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	f.setTip("B")
	second, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "B")
	require.NoError(t, err)
	f.beforeUpdate = func() { _, err := f.manager.Apply(context.Background(), second); require.NoError(t, err) }
	_, err = f.manager.Apply(context.Background(), first)
	require.ErrorIs(t, err, storage.ErrVersionMismatch)
	require.Equal(t, "B", f.state.EffectiveFromCommitURI)
	require.Len(t, f.logs, 2)
	_, err = f.manager.Apply(context.Background(), first)
	require.ErrorIs(t, err, storage.ErrVersionMismatch)
}

func TestStateUsesSuppliedBoundaryWhenHeadAdvances(t *testing.T) {
	f := newStateFixture()
	initializeFixture(t, f)
	f.setTip("D")
	change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	applied, err := f.manager.Apply(context.Background(), change)
	require.NoError(t, err)
	require.Equal(t, "A", applied.EffectiveFromCommitURI)
}

func TestStateBoundaryAndDecisionValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tip   string
		state entity.QueueEnablement
		want  error
	}{
		{"equal boundary", "A", entity.QueueEnablementDisabled, nil},
		{"forward boundary", "D", entity.QueueEnablementDisabled, nil},
		{"empty boundary", "", entity.QueueEnablementDisabled, ErrInvalidChange},
		{"backward boundary", "before", entity.QueueEnablementDisabled, ErrInvalidChange},
		{"rewritten lineage", "other", entity.QueueEnablementDisabled, sourcecontrol.ErrNotFound},
		{"unknown enablement", "D", entity.QueueEnablementUnknown, ErrInvalidChange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStateFixture()
			initializeFixture(t, f)
			f.applyState(t, entity.QueueEnablementEnabled, "A")
			f.setTip(tc.tip)
			if tc.tip == "before" {
				f.source.sc = fixedLatestSource{SourceControl: scfake.New(sourcecontrol.Config{QueueName: "repo/main"}, []string{"G", "A", "before"}), tip: "before"}
			}
			change, err := f.manager.Prepare(context.Background(), "repo/main", tc.state, tc.tip)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, int64(2), f.state.Version)
				return
			}
			require.NoError(t, err)
			_, err = f.manager.Apply(context.Background(), change)
			require.NoError(t, err)
			require.Equal(t, int64(3), f.state.Version)
		})
	}
	f := newStateFixture()
	initializeFixture(t, f)
	change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	f.setTip("other")
	_, err = f.manager.Apply(context.Background(), change)
	require.ErrorIs(t, err, sourcecontrol.ErrNotFound)
	require.Equal(t, int64(1), f.state.Version)
}

func TestStateFailuresAndHistoryCorruption(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(*stateFixture)
	}{
		{"conflicting current log", func(f *stateFixture) {
			log := f.logs[entity.QueueLogID(1)]
			log.TimestampMs++
			f.logs[log.ID] = log
		}},
		{"wrong queue", func(f *stateFixture) { f.state.Queue = "other" }},
		{"invalid initial state", func(f *stateFixture) { f.state.State = entity.QueueEnablementEnabled }},
		{"invalid version", func(f *stateFixture) { f.state.Version = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStateFixture()
			initializeFixture(t, f)
			tc.corrupt(f)
			_, err := f.manager.Initialize(context.Background(), "repo/main")
			require.ErrorIs(t, err, queuelog.ErrInconsistentHistory)
		})
	}
	f := newStateFixture()
	initializeFixture(t, f)
	_, err := queuelog.LoadVersion(context.Background(), memoryStateLogStore{f}, "repo/main", 2)
	require.ErrorIs(t, err, queuelog.ErrInconsistentHistory)
	require.False(t, storage.IsNotFound(err))
	f.state.Version = math.MaxInt64
	f.state.EffectiveFromCommitURI = "A"
	_, err = f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.ErrorIs(t, err, ErrInvalidChange)
	_, err = f.manager.Apply(context.Background(), PreparedChange{})
	require.ErrorIs(t, err, ErrInvalidChange)
	_, err = f.manager.Initialize(context.Background(), "")
	require.ErrorIs(t, err, ErrInvalidChange)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.manager.Initialize(ctx, "repo/main")
	require.ErrorIs(t, err, context.Canceled)
}

func TestStateDependencyFailuresRemainVisible(t *testing.T) {
	failure := errors.New("dependency unavailable")
	f := newStateFixture()
	f.createErr = failure
	_, err := f.manager.Initialize(context.Background(), "repo/main")
	require.ErrorIs(t, err, failure)
	f.createErr = nil
	initializeFixture(t, f)
	f.getErr = failure
	_, err = f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.ErrorIs(t, err, failure)
	f.getErr = nil
	change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	f.source.err = failure
	_, err = f.manager.Apply(context.Background(), change)
	require.ErrorIs(t, err, failure)
	f.source.err = nil
	f.logGetErr = failure
	_, err = f.manager.Initialize(context.Background(), "repo/main")
	require.ErrorIs(t, err, failure)
}

func TestStateChangesPreserveOperationalQueueUpdates(t *testing.T) {
	for _, duringWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "between prepare and apply", true: "CAS conflict during apply"}[duringWrite], func(t *testing.T) {
			f := newStateFixture()
			initializeFixture(t, f)
			change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
			require.NoError(t, err)
			mutateQueue := func() {
				f.queue.Version++
				f.queue.LastGreenURI = "green"
				f.queue.InFlightRequestIDs = []string{"6"}
				f.queue.LatestRequestID = "7"
			}
			if duringWrite {
				f.beforeUpdate = mutateQueue
			} else {
				mutateQueue()
			}
			applied, err := f.manager.Apply(context.Background(), change)
			require.NoError(t, err)
			require.Equal(t, int64(2), applied.Version)
			require.Equal(t, int32(3), f.queue.Version)
			require.Equal(t, "green", f.queue.LastGreenURI)
			require.Equal(t, []string{"6"}, f.queue.InFlightRequestIDs)
			require.Equal(t, "7", f.queue.LatestRequestID)
			require.Equal(t, applied, f.summary.Snapshot())
			require.Len(t, f.logs, 2)
		})
	}
}

func TestStateInitializationAdoptsExistingOperationalQueue(t *testing.T) {
	f := newStateFixture()
	f.queue = entity.Queue{Name: "repo/main", Version: 12, InFlightRequestIDs: []string{"8", "9"}, LastGreenURI: "green", LatestRequestID: "9"}
	initial := initializeFixture(t, f)
	require.Equal(t, int64(1), initial.Version)
	require.Equal(t, int32(13), f.queue.Version)
	require.Equal(t, []string{"8", "9"}, f.queue.InFlightRequestIDs)
	require.Equal(t, "green", f.queue.LastGreenURI)
	require.Equal(t, "9", f.queue.LatestRequestID)
	require.Equal(t, initial, f.summary.Snapshot())
}

func TestStateProjectionFailureDoesNotLoseAppliedState(t *testing.T) {
	f := newStateFixture()
	initial := initializeFixture(t, f)
	change, err := f.manager.Prepare(context.Background(), "repo/main", entity.QueueEnablementEnabled, "A")
	require.NoError(t, err)
	f.beforeUpdate = func() { f.summaryUpdateErr = errors.New("projection unavailable") }
	_, err = f.manager.Apply(context.Background(), change)
	require.ErrorIs(t, err, f.summaryUpdateErr)
	require.Equal(t, int64(2), f.state.Version)
	require.Equal(t, initial, f.summary.Snapshot())
	require.Len(t, f.logs, 2)
	f.summaryUpdateErr = nil
	replay, err := f.manager.Apply(context.Background(), change)
	require.NoError(t, err)
	require.Equal(t, f.state, replay)
	require.Equal(t, replay, f.summary.Snapshot())
}
