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

package queuelog

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type materializerFixture struct {
	m                          Materializer
	stores                     *storagemock.MockStorage
	logs                       map[string]entity.QueueLog
	summary                    entity.QueueSummary
	logErr, summaryErr         error
	beforeCreate, beforeUpdate func()
}

func newMaterializerFixture(t *testing.T) *materializerFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &materializerFixture{m: NewMaterializer(tally.NoopScope), stores: storagemock.NewMockStorage(ctrl), logs: make(map[string]entity.QueueLog)}
	logs, summaries := storagemock.NewMockQueueLogStore(ctrl), storagemock.NewMockQueueSummaryStore(ctrl)
	f.stores.EXPECT().GetQueueLogStore().Return(logs).AnyTimes()
	f.stores.EXPECT().GetQueueSummaryStore().Return(summaries).AnyTimes()
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.QueueLog) error {
		if f.logErr != nil {
			return f.logErr
		}
		if _, exists := f.logs[log.ID]; exists {
			return storage.ErrAlreadyExists
		}
		f.logs[log.ID] = log
		return nil
	}).AnyTimes()
	logs.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, id string) (entity.QueueLog, error) {
		log, ok := f.logs[id]
		if !ok {
			return entity.QueueLog{}, storage.ErrNotFound
		}
		return log, nil
	}).AnyTimes()
	summaries.EXPECT().Get(gomock.Any()).DoAndReturn(func(context.Context) (entity.QueueSummary, error) {
		if f.summary.Version == 0 {
			return entity.QueueSummary{}, storage.ErrNotFound
		}
		return f.summary, nil
	}).AnyTimes()
	summaries.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.QueueSummary) error {
		if f.beforeCreate != nil {
			hook := f.beforeCreate
			f.beforeCreate = nil
			hook()
		}
		if f.summaryErr != nil {
			return f.summaryErr
		}
		if f.summary.Version != 0 {
			return storage.ErrAlreadyExists
		}
		require.Equal(t, summary.Snapshot(), f.logs[entity.QueueLogID(summary.QueueStateVersion)].Snapshot())
		f.summary = summary
		return nil
	}).AnyTimes()
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.QueueSummary, oldVersion, newVersion int64) error {
		if f.beforeUpdate != nil {
			hook := f.beforeUpdate
			f.beforeUpdate = nil
			hook()
		}
		if f.summaryErr != nil {
			return f.summaryErr
		}
		if f.summary.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		require.Equal(t, oldVersion, summary.Version)
		require.Equal(t, summary.Snapshot(), f.logs[entity.QueueLogID(summary.QueueStateVersion)].Snapshot())
		summary.Version = newVersion
		f.summary = summary
		return nil
	}).AnyTimes()
	return f
}

func testQueueLog(version int64) entity.QueueLog {
	log := entity.QueueLog{ID: entity.QueueLogID(version), Queue: "repo/main", TimestampMs: version * 1000, QueueStateVersion: version, State: entity.QueueEnablementDisabled}
	if version > 1 {
		log.EffectiveFromCommitURI = "A"
	}
	if version%2 == 0 {
		log.State = entity.QueueEnablementEnabled
	}
	return log
}

func TestMaterializerReplayDoesNotRegressSummary(t *testing.T) {
	f := newMaterializerFixture(t)
	for _, v := range []int64{1, 2, 3, 2, 1, 3} {
		require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(v)))
	}
	require.Len(t, f.logs, 3)
	require.Equal(t, testQueueLog(3).Snapshot(), f.summary.Snapshot())
	require.Equal(t, int64(3), f.summary.Version)
}

func TestMaterializerRecoversAfterProjectionFailure(t *testing.T) {
	for _, version := range []int64{1, 2} {
		t.Run(map[int64]string{1: "creation", 2: "update"}[version], func(t *testing.T) {
			f := newMaterializerFixture(t)
			if version == 2 {
				require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(1)))
			}
			log := testQueueLog(version)
			f.summaryErr = errors.New("projection unavailable")
			require.ErrorIs(t, f.m.PersistLog(context.Background(), f.stores, log), f.summaryErr)
			require.Equal(t, log, f.logs[log.ID])
			require.Equal(t, version-1, f.summary.QueueStateVersion)
			f.summaryErr = nil
			require.NoError(t, f.m.PersistLog(context.Background(), f.stores, log))
			require.Equal(t, log.Snapshot(), f.summary.Snapshot())
		})
	}
}

func TestMaterializerConvergesAfterProjectionRaces(t *testing.T) {
	for _, creation := range []bool{true, false} {
		t.Run(map[bool]string{true: "create race", false: "update race"}[creation], func(t *testing.T) {
			f := newMaterializerFixture(t)
			if creation {
				f.beforeCreate = func() { require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(3))) }
			} else {
				require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(1)))
				f.beforeUpdate = func() { require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(3))) }
			}
			require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(2)))
			require.Equal(t, testQueueLog(3).Snapshot(), f.summary.Snapshot())
		})
	}
}

func TestMaterializerConflictsAndStorageErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(*materializerFixture)
		want    error
	}{
		{"conflicting retained log", func(f *materializerFixture) {
			log := f.logs[entity.QueueLogID(2)]
			log.TimestampMs++
			f.logs[log.ID] = log
		}, ErrInconsistentHistory},
		{"conflicting summary", func(f *materializerFixture) { f.summary.StateTimestampMs++ }, ErrInconsistentHistory},
		{"wrong queue summary", func(f *materializerFixture) { f.summary.Queue = "other" }, ErrInconsistentHistory},
		{"invalid summary version", func(f *materializerFixture) { f.summary.Version = -1 }, ErrInconsistentHistory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMaterializerFixture(t)
			require.NoError(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(2)))
			tc.corrupt(f)
			require.ErrorIs(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(2)), tc.want)
		})
	}
	f := newMaterializerFixture(t)
	f.logErr = errors.New("history unavailable")
	require.ErrorIs(t, f.m.PersistLog(context.Background(), f.stores, testQueueLog(1)), f.logErr)
	require.Empty(t, f.logs)
	require.Zero(t, f.summary.Version)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, f.m.PersistLog(ctx, f.stores, testQueueLog(1)), context.Canceled)
}
