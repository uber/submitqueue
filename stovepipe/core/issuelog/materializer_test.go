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

package issuelog

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type logKey struct {
	task, unit, build string
	status            entity.IssueStatus
}

type materializerFixture struct {
	m                          Materializer
	stores                     *storagemock.MockStorage
	logs                       map[logKey]entity.IssueLog
	summary                    entity.IssueSummary
	failures                   map[string]error
	beforeCreate, beforeUpdate func()
}

func newMaterializerFixture(t *testing.T) *materializerFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &materializerFixture{m: NewMaterializer(tally.NoopScope), stores: storagemock.NewMockStorage(ctrl),
		logs: make(map[logKey]entity.IssueLog), failures: make(map[string]error)}
	logs := storagemock.NewMockIssueLogStore(ctrl)
	summaries := storagemock.NewMockIssueSummaryStore(ctrl)
	builds := storagemock.NewMockBuildStore(ctrl)
	f.stores.EXPECT().GetIssueLogStore().Return(logs).AnyTimes()
	f.stores.EXPECT().GetIssueSummaryStore().Return(summaries).AnyTimes()
	f.stores.EXPECT().GetBuildStore().Return(builds).AnyTimes()
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.IssueLog) error {
		if err := f.failures["log/create"]; err != nil {
			return err
		}
		key := logKey{log.TaskID, log.UnitID, log.BuildID, log.Status}
		if _, ok := f.logs[key]; ok {
			return storage.ErrAlreadyExists
		}
		f.logs[key] = log
		return nil
	}).AnyTimes()
	logs.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, task, unit, build string, status entity.IssueStatus) (entity.IssueLog, error) {
		if err := f.failures["log/get"]; err != nil {
			return entity.IssueLog{}, err
		}
		log, ok := f.logs[logKey{task, unit, build, status}]
		if !ok {
			return entity.IssueLog{}, storage.ErrNotFound
		}
		return log, nil
	}).AnyTimes()
	builds.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, buildID string) (entity.Build, error) {
		if err := f.failures["build/get"]; err != nil {
			return entity.Build{}, err
		}
		requestID, ok := map[string]string{"build/z": "9", "build/a": "10", "build/m": "11"}[buildID]
		if !ok {
			return entity.Build{}, storage.ErrNotFound
		}
		return entity.Build{ID: buildID, RequestID: requestID, Version: 1}, nil
	}).AnyTimes()
	summaries.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, string, string) (entity.IssueSummary, error) {
		if err := f.failures["summary/get"]; err != nil {
			return entity.IssueSummary{}, err
		}
		if f.summary.Version == 0 {
			return entity.IssueSummary{}, storage.ErrNotFound
		}
		return f.summary, nil
	}).AnyTimes()
	summaries.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.IssueSummary) error {
		if f.beforeCreate != nil {
			hook := f.beforeCreate
			f.beforeCreate = nil
			hook()
		}
		if err := f.failures["summary/create"]; err != nil {
			return err
		}
		if f.summary.Version != 0 {
			return storage.ErrAlreadyExists
		}
		require.Equal(t, summary.ChangedAtMs, f.logs[logKey{summary.TaskID, summary.UnitID, summary.BuildID, summary.Status}].TimestampMs)
		f.summary = summary
		return nil
	}).AnyTimes()
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.IssueSummary, oldVersion, newVersion int32) error {
		if f.beforeUpdate != nil {
			hook := f.beforeUpdate
			f.beforeUpdate = nil
			hook()
		}
		if err := f.failures["summary/update"]; err != nil {
			return err
		}
		if f.summary.Version != oldVersion {
			return storage.ErrVersionMismatch
		}
		require.Equal(t, oldVersion, summary.Version)
		summary.Version = newVersion
		f.summary = summary
		return nil
	}).AnyTimes()
	return f
}

func testIssueLog(build string, status entity.IssueStatus) entity.IssueLog {
	return entity.IssueLog{TaskID: "validation", UnitID: "unit", BuildID: build, Status: status, TimestampMs: 1000}
}

func TestMaterializerOpenCloseReopenAndReplay(t *testing.T) {
	f := newMaterializerFixture(t)
	opened := testIssueLog("build/z", entity.IssueStatusOpen)
	closed := testIssueLog("build/a", entity.IssueStatusClosed)
	reopened := testIssueLog("build/m", entity.IssueStatusOpen)
	for _, step := range []struct {
		log     entity.IssueLog
		opening string
		status  entity.IssueStatus
		version int32
	}{
		{opened, opened.BuildID, entity.IssueStatusOpen, 1},
		{closed, opened.BuildID, entity.IssueStatusClosed, 2},
		{opened, opened.BuildID, entity.IssueStatusClosed, 2},
		{reopened, reopened.BuildID, entity.IssueStatusOpen, 3},
		{closed, opened.BuildID, entity.IssueStatusOpen, 3},
		{reopened, reopened.BuildID, entity.IssueStatusOpen, 3},
	} {
		require.NoError(t, f.m.PersistLog(context.Background(), f.stores, step.log, step.opening))
		require.Equal(t, step.status, f.summary.Status)
		require.Equal(t, step.version, f.summary.Version)
	}
	require.Equal(t, "11", f.summary.RequestID)
	require.Equal(t, reopened.BuildID, f.summary.FirstSeenBuildID)
	require.Len(t, f.logs, 3)
}

func TestMaterializerCloseWinsWithinRequest(t *testing.T) {
	for _, closeFirst := range []bool{false, true} {
		name := "opening first"
		if closeFirst {
			name = "closing first"
		}
		t.Run(name, func(t *testing.T) {
			f := newMaterializerFixture(t)
			opened := testIssueLog("build/z", entity.IssueStatusOpen)
			closed := testIssueLog("build/z", entity.IssueStatusClosed)
			logs := []entity.IssueLog{opened, closed}
			if closeFirst {
				logs = []entity.IssueLog{closed, opened}
			}
			for _, log := range logs {
				require.NoError(t, f.m.PersistLog(context.Background(), f.stores, log, opened.BuildID))
			}
			require.Equal(t, entity.IssueStatusClosed, f.summary.Status)
			require.Equal(t, "9", f.summary.RequestID)
			require.Equal(t, opened.BuildID, f.summary.FirstSeenBuildID)
		})
	}
}

func TestMaterializerRepairsProjectionAfterFailure(t *testing.T) {
	for _, step := range []string{"summary/create", "summary/update"} {
		t.Run(step, func(t *testing.T) {
			f := newMaterializerFixture(t)
			log := testIssueLog("build/z", entity.IssueStatusOpen)
			opening := log.BuildID
			if step == "summary/update" {
				require.NoError(t, f.m.PersistLog(context.Background(), f.stores, log, opening))
				log = testIssueLog("build/a", entity.IssueStatusClosed)
			}
			f.failures[step] = assert.AnError
			require.ErrorIs(t, f.m.PersistLog(context.Background(), f.stores, log, opening), assert.AnError)
			delete(f.failures, step)
			retry := log
			retry.TimestampMs = 9999
			require.NoError(t, f.m.PersistLog(context.Background(), f.stores, retry, opening))
			require.Equal(t, log.TimestampMs, f.summary.ChangedAtMs)
			require.Equal(t, log.BuildID, f.summary.BuildID)
		})
	}
}

func TestMaterializerReloadsConcurrentWinner(t *testing.T) {
	for _, step := range []string{"create", "update"} {
		t.Run(step, func(t *testing.T) {
			f := newMaterializerFixture(t)
			opened := testIssueLog("build/z", entity.IssueStatusOpen)
			incoming := testIssueLog("build/a", entity.IssueStatusClosed)
			winner := testIssueLog("build/m", entity.IssueStatusOpen)
			if step == "create" {
				f.beforeCreate = func() { require.NoError(t, f.m.PersistLog(context.Background(), f.stores, winner, winner.BuildID)) }
			} else {
				require.NoError(t, f.m.PersistLog(context.Background(), f.stores, opened, opened.BuildID))
				f.beforeUpdate = func() { require.NoError(t, f.m.PersistLog(context.Background(), f.stores, winner, winner.BuildID)) }
			}
			require.NoError(t, f.m.PersistLog(context.Background(), f.stores, incoming, opened.BuildID))
			require.Equal(t, winner.BuildID, f.summary.BuildID)
			require.Equal(t, winner.BuildID, f.summary.FirstSeenBuildID)
			require.Equal(t, entity.IssueStatusOpen, f.summary.Status)
		})
	}
}

func TestMaterializerReturnsStorageErrors(t *testing.T) {
	for _, step := range []string{"log/create", "log/get", "build/get", "summary/get", "summary/create", "summary/update"} {
		t.Run(step, func(t *testing.T) {
			f := newMaterializerFixture(t)
			log := testIssueLog("build/z", entity.IssueStatusOpen)
			opening := log.BuildID
			if step == "log/get" || step == "summary/update" {
				require.NoError(t, f.m.PersistLog(context.Background(), f.stores, log, opening))
			}
			if step == "summary/update" {
				log = testIssueLog("build/a", entity.IssueStatusClosed)
			}
			f.failures[step] = assert.AnError
			require.ErrorIs(t, f.m.PersistLog(context.Background(), f.stores, log, opening), assert.AnError)
		})
	}
}

func TestMaterializerRejectsInvalidTransitions(t *testing.T) {
	for _, scenario := range []string{"invalid status", "missing opening", "wrong opening", "invalid timestamp", "missing build"} {
		t.Run(scenario, func(t *testing.T) {
			f := newMaterializerFixture(t)
			log := testIssueLog("build/z", entity.IssueStatusOpen)
			opening := log.BuildID
			switch scenario {
			case "invalid status":
				log.Status = entity.IssueStatusUnknown
			case "missing opening":
				opening = ""
			case "wrong opening":
				opening = "build/a"
			case "invalid timestamp":
				log.TimestampMs = 0
			case "missing build":
				log.BuildID, opening = "unknown", "unknown"
			}
			require.Error(t, f.m.PersistLog(context.Background(), f.stores, log, opening))
			require.Empty(t, f.summary)
		})
	}
}

func TestMaterializerRejectsConflictingSummary(t *testing.T) {
	for _, scenario := range []string{"build", "opening", "timestamp", "identity", "invalid summary", "exhausted version"} {
		t.Run(scenario, func(t *testing.T) {
			f := newMaterializerFixture(t)
			log := testIssueLog("build/z", entity.IssueStatusOpen)
			require.NoError(t, f.m.PersistLog(context.Background(), f.stores, log, log.BuildID))
			switch scenario {
			case "build":
				f.summary.BuildID = "different"
			case "opening":
				f.summary.FirstSeenBuildID = "different"
			case "timestamp":
				f.summary.ChangedAtMs++
			case "identity":
				f.summary.TaskID = "different"
			case "invalid summary":
				f.summary.Status = entity.IssueStatusUnknown
			case "exhausted version":
				f.summary.Version = math.MaxInt32
				log = testIssueLog("build/m", entity.IssueStatusOpen)
			}
			current := f.summary
			require.Error(t, f.m.PersistLog(context.Background(), f.stores, log, log.BuildID))
			require.Equal(t, current, f.summary)
		})
	}
}

func TestMaterializerHonorsCanceledContext(t *testing.T) {
	f := newMaterializerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	log := testIssueLog("build/z", entity.IssueStatusOpen)
	require.ErrorIs(t, f.m.PersistLog(ctx, f.stores, log, log.BuildID), context.Canceled)
	require.Empty(t, f.logs)
	require.Empty(t, f.summary)
}
