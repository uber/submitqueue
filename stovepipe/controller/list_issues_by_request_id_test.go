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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type requestIssuesFixture struct {
	controller ListIssuesByRequestIDController
	factory    *storagemock.MockFactory
	store      *storagemock.MockStorage
	logs       *storagemock.MockRequestLogStore
	summaries  *storagemock.MockRequestSummaryStore
	issues     *storagemock.MockBuildIssueStore
}

func newRequestIssuesFixture(t *testing.T) requestIssuesFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := requestIssuesFixture{factory: storagemock.NewMockFactory(ctrl), store: storagemock.NewMockStorage(ctrl), logs: storagemock.NewMockRequestLogStore(ctrl), summaries: storagemock.NewMockRequestSummaryStore(ctrl), issues: storagemock.NewMockBuildIssueStore(ctrl)}
	f.controller = NewListIssuesByRequestIDController(tally.NoopScope, f.factory, []string{"queue"})
	f.store.EXPECT().GetRequestLogStore().Return(f.logs).AnyTimes()
	f.store.EXPECT().GetRequestSummaryStore().Return(f.summaries).AnyTimes()
	f.store.EXPECT().GetBuildIssueStore().Return(f.issues).AnyTimes()
	return f
}

func (f requestIssuesFixture) expectHistory(logs []entity.RequestLog) {
	f.factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(f.store, nil)
	f.logs.EXPECT().List(gomock.Any(), "7").Return(logs, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "7").Return(entity.RequestSummary{RequestID: "7", Queue: "queue"}, nil)
}

func issueBuildLog(buildID string, event entity.RequestEvent) entity.RequestLog {
	return entity.RequestLog{Event: event, Metadata: map[string]string{requestlog.MetadataKeyBuildID: buildID}}
}

func requestIssuesToken(t *testing.T, cursor requestIssuesPageToken) string {
	t.Helper()
	contents, err := json.Marshal(cursor)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(contents)
}

func TestRequestIssuesAcrossBuildsAndContinuation(t *testing.T) {
	f := newRequestIssuesFixture(t)
	logs := []entity.RequestLog{
		issueBuildLog("build/b", entity.RequestEventBuildAnalysisAbandoned),
		issueBuildLog("build/a", entity.RequestEventBuildTriggered),
		issueBuildLog("build/b", entity.RequestEventBuildAnalysisCompleted),
		issueBuildLog("build/b", entity.RequestEventBuildAnalysisAbandoned),
		{State: entity.RequestStateFailed, RequestVersion: 3, Metadata: map[string]string{"build_id": "build/b"}},
	}
	req := entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageSize: 2}
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), "build/a", storage.BuildIssueRange{Limit: 3}).Return([]entity.BuildIssue{{TaskID: "task"}}, nil)
	f.issues.EXPECT().List(gomock.Any(), "build/b", storage.BuildIssueRange{Limit: 2}).Return([]entity.BuildIssue{{TaskID: "task", FixID: "repair/opaque"}, {TaskID: "task", UnitID: "unit"}}, nil)
	got, err := f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.RequestIssue{{BuildID: "build/a", TaskID: "task"}, {BuildID: "build/b", TaskID: "task", FixID: "repair/opaque"}}, got.Issues)
	require.Equal(t, []entity.IssueBuild{{BuildID: "build/a"}, {BuildID: "build/b", AnalysisStatus: entity.IssueAnalysisCompleted}}, got.Builds)
	require.Equal(t, "build/b", got.WinningBuildID)
	require.NotEmpty(t, got.NextPageToken)
	req.PageToken = got.NextPageToken
	req.PageSize = 1
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), "build/b", storage.BuildIssueRange{After: storage.BuildIssueCursor{TaskID: "task"}, Limit: 2}).Return([]entity.BuildIssue{{TaskID: "task", UnitID: "unit"}}, nil)
	got, err = f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.RequestIssue{{BuildID: "build/b", TaskID: "task", UnitID: "unit"}}, got.Issues)
	require.Empty(t, got.NextPageToken)
}

func TestRequestIssuesContinuationAtBuildBoundary(t *testing.T) {
	f := newRequestIssuesFixture(t)
	logs := []entity.RequestLog{issueBuildLog("a", entity.RequestEventBuildTriggered), issueBuildLog("b", entity.RequestEventBuildTriggered)}
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), "a", storage.BuildIssueRange{Limit: 2}).Return([]entity.BuildIssue{{TaskID: "task"}}, nil)
	req := entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageSize: 1}
	got, err := f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	req.PageToken = got.NextPageToken
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), "b", storage.BuildIssueRange{Limit: 2}).Return(nil, nil)
	got, err = f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, got.Issues)
	require.Empty(t, got.NextPageToken)
}

func TestRequestIssuesBoundsEmptyBuildTraversal(t *testing.T) {
	f := newRequestIssuesFixture(t)
	logs := make([]entity.RequestLog, maxIssueBuildsPerPage+1)
	for i := range logs {
		logs[i] = issueBuildLog(fmt.Sprintf("build/%03d", i), entity.RequestEventBuildTriggered)
	}
	f.expectHistory(logs)
	for _, log := range logs[:maxIssueBuildsPerPage] {
		f.issues.EXPECT().List(gomock.Any(), log.Metadata["build_id"], storage.BuildIssueRange{Limit: 51}).Return(nil, nil)
	}
	req := entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"}
	got, err := f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, got.Issues)
	require.NotEmpty(t, got.NextPageToken)
	req.PageToken = got.NextPageToken
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), "build/100", storage.BuildIssueRange{Limit: 51}).Return([]entity.BuildIssue{{TaskID: "task"}}, nil)
	got, err = f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, got.Issues, 1)
	require.Empty(t, got.NextPageToken)
}

func TestRequestIssuesRejectsInvalidInputBeforeStorage(t *testing.T) {
	valid := requestIssuesPageToken{Version: 1, Queue: "queue", RequestID: "7", BuildID: "build"}
	for _, change := range []func(*requestIssuesPageToken){
		func(c *requestIssuesPageToken) { c.Queue = "other" },
		func(c *requestIssuesPageToken) { c.RequestID = "8" },
		func(c *requestIssuesPageToken) { c.Version = 2 },
		func(c *requestIssuesPageToken) { c.BuildID = "" },
		func(c *requestIssuesPageToken) { c.BuildID = strings.Repeat("x", 1021) },
		func(c *requestIssuesPageToken) { c.UnitID = "unit" },
		func(c *requestIssuesPageToken) { c.TaskID = strings.Repeat("x", 256) },
		func(c *requestIssuesPageToken) { c.TaskID = "task"; c.UnitID = strings.Repeat("x", 1025) },
	} {
		f := newRequestIssuesFixture(t)
		cursor := valid
		change(&cursor)
		_, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageToken: requestIssuesToken(t, cursor)})
		require.True(t, IsInvalidRequest(err), "%v", err)
	}
	for _, req := range []entity.ListIssuesByRequestIDRequest{
		{Queue: "other", RequestID: "7"}, {Queue: "", RequestID: "7"}, {Queue: "queue", RequestID: "07"},
		{Queue: "queue", RequestID: "7", PageSize: -1}, {Queue: "queue", RequestID: "7", PageSize: 201},
		{Queue: "queue", RequestID: "7", PageToken: "%%%"}, {Queue: "queue", RequestID: "7", PageToken: "eA"},
	} {
		f := newRequestIssuesFixture(t)
		_, err := f.controller.ListIssuesByRequestID(context.Background(), req)
		require.True(t, IsInvalidRequest(err), "%v", err)
	}
}

func TestRequestIssuesDoesNotReadForeignBuildFromCursor(t *testing.T) {
	f := newRequestIssuesFixture(t)
	f.expectHistory([]entity.RequestLog{issueBuildLog("own", entity.RequestEventBuildTriggered)})
	_, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageToken: requestIssuesToken(t, requestIssuesPageToken{Version: 1, Queue: "queue", RequestID: "7", BuildID: "foreign"})})
	require.True(t, IsInvalidRequest(err))
}

func TestRequestIssuesNoBuildAndAbandonedAnalysis(t *testing.T) {
	for _, logs := range [][]entity.RequestLog{nil, {issueBuildLog("a", entity.RequestEventBuildAnalysisAbandoned)}} {
		f := newRequestIssuesFixture(t)
		f.expectHistory(logs)
		if len(logs) > 0 {
			f.issues.EXPECT().List(gomock.Any(), "a", storage.BuildIssueRange{Limit: 51}).Return(nil, nil)
		}
		got, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"})
		require.NoError(t, err)
		require.Empty(t, got.Issues)
		require.Empty(t, got.NextPageToken)
		if len(logs) > 0 {
			require.Equal(t, entity.IssueAnalysisAbandoned, got.Builds[0].AnalysisStatus)
		}
	}
}

func TestRequestIssuesStorageFailuresNeverReturnPartialPage(t *testing.T) {
	f := newRequestIssuesFixture(t)
	f.expectHistory([]entity.RequestLog{issueBuildLog("a", entity.RequestEventBuildTriggered), issueBuildLog("b", entity.RequestEventBuildTriggered)})
	f.issues.EXPECT().List(gomock.Any(), "a", storage.BuildIssueRange{Limit: 51}).Return([]entity.BuildIssue{{TaskID: "task"}}, nil)
	f.issues.EXPECT().List(gomock.Any(), "b", storage.BuildIssueRange{Limit: 50}).Return(nil, context.DeadlineExceeded)
	got, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, got)
}

func TestRequestIssuesHistoryReadErrors(t *testing.T) {
	for _, missingSummary := range []bool{false, true} {
		f := newRequestIssuesFixture(t)
		f.factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(f.store, nil)
		if missingSummary {
			f.logs.EXPECT().List(gomock.Any(), "7").Return([]entity.RequestLog{}, nil)
			f.summaries.EXPECT().Get(gomock.Any(), "7").Return(entity.RequestSummary{}, storage.ErrNotFound)
		} else {
			f.logs.EXPECT().List(gomock.Any(), "7").Return(nil, storage.ErrNotFound)
		}
		_, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"})
		require.Error(t, err)
		if missingSummary {
			require.True(t, errs.IsRetryable(err))
			require.False(t, IsRequestHistoryNotFound(err))
		} else {
			require.True(t, IsRequestHistoryNotFound(err))
		}
	}
}

func TestRequestIssuesOpaqueIdentifierLimits(t *testing.T) {
	f := newRequestIssuesFixture(t)
	buildID := strings.Repeat("😀", 255)
	taskID := strings.Repeat("t", 255)
	unitID := strings.Repeat("😀", 256)
	logs := []entity.RequestLog{issueBuildLog(buildID, entity.RequestEventBuildTriggered)}
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), buildID, storage.BuildIssueRange{Limit: 2}).Return([]entity.BuildIssue{{TaskID: taskID, UnitID: unitID}, {TaskID: "z"}}, nil)
	req := entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageSize: 1}
	got, err := f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, buildID, got.Issues[0].BuildID)
	req.PageToken = got.NextPageToken
	f.expectHistory(logs)
	f.issues.EXPECT().List(gomock.Any(), buildID, storage.BuildIssueRange{After: storage.BuildIssueCursor{TaskID: taskID, UnitID: unitID}, Limit: 2}).Return(nil, nil)
	_, err = f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
}
