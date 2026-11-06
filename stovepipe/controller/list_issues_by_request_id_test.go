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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type requestIssuesFixture struct {
	controller ListIssuesByRequestIDController
	factory    *storagemock.MockFactory
	store      *storagemock.MockStorage
	summaries  *storagemock.MockRequestSummaryStore
	issues     *storagemock.MockRequestIssueSummaryStore
}

func newRequestIssuesFixture(t *testing.T) requestIssuesFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := requestIssuesFixture{factory: storagemock.NewMockFactory(ctrl), store: storagemock.NewMockStorage(ctrl), summaries: storagemock.NewMockRequestSummaryStore(ctrl), issues: storagemock.NewMockRequestIssueSummaryStore(ctrl)}
	f.controller = NewListIssuesByRequestIDController(tally.NoopScope, f.factory, []string{"queue"})
	f.store.EXPECT().GetRequestSummaryStore().Return(f.summaries).AnyTimes()
	f.store.EXPECT().GetRequestIssueSummaryStore().Return(f.issues).AnyTimes()
	return f
}

func (f requestIssuesFixture) expectRequest() {
	f.factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(f.store, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "7").Return(entity.RequestSummary{Queue: "queue", RequestID: "7"}, nil)
}

func requestIssuesToken(t *testing.T, cursor requestIssuesPageToken) string {
	t.Helper()
	contents, err := json.Marshal(cursor)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(contents)
}

func TestRequestIssuesListsOnlyReadModelsAndPaginatesAcrossBuilds(t *testing.T) {
	f := newRequestIssuesFixture(t)
	first := entity.RequestIssueSummary{RequestID: "7", BuildID: "a", TaskID: "task"}
	second := entity.RequestIssueSummary{RequestID: "7", BuildID: "b", TaskID: "task", FixID: "repair/opaque", Novelty: entity.IssueNoveltyExisting}
	third := entity.RequestIssueSummary{RequestID: "7", BuildID: "b", TaskID: "task", UnitID: "unit"}
	f.expectRequest()
	f.issues.EXPECT().List(gomock.Any(), "7", storage.RequestIssueRange{Limit: 3}).Return([]entity.RequestIssueSummary{first, second, third}, nil)
	req := entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7", PageSize: 2}
	got, err := f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.RequestIssue{{BuildID: "a", TaskID: "task"}, {BuildID: "b", TaskID: "task", FixID: "repair/opaque", Novelty: entity.IssueNoveltyExisting}}, got.Issues)
	require.NotEmpty(t, got.NextPageToken)
	req.PageSize, req.PageToken = 1, got.NextPageToken
	f.expectRequest()
	f.issues.EXPECT().List(gomock.Any(), "7", storage.RequestIssueRange{After: storage.RequestIssueCursor{BuildID: "b", TaskID: "task"}, Limit: 2}).Return([]entity.RequestIssueSummary{third}, nil)
	got, err = f.controller.ListIssuesByRequestID(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.RequestIssue{{BuildID: "b", TaskID: "task", UnitID: "unit"}}, got.Issues)
	require.Empty(t, got.NextPageToken)
}

func TestRequestIssuesEmptyPageAndRequestScopedCursor(t *testing.T) {
	for _, buildID := range []string{"", "other-build"} {
		t.Run(buildID, func(t *testing.T) {
			f := newRequestIssuesFixture(t)
			f.expectRequest()
			req := entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"}
			if buildID != "" {
				req.PageToken = requestIssuesToken(t, requestIssuesPageToken{Version: 1, Queue: "queue", RequestID: "7", BuildID: buildID})
			}
			f.issues.EXPECT().List(gomock.Any(), "7", storage.RequestIssueRange{After: storage.RequestIssueCursor{BuildID: buildID}, Limit: 51}).Return(nil, nil)
			got, err := f.controller.ListIssuesByRequestID(context.Background(), req)
			require.NoError(t, err)
			require.Empty(t, got.Issues)
			require.Empty(t, got.NextPageToken)
		})
	}
}

func TestRequestIssuesSummaryFailures(t *testing.T) {
	for _, kind := range []string{"missing", "storage", "wrong queue", "wrong request"} {
		t.Run(kind, func(t *testing.T) {
			f := newRequestIssuesFixture(t)
			f.factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(f.store, nil)
			summary := entity.RequestSummary{Queue: "queue", RequestID: "7"}
			var sourceErr error
			switch kind {
			case "missing":
				sourceErr = storage.ErrNotFound
			case "storage":
				sourceErr = context.DeadlineExceeded
			case "wrong queue":
				summary.Queue = "other"
			case "wrong request":
				summary.RequestID = "8"
			}
			f.summaries.EXPECT().Get(gomock.Any(), "7").Return(summary, sourceErr)
			got, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"})
			require.Error(t, err)
			require.Empty(t, got)
			if kind == "missing" {
				require.True(t, IsRequestHistoryNotFound(err))
			}
			if kind == "storage" {
				require.ErrorIs(t, err, sourceErr)
			}
		})
	}
}

func TestRequestIssuesProjectionReadFailsWithoutPartialResult(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		f := newRequestIssuesFixture(t)
		f.expectRequest()
		rows := []entity.RequestIssueSummary{{RequestID: "8", BuildID: "foreign", TaskID: "task"}}
		var sourceErr error
		if !foreign {
			sourceErr = context.DeadlineExceeded
		}
		f.issues.EXPECT().List(gomock.Any(), "7", storage.RequestIssueRange{Limit: 51}).Return(rows, sourceErr)
		got, err := f.controller.ListIssuesByRequestID(context.Background(), entity.ListIssuesByRequestIDRequest{Queue: "queue", RequestID: "7"})
		require.Error(t, err)
		require.Empty(t, got)
	}
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
