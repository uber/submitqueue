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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type issueHistoryFixture struct {
	c         *listIssueHistoryController
	requests  *storagemock.MockRequestAcceptanceStore
	summaries *storagemock.MockRequestIssueSummaryStore
	logs      *storagemock.MockIssueLogStore
}

func newIssueHistoryFixture(t *testing.T) issueHistoryFixture {
	t.Helper()

	mocks := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mocks)
	store := storagemock.NewMockStorage(mocks)
	f := issueHistoryFixture{
		c:         NewListIssueHistoryController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"}).(*listIssueHistoryController),
		requests:  storagemock.NewMockRequestAcceptanceStore(mocks),
		summaries: storagemock.NewMockRequestIssueSummaryStore(mocks),
		logs:      storagemock.NewMockIssueLogStore(mocks),
	}
	f.c.now = func() time.Time { return time.UnixMilli(1000) }
	factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(store, nil).AnyTimes()
	store.EXPECT().GetRequestAcceptanceStore().Return(f.requests).AnyTimes()
	store.EXPECT().GetRequestIssueSummaryStore().Return(f.summaries).AnyTimes()
	store.EXPECT().GetIssueLogStore().Return(f.logs).AnyTimes()

	return f
}

func TestIssueHistoryPagination(t *testing.T) {
	f := newIssueHistoryFixture(t)
	requests := []entity.RequestAcceptance{
		{Queue: "queue", RequestID: "3", AcceptedAtMs: 300},
		{Queue: "queue", RequestID: "2", AcceptedAtMs: 200},
		{Queue: "queue", RequestID: "1", AcceptedAtMs: 100},
	}
	rows := []entity.IssueLog{
		{TaskID: "task", UnitID: "unit", BuildID: "a", Event: entity.IssueEventClosed, TimestampMs: 900, FirstSeenBuildID: "c"},
		{TaskID: "task", UnitID: "unit", BuildID: "b", Event: entity.IssueEventReported, TimestampMs: 100, FirstSeenBuildID: "c"},
		{TaskID: "task", UnitID: "unit", BuildID: "c", Event: entity.IssueEventOpened, TimestampMs: 5000, FirstSeenBuildID: "c"},
	}
	f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: 3}).Return(requests, nil)
	for i, row := range rows {
		f.summaries.EXPECT().Get(gomock.Any(), requests[i].RequestID, "task", "unit").Return(entity.RequestIssueSummary{BuildID: row.BuildID, Kind: entity.RequestIssueKind(row.Event)}, nil)
		f.logs.EXPECT().Get(gomock.Any(), "task", "unit", row.BuildID, row.Event).Return(row, nil)
	}

	req := entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", UnitID: "unit", PageSize: 2}

	first, err := f.c.ListIssueHistory(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{{Log: rows[0], RequestID: "3"}, {Log: rows[1], RequestID: "2"}}, first.Events)
	require.NotEmpty(t, first.NextPageToken)
	require.EqualValues(t, 0, first.AcceptedAtOrAfterMs)
	require.EqualValues(t, 1000, first.AcceptedBeforeMs)
	f.c.now = func() time.Time { return time.UnixMilli(2000) }
	f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{
		AcceptedBeforeMs: 1000, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 200, RequestID: "2"}, Limit: 3,
	}).Return(requests[2:], nil)
	req.PageToken = first.NextPageToken

	last, err := f.c.ListIssueHistory(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{{Log: rows[2], RequestID: "1"}}, last.Events)
	require.Empty(t, last.NextPageToken)
	require.EqualValues(t, 1000, last.AcceptedBeforeMs)
}

func TestIssueHistoryBoundsSkippedRequestWork(t *testing.T) {
	for _, count := range []int{0, defaultListPageSize, defaultListPageSize + 1, maxIssueHistoryRequests, maxIssueHistoryRequests + 1} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			f := newIssueHistoryFixture(t)
			requests := make([]entity.RequestAcceptance, count)
			for i := range requests {
				requests[i] = entity.RequestAcceptance{Queue: "queue", RequestID: strconv.Itoa(900 - i), AcceptedAtMs: int64(900 - i)}
			}
			f.requests.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, bounds storage.RequestAcceptanceRange) ([]entity.RequestAcceptance, error) {
				start := 0
				if bounds.Before.AcceptedAtMs > 0 {
					start = 901 - int(bounds.Before.AcceptedAtMs)
				}
				return requests[start:min(start+bounds.Limit, len(requests))], nil
			}).AnyTimes()
			f.summaries.EXPECT().Get(gomock.Any(), gomock.Any(), "task", "").Return(entity.RequestIssueSummary{}, storage.ErrNotFound).Times(count)

			req := entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task"}

			first, err := f.c.ListIssueHistory(context.Background(), req)
			require.NoError(t, err)
			require.Empty(t, first.Events)
			if count <= maxIssueHistoryRequests {
				require.Empty(t, first.NextPageToken)
				return
			}
			require.NotEmpty(t, first.NextPageToken)

			data, err := base64.RawURLEncoding.DecodeString(first.NextPageToken)
			require.NoError(t, err)
			var token issueHistoryPageToken
			require.NoError(t, json.Unmarshal(data, &token))
			require.Equal(t, requests[maxIssueHistoryRequests-1].RequestID, token.Before.RequestID)
			req.PageToken = first.NextPageToken

			last, err := f.c.ListIssueHistory(context.Background(), req)
			require.NoError(t, err)
			require.Empty(t, last.Events)
			require.Empty(t, last.NextPageToken)
		})
	}
}

func TestIssueHistoryFillsPageAcrossSkippedRequests(t *testing.T) {
	f := newIssueHistoryFixture(t)
	requests := []entity.RequestAcceptance{
		{Queue: "queue", RequestID: "5", AcceptedAtMs: 500},
		{Queue: "queue", RequestID: "4", AcceptedAtMs: 400},
		{Queue: "queue", RequestID: "3", AcceptedAtMs: 300},
		{Queue: "queue", RequestID: "2", AcceptedAtMs: 200},
		{Queue: "queue", RequestID: "1", AcceptedAtMs: 100},
	}
	gomock.InOrder(
		f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: 3}).Return(requests[:3], nil),
		f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 400, RequestID: "4"}, Limit: 2}).Return(requests[2:4], nil),
		f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 300, RequestID: "3"}, Limit: 2}).Return(requests[3:], nil),
		f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 200, RequestID: "2"}, Limit: 3}).Return(requests[4:], nil),
	)
	f.summaries.EXPECT().Get(gomock.Any(), "5", "task", "").Return(entity.RequestIssueSummary{}, storage.ErrNotFound)
	for _, requestID := range []string{"4", "3", "2", "1"} {
		f.summaries.EXPECT().Get(gomock.Any(), requestID, "task", "").Return(entity.RequestIssueSummary{BuildID: requestID, Kind: entity.RequestIssueKindReported}, nil)
		if requestID == "3" {
			f.logs.EXPECT().Get(gomock.Any(), "task", "", requestID, entity.IssueEventReported).Return(entity.IssueLog{}, storage.ErrNotFound)
		} else {
			f.logs.EXPECT().Get(gomock.Any(), "task", "", requestID, entity.IssueEventReported).Return(entity.IssueLog{BuildID: requestID, Event: entity.IssueEventReported}, nil)
		}
	}

	req := entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", PageSize: 2}

	first, err := f.c.ListIssueHistory(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{
		{RequestID: "4", Log: entity.IssueLog{BuildID: "4", Event: entity.IssueEventReported}},
		{RequestID: "2", Log: entity.IssueLog{BuildID: "2", Event: entity.IssueEventReported}},
	}, first.Events)
	require.NotEmpty(t, first.NextPageToken)
	req.PageToken = first.NextPageToken

	last, err := f.c.ListIssueHistory(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{{RequestID: "1", Log: entity.IssueLog{BuildID: "1", Event: entity.IssueEventReported}}}, last.Events)
	require.Empty(t, last.NextPageToken)
}

func TestIssueHistoryExhaustsFullEventPage(t *testing.T) {
	f := newIssueHistoryFixture(t)
	requests := []entity.RequestAcceptance{{Queue: "queue", RequestID: "2", AcceptedAtMs: 200}, {Queue: "queue", RequestID: "1", AcceptedAtMs: 100}}
	f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: 3}).Return(requests, nil)
	for _, request := range requests {
		f.summaries.EXPECT().Get(gomock.Any(), request.RequestID, "task", "").Return(entity.RequestIssueSummary{Kind: entity.RequestIssueKindReported, BuildID: request.RequestID}, nil)
		f.logs.EXPECT().Get(gomock.Any(), "task", "", request.RequestID, entity.IssueEventReported).Return(entity.IssueLog{BuildID: request.RequestID, Event: entity.IssueEventReported}, nil)
	}

	result, err := f.c.ListIssueHistory(context.Background(), entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", PageSize: 2})
	require.NoError(t, err)
	require.Len(t, result.Events, 2)
	require.Empty(t, result.NextPageToken)
}

func TestIssueHistoryRequestWindowAndMissingProjections(t *testing.T) {
	f := newIssueHistoryFixture(t)
	f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedAtOrAfterMs: 100, AcceptedBeforeMs: 400, Limit: defaultListPageSize + 1}).Return([]entity.RequestAcceptance{
		{Queue: "queue", RequestID: "4", AcceptedAtMs: 350},
		{Queue: "queue", RequestID: "3", AcceptedAtMs: 300},
		{Queue: "queue", RequestID: "2", AcceptedAtMs: 200},
		{Queue: "queue", RequestID: "1", AcceptedAtMs: 100},
	}, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "4", "task", "").Return(entity.RequestIssueSummary{Kind: "unknown", BuildID: "unsupported"}, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "3", "task", "").Return(entity.RequestIssueSummary{}, storage.ErrNotFound)
	f.summaries.EXPECT().Get(gomock.Any(), "2", "task", "").Return(entity.RequestIssueSummary{Kind: entity.RequestIssueKindOpened, BuildID: "missing"}, nil)
	f.logs.EXPECT().Get(gomock.Any(), "task", "", "missing", entity.IssueEventOpened).Return(entity.IssueLog{}, storage.ErrNotFound)
	f.summaries.EXPECT().Get(gomock.Any(), "1", "task", "").Return(entity.RequestIssueSummary{Kind: entity.RequestIssueKindOpened, BuildID: "build"}, nil)
	log := entity.IssueLog{TaskID: "task", BuildID: "build", Event: entity.IssueEventOpened, TimestampMs: 900}
	f.logs.EXPECT().Get(gomock.Any(), "task", "", "build", entity.IssueEventOpened).Return(log, nil)

	result, err := f.c.ListIssueHistory(context.Background(), entity.ListIssueHistoryRequest{
		Queue: "queue", TaskID: "task", HasAcceptedAtOrAfterMs: true, AcceptedAtOrAfterMs: 100, HasAcceptedBeforeMs: true, AcceptedBeforeMs: 400,
	})
	require.NoError(t, err)
	require.Equal(t, []entity.IssueHistoryEntry{{Log: log, RequestID: "1"}}, result.Events)
	require.EqualValues(t, 100, result.AcceptedAtOrAfterMs)
	require.EqualValues(t, 400, result.AcceptedBeforeMs)
	require.Empty(t, result.NextPageToken)
}

func TestIssueHistoryErrorsDoNotReturnPartialPages(t *testing.T) {
	for _, stage := range []string{"requests", "summaries", "logs", "canceled"} {
		t.Run(stage, func(t *testing.T) {
			f := newIssueHistoryFixture(t)
			var requestErr error
			if stage == "requests" {
				requestErr = assert.AnError
			}
			f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: defaultListPageSize + 1}).Return([]entity.RequestAcceptance{
				{Queue: "queue", RequestID: "2", AcceptedAtMs: 200}, {Queue: "queue", RequestID: "1", AcceptedAtMs: 100},
			}, requestErr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage != "requests" {
				f.summaries.EXPECT().Get(gomock.Any(), "2", "task", "").Return(entity.RequestIssueSummary{Kind: entity.RequestIssueKindOpened, BuildID: "first"}, nil)
				f.logs.EXPECT().Get(gomock.Any(), "task", "", "first", entity.IssueEventOpened).DoAndReturn(func(context.Context, string, string, string, entity.IssueEvent) (entity.IssueLog, error) {
					if stage == "canceled" {
						cancel()
					}
					return entity.IssueLog{BuildID: "first", Event: entity.IssueEventOpened}, nil
				})
				if stage != "canceled" {
					var summaryErr error
					if stage == "summaries" {
						summaryErr = assert.AnError
					}
					f.summaries.EXPECT().Get(gomock.Any(), "1", "task", "").Return(entity.RequestIssueSummary{Kind: entity.RequestIssueKindOpened, BuildID: "second"}, summaryErr)
					if stage == "logs" {
						f.logs.EXPECT().Get(gomock.Any(), "task", "", "second", entity.IssueEventOpened).Return(entity.IssueLog{}, assert.AnError)
					}
				}
			}
			got, err := f.c.ListIssueHistory(ctx, entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task"})
			want := assert.AnError
			if stage == "canceled" {
				want = context.Canceled
			}
			require.ErrorIs(t, err, want)
			require.Empty(t, got.Events)
			require.Empty(t, got.NextPageToken)
		})
	}
}

func TestIssueHistoryStorageSelectionFailure(t *testing.T) {
	factory := storagemock.NewMockFactory(gomock.NewController(t))
	factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(nil, assert.AnError)
	c := NewListIssueHistoryController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"})
	_, err := c.ListIssueHistory(context.Background(), entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task"})
	require.ErrorIs(t, err, assert.AnError)
}

func TestIssueHistoryInvalidRequests(t *testing.T) {
	valid := issueHistoryPageToken{Version: issueHistoryPageTokenVersion, Queue: "queue", TaskID: "task", UnitID: "unit", AcceptedBeforeMs: 1000,
		Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 100, RequestID: "1"}}
	for _, test := range []struct {
		name   string
		change func(*issueHistoryPageToken)
	}{
		{"queue", func(t *issueHistoryPageToken) { t.Queue = "other" }},
		{"task", func(t *issueHistoryPageToken) { t.TaskID = "other" }},
		{"unit", func(t *issueHistoryPageToken) { t.UnitID = "" }},
		{"version", func(t *issueHistoryPageToken) { t.Version = issueHistoryPageTokenVersion + 1 }},
		{"issue position with exact filter", func(t *issueHistoryPageToken) { t.AfterIssue.TaskID = "task" }},
		{"unit-only position with exact filter", func(t *issueHistoryPageToken) { t.AfterIssue.UnitID = "unit" }},
		{"negative window", func(t *issueHistoryPageToken) { t.AcceptedAtOrAfterMs = -1 }},
		{"empty window", func(t *issueHistoryPageToken) { t.AcceptedBeforeMs = 0 }},
		{"missing position", func(t *issueHistoryPageToken) { t.Before.AcceptedAtMs = 0 }},
		{"outside window", func(t *issueHistoryPageToken) { t.Before.AcceptedAtMs = 1000 }},
		{"before window", func(t *issueHistoryPageToken) { t.AcceptedAtOrAfterMs = 200 }},
		{"request id", func(t *issueHistoryPageToken) { t.Before.RequestID = "01" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			token := valid
			test.change(&token)

			data, err := json.Marshal(token)
			require.NoError(t, err)
			_, err = resolveIssueHistoryPageToken(entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task", UnitID: "unit", PageToken: base64.RawURLEncoding.EncodeToString(data)}, 1000)
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}

	for _, position := range []storage.RequestIssueSummaryCursor{
		{UnitID: "unit"},
		{TaskID: strings.Repeat("t", maxHistoryIdentifierBytes+1)},
		{TaskID: "task", UnitID: strings.Repeat("u", 1025)},
	} {
		token := valid
		token.TaskID, token.UnitID = "", ""
		token.AfterIssue = position

		data, err := json.Marshal(token)
		require.NoError(t, err)
		_, err = resolveIssueHistoryPageToken(entity.ListIssueHistoryRequest{Queue: "queue", PageToken: base64.RawURLEncoding.EncodeToString(data)}, 1000)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}

	data, err := json.Marshal(valid)
	require.NoError(t, err)
	for _, req := range []entity.ListIssueHistoryRequest{
		{Queue: "queue", TaskID: "task", UnitID: "unit", PageToken: base64.RawURLEncoding.EncodeToString(data), HasAcceptedBeforeMs: true, AcceptedBeforeMs: 999},
		{Queue: "queue", TaskID: "task", UnitID: "unit", PageToken: base64.RawURLEncoding.EncodeToString(data), HasAcceptedAtOrAfterMs: true, AcceptedAtOrAfterMs: 1},
		{}, {Queue: "other", TaskID: "task"}, {Queue: "queue", UnitID: "unit"},
		{Queue: "queue", TaskID: strings.Repeat("t", 256)}, {Queue: "queue", TaskID: "task", UnitID: strings.Repeat("u", 1025)},
		{Queue: "queue", TaskID: "task", PageSize: -1}, {Queue: "queue", TaskID: "task", PageSize: 201},
		{Queue: "queue", TaskID: "task", PageToken: "bad!"},
		{Queue: "queue", TaskID: "task", PageToken: base64.RawURLEncoding.EncodeToString([]byte(`{"before":{"AcceptedAtMs":100,"RequestID":"1"}}`))},
		{Queue: "queue", TaskID: "task", PageToken: base64.RawURLEncoding.EncodeToString([]byte("not json"))},
		{Queue: "queue", TaskID: "task", HasAcceptedAtOrAfterMs: true, AcceptedAtOrAfterMs: -1},
		{Queue: "queue", TaskID: "task", HasAcceptedBeforeMs: true},
	} {
		factory := storagemock.NewMockFactory(gomock.NewController(t))
		c := NewListIssueHistoryController(zap.NewNop().Sugar(), tally.NoopScope, factory, []string{"queue"})
		_, err := c.ListIssueHistory(context.Background(), req)
		require.ErrorIs(t, err, ErrInvalidRequest)
	}
}

func TestIssueHistoryAcrossQueue(t *testing.T) {
	for _, pageSize := range []int32{1, 2, 3, 4, 5} {
		t.Run(strconv.Itoa(int(pageSize)), func(t *testing.T) {
			f := newIssueHistoryFixture(t)
			requests := []entity.RequestAcceptance{
				{RequestID: "3", AcceptedAtMs: 300},
				{RequestID: "2", AcceptedAtMs: 200},
				{RequestID: "1", AcceptedAtMs: 100},
			}
			summaries := map[string][]entity.RequestIssueSummary{
				"3": {
					{TaskID: "build", Kind: entity.RequestIssueKindOpened, BuildID: "newest"},
					{TaskID: "test", UnitID: "a", Kind: entity.RequestIssueKindReported, BuildID: "newest"},
					{TaskID: "test", UnitID: "b", Kind: entity.RequestIssueKindClosed, BuildID: "newest"},
				},
				"1": {{TaskID: "test", UnitID: "a", Kind: entity.RequestIssueKindOpened, BuildID: "oldest"}},
			}
			f.requests.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, bounds storage.RequestAcceptanceRange) ([]entity.RequestAcceptance, error) {
				var rows []entity.RequestAcceptance
				for _, request := range requests {
					if bounds.Before.RequestID == "" || request.AcceptedAtMs < bounds.Before.AcceptedAtMs {
						rows = append(rows, request)
					}
				}
				return rows[:min(len(rows), bounds.Limit)], nil
			}).AnyTimes()
			f.summaries.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, requestID string, bounds storage.RequestIssueSummaryRange) ([]entity.RequestIssueSummary, error) {
				var rows []entity.RequestIssueSummary
				for _, summary := range summaries[requestID] {
					if summary.TaskID > bounds.After.TaskID || (summary.TaskID == bounds.After.TaskID && summary.UnitID > bounds.After.UnitID) {
						rows = append(rows, summary)
					}
				}
				return rows[:min(len(rows), bounds.Limit)], nil
			}).AnyTimes()
			f.logs.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, taskID, unitID, buildID string, event entity.IssueEvent) (entity.IssueLog, error) {
				return entity.IssueLog{TaskID: taskID, UnitID: unitID, BuildID: buildID, Event: event}, nil
			}).AnyTimes()

			var want, got []entity.IssueHistoryEntry
			for _, request := range requests {
				for _, summary := range summaries[request.RequestID] {
					want = append(want, entity.IssueHistoryEntry{
						RequestID: request.RequestID,
						Log:       entity.IssueLog{TaskID: summary.TaskID, UnitID: summary.UnitID, BuildID: summary.BuildID, Event: entity.IssueEvent(summary.Kind)},
					})
				}
			}

			req := entity.ListIssueHistoryRequest{Queue: "queue", PageSize: pageSize}
			for calls := 0; ; calls++ {
				require.Less(t, calls, len(want)+1)
				result, err := f.c.ListIssueHistory(context.Background(), req)
				require.NoError(t, err)
				require.LessOrEqual(t, len(result.Events), int(pageSize))
				got = append(got, result.Events...)
				if result.NextPageToken == "" {
					break
				}
				req.PageToken = result.NextPageToken
			}
			require.Equal(t, want, got)
		})
	}
}

func TestIssueHistoryRefillsWithinRequest(t *testing.T) {
	for _, remaining := range []int{0, 1, 2} {
		t.Run(strconv.Itoa(remaining), func(t *testing.T) {
			f := newIssueHistoryFixture(t)
			rows := []entity.RequestIssueSummary{
				{TaskID: "a", Kind: entity.RequestIssueKindReported, BuildID: "build"},
				{TaskID: "b", Kind: "unknown", BuildID: "build"},
				{TaskID: "c", Kind: entity.RequestIssueKindReported, BuildID: "build"},
				{TaskID: "d", Kind: entity.RequestIssueKindReported, BuildID: "build"},
				{TaskID: "e", Kind: entity.RequestIssueKindReported, BuildID: "build"},
			}
			for _, task := range []string{"f", "g"}[:remaining] {
				rows = append(rows, entity.RequestIssueSummary{TaskID: task, Kind: entity.RequestIssueKindReported, BuildID: "build"})
			}
			f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: 3}).
				Return([]entity.RequestAcceptance{{RequestID: "1", AcceptedAtMs: 100}}, nil)
			gomock.InOrder(
				f.summaries.EXPECT().List(gomock.Any(), "1", storage.RequestIssueSummaryRange{Limit: 3}).Return(rows[:3], nil),
				f.summaries.EXPECT().List(gomock.Any(), "1", storage.RequestIssueSummaryRange{After: storage.RequestIssueSummaryCursor{TaskID: "b"}, Limit: 3}).Return(rows[2:5], nil),
				f.summaries.EXPECT().List(gomock.Any(), "1", storage.RequestIssueSummaryRange{After: storage.RequestIssueSummaryCursor{TaskID: "d"}, Limit: 2}).Return(rows[4:min(len(rows), 6)], nil),
			)
			for _, task := range []string{"a", "d"} {
				f.logs.EXPECT().Get(gomock.Any(), task, "", "build", entity.IssueEventReported).Return(entity.IssueLog{}, storage.ErrNotFound)
			}
			var want []entity.IssueHistoryEntry
			for _, task := range []string{"c", "e"} {
				log := entity.IssueLog{TaskID: task, BuildID: "build", Event: entity.IssueEventReported}
				f.logs.EXPECT().Get(gomock.Any(), task, "", "build", entity.IssueEventReported).Return(log, nil)
				want = append(want, entity.IssueHistoryEntry{Log: log, RequestID: "1"})
			}

			req := entity.ListIssueHistoryRequest{Queue: "queue", PageSize: 2}
			first, err := f.c.ListIssueHistory(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, want, first.Events)
			if remaining == 0 {
				require.Empty(t, first.NextPageToken)
				return
			}
			require.NotEmpty(t, first.NextPageToken)

			req.PageToken = first.NextPageToken
			f.requests.EXPECT().List(gomock.Any(), storage.RequestAcceptanceRange{
				AcceptedBeforeMs: 1000, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 100, RequestID: "1"}, Limit: 3,
			}).Return(nil, nil)
			f.summaries.EXPECT().List(gomock.Any(), "1", storage.RequestIssueSummaryRange{After: storage.RequestIssueSummaryCursor{TaskID: "e"}, Limit: 3}).Return(rows[5:], nil)
			var wantLast []entity.IssueHistoryEntry
			for _, row := range rows[5:] {
				log := entity.IssueLog{TaskID: row.TaskID, BuildID: "build", Event: entity.IssueEventReported}
				f.logs.EXPECT().Get(gomock.Any(), row.TaskID, "", "build", entity.IssueEventReported).Return(log, nil)
				wantLast = append(wantLast, entity.IssueHistoryEntry{Log: log, RequestID: "1"})
			}

			last, err := f.c.ListIssueHistory(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, wantLast, last.Events)
			require.Empty(t, last.NextPageToken)
		})
	}
}

func TestIssueHistoryBoundsQueueSummaryWork(t *testing.T) {
	for _, pageSize := range []int32{1, maxListPageSize} {
		t.Run(strconv.Itoa(int(pageSize)), func(t *testing.T) {
			f := newIssueHistoryFixture(t)
			f.requests.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestAcceptance{{RequestID: "1", AcceptedAtMs: 100}}, nil)
			rows := make([]entity.RequestIssueSummary, maxIssueHistorySummaries+1)
			for i := range rows {
				rows[i] = entity.RequestIssueSummary{TaskID: fmt.Sprintf("task-%03d", i), Kind: entity.RequestIssueKindReported, BuildID: "build"}
			}
			f.summaries.EXPECT().List(gomock.Any(), "1", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, bounds storage.RequestIssueSummaryRange) ([]entity.RequestIssueSummary, error) {
				batch := rows
				for len(batch) > 0 && batch[0].TaskID <= bounds.After.TaskID {
					batch = batch[1:]
				}
				return batch[:min(len(batch), bounds.Limit)], nil
			}).AnyTimes()
			f.logs.EXPECT().Get(gomock.Any(), gomock.Any(), "", "build", entity.IssueEventReported).Return(entity.IssueLog{}, storage.ErrNotFound).Times(maxIssueHistorySummaries)

			req := entity.ListIssueHistoryRequest{Queue: "queue", PageSize: pageSize}
			first, err := f.c.ListIssueHistory(context.Background(), req)
			require.NoError(t, err)
			require.Empty(t, first.Events)
			require.NotEmpty(t, first.NextPageToken)

			req.PageToken = first.NextPageToken
			f.requests.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, nil)
			log := entity.IssueLog{TaskID: rows[maxIssueHistorySummaries].TaskID, BuildID: "build", Event: entity.IssueEventReported}
			f.logs.EXPECT().Get(gomock.Any(), log.TaskID, "", "build", entity.IssueEventReported).Return(log, nil)

			last, err := f.c.ListIssueHistory(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, []entity.IssueHistoryEntry{{Log: log, RequestID: "1"}}, last.Events)
			require.Empty(t, last.NextPageToken)
		})
	}
}
