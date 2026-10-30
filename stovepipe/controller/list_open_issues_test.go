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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const openIssueListQueue = "queue/main"

type openIssueListFixture struct {
	controller ListOpenIssuesController
	factory    *storagemock.MockFactory
	stores     *storagemock.MockStorage
	openIssues *storagemock.MockOpenIssueStore
	summaries  *storagemock.MockIssueSummaryStore
}

func newOpenIssueListFixture(t *testing.T) openIssueListFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := openIssueListFixture{factory: storagemock.NewMockFactory(ctrl), stores: storagemock.NewMockStorage(ctrl),
		openIssues: storagemock.NewMockOpenIssueStore(ctrl), summaries: storagemock.NewMockIssueSummaryStore(ctrl)}
	f.controller = NewListOpenIssuesController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, []string{openIssueListQueue})
	f.stores.EXPECT().GetOpenIssueStore().Return(f.openIssues).AnyTimes()
	f.stores.EXPECT().GetIssueSummaryStore().Return(f.summaries).AnyTimes()
	return f
}

func (f openIssueListFixture) expectOpenIssues(bounds storage.OpenIssueRange, openIssues []entity.OpenIssue) {
	f.factory.EXPECT().For(storage.Config{QueueName: openIssueListQueue}).Return(f.stores, nil)
	f.openIssues.EXPECT().List(gomock.Any(), bounds).Return(openIssues, nil)
}

func summaryForOpenIssue(openIssue entity.OpenIssue) entity.IssueSummary {
	return entity.IssueSummary{TaskID: openIssue.TaskID, UnitID: openIssue.UnitID, Status: entity.IssueStatusOpen,
		FirstSeenBuildID: openIssue.FirstSeenBuildID, RequestID: "100", BuildID: openIssue.FirstSeenBuildID, ChangedAtMs: 1000, Version: 1}
}

func TestListOpenIssuesReturnsBoundedPages(t *testing.T) {
	rows := []entity.OpenIssue{
		{TaskID: "a", FirstSeenBuildID: "build/1"},
		{TaskID: "a", UnitID: "unit", FirstSeenBuildID: "build/2"},
		{TaskID: "b", FirstSeenBuildID: "build/3"},
	}
	for _, count := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("%d open issues", count), func(t *testing.T) {
			f := newOpenIssueListFixture(t)
			f.expectOpenIssues(storage.OpenIssueRange{Limit: 3}, rows[:count])
			want := make([]entity.IssueSummary, 0)
			for _, row := range rows[:min(count, 2)] {
				summary := summaryForOpenIssue(row)
				want = append(want, summary)
				f.summaries.EXPECT().Get(gomock.Any(), row.TaskID, row.UnitID).Return(summary, nil)
			}
			got, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: 2})
			require.NoError(t, err)
			require.Equal(t, want, got.Issues)
			if count <= 2 {
				require.Empty(t, got.NextPageToken)
			} else {
				token, err := decodeOpenIssuesPageToken(got.NextPageToken)
				require.NoError(t, err)
				require.Equal(t, openIssuesPageToken{Version: 1, Queue: openIssueListQueue, LastTaskID: rows[1].TaskID, LastUnitID: rows[1].UnitID}, token)
			}
		})
	}
}

func TestListOpenIssuesFillsPagesPastStaleEntries(t *testing.T) {
	rows := []entity.OpenIssue{
		{TaskID: "a", FirstSeenBuildID: "build/1"},
		{TaskID: "a", UnitID: "unit", FirstSeenBuildID: "build/2"},
		{TaskID: "b", FirstSeenBuildID: "build/3"},
		{TaskID: "b", UnitID: "unit", FirstSeenBuildID: "build/4"},
		{TaskID: "c", FirstSeenBuildID: "build/5"},
		{TaskID: "d", FirstSeenBuildID: "build/6"},
	}
	cursor := func(row entity.OpenIssue) storage.OpenIssueCursor {
		return storage.OpenIssueCursor{TaskID: row.TaskID, UnitID: row.UnitID}
	}
	type read struct {
		bounds storage.OpenIssueRange
		rows   []entity.OpenIssue
	}
	for _, tt := range []struct {
		name     string
		pageSize int32
		reads    []read
		wantRows []entity.OpenIssue
		wantLast storage.OpenIssueCursor
	}{
		{
			name: "fills the page after closed and reopened references", pageSize: 3,
			reads: []read{
				{bounds: storage.OpenIssueRange{Limit: 4}, rows: rows[:4]},
				{bounds: storage.OpenIssueRange{After: cursor(rows[2]), Limit: 3}, rows: rows[3:]},
			},
			wantRows: []entity.OpenIssue{rows[0], rows[3], rows[4]}, wantLast: cursor(rows[4]),
		},
		{
			name: "continues across several batches without skipping lookahead", pageSize: 2,
			reads: []read{
				{bounds: storage.OpenIssueRange{Limit: 3}, rows: rows[:3]},
				{bounds: storage.OpenIssueRange{After: cursor(rows[1]), Limit: 2}, rows: rows[2:4]},
				{bounds: storage.OpenIssueRange{After: cursor(rows[2]), Limit: 2}, rows: rows[3:5]},
			},
			wantRows: []entity.OpenIssue{rows[0], rows[3]}, wantLast: cursor(rows[3]),
		},
		{
			name: "returns a partial final page without continuation", pageSize: 3,
			reads: []read{
				{bounds: storage.OpenIssueRange{Limit: 4}, rows: rows[:4]},
				{bounds: storage.OpenIssueRange{After: cursor(rows[2]), Limit: 3}, rows: rows[3:4]},
			},
			wantRows: []entity.OpenIssue{rows[0], rows[3]},
		},
		{
			name: "returns a full final page without continuation", pageSize: 3,
			reads: []read{
				{bounds: storage.OpenIssueRange{Limit: 4}, rows: rows[:4]},
				{bounds: storage.OpenIssueRange{After: cursor(rows[2]), Limit: 3}, rows: rows[3:5]},
			},
			wantRows: []entity.OpenIssue{rows[0], rows[3], rows[4]},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newOpenIssueListFixture(t)
			f.factory.EXPECT().For(storage.Config{QueueName: openIssueListQueue}).Return(f.stores, nil)
			var calls []any
			for _, read := range tt.reads {
				calls = append(calls, f.openIssues.EXPECT().List(gomock.Any(), read.bounds).Return(read.rows, nil))
				for _, row := range read.rows[:min(len(read.rows), read.bounds.Limit-1)] {
					summary := summaryForOpenIssue(row)
					switch row {
					case rows[1]:
						summary.Status = entity.IssueStatusClosed
					case rows[2]:
						summary.FirstSeenBuildID = "new-opening"
					}
					f.summaries.EXPECT().Get(gomock.Any(), row.TaskID, row.UnitID).Return(summary, nil)
				}
			}
			gomock.InOrder(calls...)

			got, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: tt.pageSize})
			require.NoError(t, err)
			want := make([]entity.IssueSummary, 0, len(tt.wantRows))
			for _, row := range tt.wantRows {
				want = append(want, summaryForOpenIssue(row))
			}
			require.Equal(t, want, got.Issues)
			if tt.wantLast.TaskID == "" {
				require.Empty(t, got.NextPageToken)
			} else {
				token, err := decodeOpenIssuesPageToken(got.NextPageToken)
				require.NoError(t, err)
				require.Equal(t, tt.wantLast, storage.OpenIssueCursor{TaskID: token.LastTaskID, UnitID: token.LastUnitID})
			}
		})
	}
}

func TestListOpenIssuesStopsAtInspectionBudget(t *testing.T) {
	for _, tt := range []struct {
		name      string
		total     int
		firstOpen bool
	}{
		{name: "empty page with continuation", total: 501},
		{name: "partial page with continuation", total: 501, firstOpen: true},
		{name: "empty exhausted range", total: 500},
		{name: "partial exhausted range", total: 500, firstOpen: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newOpenIssueListFixture(t)
			rows := make([]entity.OpenIssue, tt.total)
			for i := range rows {
				rows[i] = entity.OpenIssue{TaskID: fmt.Sprintf("task-%03d", i+1), FirstSeenBuildID: "build/1"}
			}
			calls := 1
			if tt.total > 500 {
				calls++
			}
			f.factory.EXPECT().For(storage.Config{QueueName: openIssueListQueue}).Return(f.stores, nil).Times(calls)
			inspected := 0
			f.openIssues.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, bounds storage.OpenIssueRange) ([]entity.OpenIssue, error) {
					after := storage.OpenIssueCursor{}
					if inspected > 0 {
						after.TaskID = rows[inspected-1].TaskID
					}
					require.Equal(t, after, bounds.After)
					require.Positive(t, bounds.Limit)
					require.LessOrEqual(t, bounds.Limit, 51)
					return rows[inspected:min(len(rows), inspected+bounds.Limit)], nil
				}).AnyTimes()
			f.summaries.EXPECT().Get(gomock.Any(), gomock.Any(), "").DoAndReturn(
				func(_ context.Context, taskID, _ string) (entity.IssueSummary, error) {
					row := rows[inspected]
					require.Equal(t, row.TaskID, taskID)
					summary := summaryForOpenIssue(row)
					if inspected != 500 && !(tt.firstOpen && inspected == 0) {
						summary.Status = entity.IssueStatusClosed
					}
					inspected++
					return summary, nil
				}).Times(tt.total)

			got, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: 50})
			require.NoError(t, err)
			require.Equal(t, 500, inspected)
			if tt.firstOpen {
				require.Equal(t, []entity.IssueSummary{summaryForOpenIssue(rows[0])}, got.Issues)
			} else {
				require.Empty(t, got.Issues)
			}
			if tt.total == 500 {
				require.Empty(t, got.NextPageToken)
				return
			}
			token, err := decodeOpenIssuesPageToken(got.NextPageToken)
			require.NoError(t, err)
			require.Equal(t, rows[499].TaskID, token.LastTaskID)
			require.Empty(t, token.LastUnitID)

			next, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{
				Queue: openIssueListQueue, PageSize: 50, PageToken: got.NextPageToken,
			})
			require.NoError(t, err)
			require.Equal(t, []entity.IssueSummary{summaryForOpenIssue(rows[500])}, next.Issues)
			require.Empty(t, next.NextPageToken)
		})
	}
}

func TestListOpenIssuesDiscardsPartialPageOnLaterReadFailure(t *testing.T) {
	for _, point := range []string{"lookup", "summary"} {
		t.Run(point, func(t *testing.T) {
			f := newOpenIssueListFixture(t)
			rows := []entity.OpenIssue{
				{TaskID: "a", FirstSeenBuildID: "build/1"},
				{TaskID: "b", FirstSeenBuildID: "build/1"},
				{TaskID: "c", FirstSeenBuildID: "build/1"},
			}
			f.expectOpenIssues(storage.OpenIssueRange{Limit: 3}, rows)
			f.summaries.EXPECT().Get(gomock.Any(), "a", "").Return(summaryForOpenIssue(rows[0]), nil)
			closed := summaryForOpenIssue(rows[1])
			closed.Status = entity.IssueStatusClosed
			f.summaries.EXPECT().Get(gomock.Any(), "b", "").Return(closed, nil)
			read := f.openIssues.EXPECT().List(gomock.Any(), storage.OpenIssueRange{After: storage.OpenIssueCursor{TaskID: "b"}, Limit: 2})
			if point == "lookup" {
				read.Return(nil, assert.AnError)
			} else {
				read.Return(rows[2:], nil)
				f.summaries.EXPECT().Get(gomock.Any(), "c", "").Return(entity.IssueSummary{}, assert.AnError)
			}
			got, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: 2})
			require.ErrorIs(t, err, assert.AnError)
			require.Empty(t, got)
		})
	}
}

func TestListOpenIssuesUsesCurrentSummaryOnContinuation(t *testing.T) {
	f := newOpenIssueListFixture(t)
	token, err := encodeOpenIssuesPageToken(openIssuesPageToken{Version: 1, Queue: openIssueListQueue, LastTaskID: "a"})
	require.NoError(t, err)
	row := entity.OpenIssue{TaskID: "b", FirstSeenBuildID: "build/1"}
	f.expectOpenIssues(storage.OpenIssueRange{After: storage.OpenIssueCursor{TaskID: "a"}, Limit: 2}, []entity.OpenIssue{row})
	nowClosed := summaryForOpenIssue(row)
	nowClosed.Status = entity.IssueStatusClosed
	f.summaries.EXPECT().Get(gomock.Any(), row.TaskID, row.UnitID).Return(nowClosed, nil)
	got, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: 1, PageToken: token})
	require.NoError(t, err)
	require.Empty(t, got.Issues)
	require.Empty(t, got.NextPageToken)
}

func TestListOpenIssuesReturnsStorageErrors(t *testing.T) {
	for _, point := range []string{"factory", "open issues", "summary", "missing summary", "cancellation", "wrong summary identity"} {
		t.Run(point, func(t *testing.T) {
			f := newOpenIssueListFixture(t)
			dependencyErr := assert.AnError
			if point == "factory" {
				f.factory.EXPECT().For(storage.Config{QueueName: openIssueListQueue}).Return(nil, dependencyErr)
			} else {
				f.factory.EXPECT().For(storage.Config{QueueName: openIssueListQueue}).Return(f.stores, nil)
				if point == "open issues" {
					f.openIssues.EXPECT().List(gomock.Any(), storage.OpenIssueRange{Limit: 2}).Return(nil, dependencyErr)
				} else {
					row := entity.OpenIssue{TaskID: "task", FirstSeenBuildID: "build/1"}
					f.openIssues.EXPECT().List(gomock.Any(), storage.OpenIssueRange{Limit: 2}).Return([]entity.OpenIssue{row}, nil)
					switch point {
					case "missing summary":
						dependencyErr = storage.ErrNotFound
					case "cancellation":
						dependencyErr = context.Canceled
					}
					if point == "wrong summary identity" {
						wrong := summaryForOpenIssue(row)
						wrong.UnitID = "other"
						f.summaries.EXPECT().Get(gomock.Any(), row.TaskID, row.UnitID).Return(wrong, nil)
					} else {
						f.summaries.EXPECT().Get(gomock.Any(), row.TaskID, row.UnitID).Return(entity.IssueSummary{}, dependencyErr)
					}
				}
			}
			got, err := f.controller.ListOpenIssues(context.Background(), entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: 1})
			require.Error(t, err)
			if point != "wrong summary identity" {
				require.ErrorIs(t, err, dependencyErr)
			}
			require.False(t, IsInvalidRequest(err))
			require.Empty(t, got)
		})
	}
}

func TestOpenIssuesTokenRoundTrip(t *testing.T) {
	for _, unit := range []string{"", "unit ", "a\x00b", "😀"} {
		want := openIssuesPageToken{Version: 1, Queue: openIssueListQueue, LastTaskID: "task", LastUnitID: unit}
		encoded, err := encodeOpenIssuesPageToken(want)
		require.NoError(t, err)
		got, err := decodeOpenIssuesPageToken(encoded)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestListOpenIssuesRejectsInvalidInputBeforeStorage(t *testing.T) {
	valid := openIssuesPageToken{Version: 1, Queue: openIssueListQueue, LastTaskID: "task"}
	for _, tt := range []struct {
		name        string
		req         entity.ListOpenIssuesRequest
		tokenChange func(*openIssuesPageToken)
	}{
		{name: "missing queue"},
		{name: "unconfigured queue", req: entity.ListOpenIssuesRequest{Queue: "other"}},
		{name: "negative size", req: entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: -1}},
		{name: "oversized page", req: entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: 201}},
		{name: "bad encoding", req: entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageToken: "%"}},
		{name: "bad JSON", req: entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageToken: base64.RawURLEncoding.EncodeToString([]byte("not json"))}},
		{name: "wrong version", tokenChange: func(p *openIssuesPageToken) { p.Version++ }},
		{name: "empty token queue", tokenChange: func(p *openIssuesPageToken) { p.Queue = "" }},
		{name: "different queue", tokenChange: func(p *openIssuesPageToken) { p.Queue = "other" }},
		{name: "missing task", tokenChange: func(p *openIssuesPageToken) { p.LastTaskID = "" }},
		{name: "oversized task", tokenChange: func(p *openIssuesPageToken) { p.LastTaskID = strings.Repeat("t", 256) }},
		{name: "oversized unit", tokenChange: func(p *openIssuesPageToken) { p.LastUnitID = strings.Repeat("u", 1025) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.req
			if tt.tokenChange != nil {
				token := valid
				tt.tokenChange(&token)
				var err error
				req.Queue = openIssueListQueue
				req.PageToken, err = encodeOpenIssuesPageToken(token)
				require.NoError(t, err)
			}
			f := newOpenIssueListFixture(t)
			_, err := f.controller.ListOpenIssues(context.Background(), req)
			require.True(t, IsInvalidRequest(err))
		})
	}
}

func TestOpenIssuesRangeUsesBoundedDefaults(t *testing.T) {
	for _, tt := range []struct {
		size  int32
		limit int
	}{{0, 51}, {1, 2}, {200, 201}} {
		bounds, err := resolveOpenIssueRange(entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageSize: tt.size})
		require.NoError(t, err)
		require.Equal(t, tt.limit, bounds.Limit)
	}
}

func TestOpenIssuesRejectsRequestListTokens(t *testing.T) {
	token, err := encodeListPageToken(listPageToken{Version: listPageTokenVersion, Queue: openIssueListQueue,
		AcceptedBeforeMs: 1000, LastAcceptedAtMs: 500, LastRequestID: "100"})
	require.NoError(t, err)
	_, err = resolveOpenIssueRange(entity.ListOpenIssuesRequest{Queue: openIssueListQueue, PageToken: token})
	require.True(t, IsInvalidRequest(err))
}
