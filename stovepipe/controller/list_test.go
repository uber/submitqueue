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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type listTestFixture struct {
	controller  *listController
	factory     *storagemock.MockFactory
	stores      *storagemock.MockStorage
	acceptances *storagemock.MockRequestAcceptanceStore
	summaries   *storagemock.MockRequestSummaryStore
}

func newListTestFixture(t *testing.T) listTestFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := listTestFixture{
		factory:     storagemock.NewMockFactory(ctrl),
		stores:      storagemock.NewMockStorage(ctrl),
		acceptances: storagemock.NewMockRequestAcceptanceStore(ctrl),
		summaries:   storagemock.NewMockRequestSummaryStore(ctrl),
	}
	f.controller = NewListController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, []string{"monorepo/main"}).(*listController)
	f.controller.now = func() time.Time { return time.UnixMilli(1000) }
	f.stores.EXPECT().GetRequestAcceptanceStore().Return(f.acceptances).AnyTimes()
	f.stores.EXPECT().GetRequestSummaryStore().Return(f.summaries).AnyTimes()
	return f
}

func listTestSummary(timestamp int64, suffix string) entity.RequestSummary {
	return entity.RequestSummary{
		Queue: "monorepo/main", RequestID: "request/monorepo/main/" + suffix,
		URI: "git://repo/" + suffix, BaseURI: "git://repo/base", AcceptedAtMs: timestamp,
		State: entity.RequestStateProcessing, RequestVersion: 2, StateTimestampMs: 2000, Version: 2,
	}
}

func (f listTestFixture) expectPage(query storage.RequestAcceptanceRange, summaries []entity.RequestSummary) {
	mappings := make([]entity.RequestAcceptance, 0, len(summaries))
	for _, summary := range summaries {
		mappings = append(mappings, entity.RequestAcceptance{Queue: summary.Queue, AcceptedAtMs: summary.AcceptedAtMs, RequestID: summary.RequestID})
	}
	f.factory.EXPECT().For(storage.Config{QueueName: "monorepo/main"}).Return(f.stores, nil)
	f.acceptances.EXPECT().List(gomock.Any(), query).Return(mappings, nil)
	for _, summary := range summaries[:min(len(summaries), query.Limit-1)] {
		f.summaries.EXPECT().Get(gomock.Any(), summary.RequestID).Return(summary, nil)
	}
}

func TestListReadsOnePage(t *testing.T) {
	first, second, lookahead := listTestSummary(900, "9"), listTestSummary(900, "10"), listTestSummary(800, "1")
	for _, tt := range []struct {
		name string
		rows []entity.RequestSummary
		want []entity.RequestSummary
		more bool
	}{
		{"no matches", nil, []entity.RequestSummary{}, false},
		{"short page", []entity.RequestSummary{first}, []entity.RequestSummary{first}, false},
		{"exact page", []entity.RequestSummary{first, second}, []entity.RequestSummary{first, second}, false},
		{"lookahead", []entity.RequestSummary{first, second, lookahead}, []entity.RequestSummary{first, second}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			f.expectPage(storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: 3}, tt.rows)
			got, err := f.controller.List(context.Background(), entity.ListRequest{Queue: "monorepo/main", PageSize: 2})
			require.NoError(t, err)
			require.Equal(t, tt.want, got.Requests)
			if !tt.more {
				require.Empty(t, got.NextPageToken)
				return
			}
			token, err := decodeListPageToken(got.NextPageToken)
			require.NoError(t, err)
			require.Equal(t, listPageToken{
				Version: listPageTokenVersion, Queue: "monorepo/main", AcceptedBeforeMs: 1000,
				LastAcceptedAtMs: second.AcceptedAtMs, LastRequestID: second.RequestID,
			}, token)
		})
	}
}

func TestListContinuationPreservesWindowAndReadsCurrentState(t *testing.T) {
	f := newListTestFixture(t)
	ctx := context.Background()
	first, second, third := listTestSummary(900, "9"), listTestSummary(900, "10"), listTestSummary(800, "1")
	f.expectPage(storage.RequestAcceptanceRange{AcceptedBeforeMs: 1000, Limit: 2}, []entity.RequestSummary{first, second})
	page, err := f.controller.List(ctx, entity.ListRequest{Queue: "monorepo/main", PageSize: 1})
	require.NoError(t, err)
	require.Equal(t, []entity.RequestSummary{first}, page.Requests)
	require.NotEmpty(t, page.NextPageToken)

	second.State = entity.RequestStateFailed
	second.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
	second.StateTimestampMs = 5000
	second.RequestVersion = 3
	second.Version = 3
	f.controller.now = func() time.Time { return time.UnixMilli(5000) }
	f.expectPage(storage.RequestAcceptanceRange{
		AcceptedBeforeMs: 1000, Limit: 3,
		Before: storage.RequestAcceptanceCursor{AcceptedAtMs: first.AcceptedAtMs, RequestID: first.RequestID},
	}, []entity.RequestSummary{second, third})
	page, err = f.controller.List(ctx, entity.ListRequest{Queue: "monorepo/main", PageToken: page.NextPageToken, PageSize: 2})
	require.NoError(t, err)
	require.Equal(t, []entity.RequestSummary{second, third}, page.Requests)
	require.Empty(t, page.NextPageToken)
}

func TestListRejectsInvalidInputBeforeStorageAccess(t *testing.T) {
	for _, tt := range []struct {
		name    string
		request entity.ListRequest
	}{
		{"empty queue", entity.ListRequest{}},
		{"unconfigured queue", entity.ListRequest{Queue: "other/main"}},
		{"negative bounds", explicitListWindow(-1, 1000)},
		{"invalid page size", entity.ListRequest{Queue: "monorepo/main", PageSize: 201}},
		{"malformed token", entity.ListRequest{Queue: "monorepo/main", PageToken: "%%%"}},
		{"mismatched bounds", entity.ListRequest{Queue: "monorepo/main", PageToken: mustEncodeListToken(t, validListToken()), HasAcceptedBeforeMs: true, AcceptedBeforeMs: 1001}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			result, err := f.controller.List(context.Background(), tt.request)
			require.ErrorIs(t, err, ErrInvalidRequest)
			require.True(t, IsInvalidRequest(err))
			require.True(t, errs.IsUserError(err))
			require.Equal(t, entity.ListResult{}, result)
		})
	}
}

func TestListRechecksConfiguredQueueOnContinuation(t *testing.T) {
	f := newListTestFixture(t)
	controller := NewListController(zap.NewNop().Sugar(), tally.NoopScope, f.factory, []string{"other/main"})
	_, err := controller.List(context.Background(), entity.ListRequest{
		Queue: "monorepo/main", PageToken: mustEncodeListToken(t, validListToken()),
	})
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestListPreservesStorageFailures(t *testing.T) {
	failure := errs.NewRetryableError(errors.New("unavailable"))
	for _, tt := range []struct {
		name                                     string
		resolveErr, mappingErr, summaryErr, want error
	}{
		{name: "resolve", resolveErr: failure, want: failure},
		{name: "mapping", mappingErr: failure, want: failure},
		{name: "summary", summaryErr: failure, want: failure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			f.factory.EXPECT().For(storage.Config{QueueName: "monorepo/main"}).Return(f.stores, tt.resolveErr)
			if tt.resolveErr == nil {
				f.acceptances.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestAcceptance{
					{Queue: "monorepo/main", AcceptedAtMs: 900, RequestID: "request/monorepo/main/1"},
				}, tt.mappingErr)
				if tt.mappingErr == nil {
					f.summaries.EXPECT().Get(gomock.Any(), "request/monorepo/main/1").Return(entity.RequestSummary{}, tt.summaryErr)
				}
			}
			result, err := f.controller.List(context.Background(), entity.ListRequest{Queue: "monorepo/main"})
			require.Equal(t, entity.ListResult{}, result)
			require.ErrorIs(t, err, tt.want)
			require.False(t, IsListConsistency(err))
			require.True(t, errs.IsRetryable(err))
			require.False(t, errs.IsUserError(err))
		})
	}
}
