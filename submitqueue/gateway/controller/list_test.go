// Copyright (c) 2025 Uber Technologies, Inc.
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
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	storage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
	gwstoragemock "github.com/uber/submitqueue/submitqueue/gateway/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type listTestFixture struct {
	controller   ListController
	receipts     *storagemock.MockRequestReceiptStore
	summaries    *storagemock.MockRequestSummaryStore
	queueConfigs *qcmock.MockStore
}

func newListTestFixture(t *testing.T) listTestFixture {
	ctrl := gomock.NewController(t)
	f := listTestFixture{
		receipts: storagemock.NewMockRequestReceiptStore(ctrl), summaries: storagemock.NewMockRequestSummaryStore(ctrl),
		queueConfigs: qcmock.NewMockStore(ctrl),
	}
	agg := gwstoragemock.NewMockStorage(ctrl)
	agg.EXPECT().GetRequestReceiptStore().Return(f.receipts).AnyTimes()
	agg.EXPECT().GetRequestSummaryStore().Return(f.summaries).AnyTimes()
	factory := gwstoragemock.NewMockFactory(ctrl)
	factory.EXPECT().For(storage.Config{QueueName: "q"}).Return(agg, nil).AnyTimes()
	f.controller = NewListController(zap.NewNop().Sugar(), tally.NoopScope, factory, f.queueConfigs)
	return f
}

func boundedListRequest(queue string, after, before int64) entity.ListRequest {
	return entity.ListRequest{
		Queue:               queue,
		ReceivedAtOrAfterMs: after, HasReceivedAtOrAfterMs: true,
		ReceivedBeforeMs: before, HasReceivedBeforeMs: true,
	}
}

func mustEncodeListPageToken(t *testing.T, token listPageToken) string {
	encoded, err := encodeListPageToken(token)
	require.NoError(t, err)
	return encoded
}

func TestList_ReceiptPages(t *testing.T) {
	receipts := []entity.RequestReceipt{
		{RequestID: "9", Queue: "q", ReceivedAtMs: 190},
		{RequestID: "10", Queue: "q", ReceivedAtMs: 190},
		{RequestID: "1", Queue: "q", ReceivedAtMs: 170},
	}
	tests := []struct {
		name     string
		receipts []entity.RequestReceipt
		wantIDs  []string
		wantNext bool
	}{
		{name: "empty", receipts: nil, wantIDs: []string{}},
		{name: "partial final page", receipts: receipts[:1], wantIDs: []string{"9"}},
		{name: "full final page", receipts: receipts[:2], wantIDs: []string{"9", "10"}},
		{name: "timestamp ties use string order", receipts: receipts, wantIDs: []string{"9", "10"}, wantNext: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
			f.receipts.EXPECT().List(gomock.Any(), basestorage.RequestReceiptRange{
				ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: 3,
			}).Return(tt.receipts, nil)
			want := make([]entity.RequestSummary, 0, len(tt.wantIDs))
			for _, receipt := range tt.receipts[:len(tt.wantIDs)] {
				summary := entity.RequestSummary{
					RequestID: receipt.RequestID, Queue: receipt.Queue, ReceivedAtMs: receipt.ReceivedAtMs,
					ChangeURIs: []string{"uri/" + receipt.RequestID}, Status: entity.RequestStatusError,
					LastError: "build failed", Metadata: map[string]string{"build": "url"}, Version: 5,
				}
				f.summaries.EXPECT().Get(gomock.Any(), receipt.RequestID).Return(summary, nil)
				want = append(want, summary)
			}
			req := boundedListRequest("q", 100, 200)
			req.PageSize = 2

			result, err := f.controller.List(context.Background(), req)

			require.NoError(t, err)
			assert.Equal(t, want, result.Requests)
			assert.Equal(t, int64(100), result.ReceivedAtOrAfterMs)
			assert.Equal(t, int64(200), result.ReceivedBeforeMs)
			ids := make([]string, 0, len(result.Requests))
			for _, summary := range result.Requests {
				ids = append(ids, summary.RequestID)
			}
			assert.Equal(t, tt.wantIDs, ids)
			if !tt.wantNext {
				assert.Empty(t, result.NextPageToken)
				return
			}
			token, err := decodeListPageToken(result.NextPageToken)
			require.NoError(t, err)
			assert.Equal(t, listPageToken{
				Version: listPageTokenVersion, Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200,
				LastReceivedAtMs: 190, LastRequestID: "10",
			}, token)
		})
	}
}

func TestList_ContinuationResolvesWindowFromToken(t *testing.T) {
	token := mustEncodeListPageToken(t, listPageToken{
		Version: listPageTokenVersion, Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, LastReceivedAtMs: 190, LastRequestID: "10",
	})
	legacyToken := base64.RawURLEncoding.EncodeToString([]byte(
		"queue=q&received_at_or_after_ms=100&received_before_ms=200&last_received_at_ms=190&last_request_id=10"))
	withBounds := boundedListRequest("q", 100, 200)
	withBounds.PageToken = token
	lowerOnly := entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 100, HasReceivedAtOrAfterMs: true, PageToken: token}
	tests := []struct {
		name    string
		request entity.ListRequest
	}{
		{name: "token only", request: entity.ListRequest{Queue: "q", PageToken: token}},
		{name: "matching bounds", request: withBounds},
		{name: "one matching bound", request: lowerOnly},
		{name: "unversioned token", request: entity.ListRequest{Queue: "q", PageToken: legacyToken}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
			f.receipts.EXPECT().List(gomock.Any(), basestorage.RequestReceiptRange{
				ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: 51,
				Before: basestorage.RequestReceiptCursor{ReceivedAtMs: 190, RequestID: "10"},
			}).Return([]entity.RequestReceipt{{Queue: "q", ReceivedAtMs: 170, RequestID: "1"}}, nil)
			summary := entity.RequestSummary{Queue: "q", ReceivedAtMs: 170, RequestID: "1", Status: entity.RequestStatusLanded}
			f.summaries.EXPECT().Get(gomock.Any(), "1").Return(summary, nil)

			result, err := f.controller.List(context.Background(), tt.request)

			require.NoError(t, err)
			assert.Equal(t, entity.ListResult{
				Requests: []entity.RequestSummary{summary}, ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200,
			}, result)
		})
	}
}

func TestList_Errors(t *testing.T) {
	validToken := func(edit func(*listPageToken)) string {
		token := listPageToken{
			Version: listPageTokenVersion, Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, LastReceivedAtMs: 150, LastRequestID: "1",
		}
		edit(&token)
		return mustEncodeListPageToken(t, token)
	}
	withToken := func(req entity.ListRequest, token string) entity.ListRequest {
		req.PageToken = token
		return req
	}
	goodToken := validToken(func(*listPageToken) {})
	tamperedToken := base64.RawURLEncoding.EncodeToString([]byte(`{"version":1,"queue":"q","received_at_or_after_ms":"x"}`))
	invalidFieldsToken := base64.RawURLEncoding.EncodeToString([]byte("queue=q&received_at_or_after_ms=100&received_before_ms=200&last_received_at_ms=150"))
	invalidNumberToken := base64.RawURLEncoding.EncodeToString([]byte("queue=q&received_at_or_after_ms=x&received_before_ms=200&last_received_at_ms=150&last_request_id=q%2F1"))
	backendErr := errors.New("store down")
	tests := []struct {
		name        string
		request     entity.ListRequest
		setup       func(listTestFixture)
		queueErr    error
		wantErr     error
		wantInvalid bool
		wantUnknown bool
	}{
		{name: "empty queue", request: boundedListRequest("", 1, 2), wantInvalid: true},
		{name: "unknown queue", request: boundedListRequest("missing", 1, 2), queueErr: queueconfig.ErrNotFound, wantUnknown: true, wantInvalid: true},
		{name: "queue config failure", request: boundedListRequest("q", 1, 2), queueErr: backendErr, wantErr: backendErr},
		{name: "first page without bounds", request: entity.ListRequest{Queue: "q"}, wantInvalid: true},
		{name: "first page without lower bound", request: entity.ListRequest{Queue: "q", ReceivedBeforeMs: 2, HasReceivedBeforeMs: true}, wantInvalid: true},
		{name: "first page without upper bound", request: entity.ListRequest{Queue: "q", ReceivedAtOrAfterMs: 1, HasReceivedAtOrAfterMs: true}, wantInvalid: true},
		{name: "invalid range", request: boundedListRequest("q", 2, 2), wantInvalid: true},
		{name: "negative page size", request: entity.ListRequest{Queue: "q", PageSize: -1, ReceivedAtOrAfterMs: 1, HasReceivedAtOrAfterMs: true, ReceivedBeforeMs: 2, HasReceivedBeforeMs: true}, wantInvalid: true},
		{name: "page size above maximum", request: entity.ListRequest{Queue: "q", PageSize: 201, ReceivedAtOrAfterMs: 1, HasReceivedAtOrAfterMs: true, ReceivedBeforeMs: 2, HasReceivedBeforeMs: true}, wantInvalid: true},
		{name: "malformed token", request: entity.ListRequest{Queue: "q", PageToken: "%%%"}, wantInvalid: true},
		{name: "tampered token", request: entity.ListRequest{Queue: "q", PageToken: tamperedToken}, wantInvalid: true},
		{name: "unknown token version", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.Version = 2 })}, wantInvalid: true},
		{name: "missing token version", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.Version = 0 })}, wantInvalid: true},
		{name: "invalid unversioned token number", request: entity.ListRequest{Queue: "q", PageToken: invalidNumberToken}, wantInvalid: true},
		{name: "invalid unversioned token fields", request: entity.ListRequest{Queue: "q", PageToken: invalidFieldsToken}, wantInvalid: true},
		{name: "zero cursor time", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.LastReceivedAtMs = 0 })}, wantInvalid: true},
		{name: "cursor before window", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.LastReceivedAtMs = 99 })}, wantInvalid: true},
		{name: "cursor at window end", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.LastReceivedAtMs = 200 })}, wantInvalid: true},
		{name: "token with empty window", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.ReceivedBeforeMs = 100 })}, wantInvalid: true},
		{name: "token for another queue", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.Queue = "other" })}, wantInvalid: true},
		{name: "mismatched lower bound", request: withToken(boundedListRequest("q", 101, 200), goodToken), wantInvalid: true},
		{name: "mismatched upper bound", request: withToken(entity.ListRequest{Queue: "q", ReceivedBeforeMs: 0, HasReceivedBeforeMs: true}, goodToken), wantInvalid: true},
		{name: "empty status", request: withStatuses(boundedListRequest("q", 1, 2), entity.RequestStatusUnknown), wantInvalid: true},
		{name: "too many statuses", request: withStatuses(boundedListRequest("q", 1, 2), slices.Repeat([]entity.RequestStatus{entity.RequestStatusLanded}, maxListStatuses+1)...), wantInvalid: true},
		{name: "token with non-canonical statuses", request: entity.ListRequest{Queue: "q", PageToken: validToken(func(tok *listPageToken) { tok.Statuses = []string{"landed", "error"} })}, wantInvalid: true},
		{name: "statuses differ from token", request: withStatuses(withToken(entity.ListRequest{Queue: "q"}, validToken(func(tok *listPageToken) { tok.Statuses = []string{"landed"} })), entity.RequestStatusError), wantInvalid: true},
		{name: "statuses added to unfiltered token", request: withStatuses(withToken(entity.ListRequest{Queue: "q"}, goodToken), entity.RequestStatusLanded), wantInvalid: true},
		{
			name: "receipt scan failure", request: boundedListRequest("q", 1, 2), wantErr: backendErr,
			setup: func(f listTestFixture) {
				f.receipts.EXPECT().List(gomock.Any(), basestorage.RequestReceiptRange{ReceivedAtOrAfterMs: 1, ReceivedBeforeMs: 2, Limit: 51}).Return(nil, backendErr)
			},
		},
		{
			name: "summary read failure", request: boundedListRequest("q", 1, 2), wantErr: backendErr,
			setup: func(f listTestFixture) {
				f.receipts.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestReceipt{{Queue: "q", ReceivedAtMs: 1, RequestID: "1"}}, nil)
				f.summaries.EXPECT().Get(gomock.Any(), "1").Return(entity.RequestSummary{}, backendErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			if tt.request.Queue != "" {
				f.queueConfigs.EXPECT().Get(gomock.Any(), tt.request.Queue).Return(entity.QueueConfig{}, tt.queueErr)
			}
			if tt.setup != nil {
				tt.setup(f)
			}
			result, err := f.controller.List(context.Background(), tt.request)
			require.Error(t, err)
			assert.Equal(t, entity.ListResult{}, result)
			assert.Equal(t, tt.wantInvalid, IsInvalidRequest(err))
			assert.Equal(t, tt.wantUnknown, IsUnrecognizedQueue(err))
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				assert.False(t, errs.IsUserError(err))
			}
		})
	}
}

func TestList_StorageResolutionFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	factory := gwstoragemock.NewMockFactory(ctrl)
	backendErr := errors.New("storage unavailable")
	factory.EXPECT().For(storage.Config{QueueName: "q"}).Return(nil, backendErr)
	queueConfigs := qcmock.NewMockStore(ctrl)
	queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
	c := NewListController(zap.NewNop().Sugar(), tally.NoopScope, factory, queueConfigs)

	result, err := c.List(context.Background(), boundedListRequest("q", 1, 2))

	assert.ErrorIs(t, err, backendErr)
	assert.Equal(t, entity.ListResult{}, result)
}

func withStatuses(req entity.ListRequest, statuses ...entity.RequestStatus) entity.ListRequest {
	req.Statuses = statuses
	return req
}

func listedReceipt(id string, receivedAtMs int64) entity.RequestReceipt {
	return entity.RequestReceipt{RequestID: id, Queue: "q", ReceivedAtMs: receivedAtMs}
}

func listedSummary(receipt entity.RequestReceipt, status entity.RequestStatus) entity.RequestSummary {
	return entity.RequestSummary{RequestID: receipt.RequestID, Queue: "q", ReceivedAtMs: receipt.ReceivedAtMs, Status: status}
}

func TestList_StatusFilter(t *testing.T) {
	landed, errored := entity.RequestStatusLanded, entity.RequestStatusError
	r := []entity.RequestReceipt{
		listedReceipt("6", 160), listedReceipt("5", 150), listedReceipt("4", 140),
		listedReceipt("3", 130), listedReceipt("2", 120), listedReceipt("1", 110),
	}
	cursor := func(receipt entity.RequestReceipt) basestorage.RequestReceiptCursor {
		return basestorage.RequestReceiptCursor{ReceivedAtMs: receipt.ReceivedAtMs, RequestID: receipt.RequestID}
	}
	window := func(limit int, before basestorage.RequestReceiptCursor) basestorage.RequestReceiptRange {
		return basestorage.RequestReceiptRange{ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: limit, Before: before}
	}
	type read struct {
		query    basestorage.RequestReceiptRange
		receipts []entity.RequestReceipt
	}
	tests := []struct {
		name     string
		statuses map[string]entity.RequestStatus
		reads    []read
		wantIDs  []string
		// wantLast is the receipt a continuation resumes after; empty means the range was exhausted.
		wantLast string
	}{
		{
			name:     "keeps reading past non-matching receipts until the page fills",
			statuses: map[string]entity.RequestStatus{"6": errored, "5": landed, "4": errored, "3": landed, "2": landed},
			reads: []read{
				{query: window(3, basestorage.RequestReceiptCursor{}), receipts: r[0:3]},
				{query: window(2, cursor(r[1])), receipts: r[2:4]},
				{query: window(2, cursor(r[2])), receipts: r[3:5]},
			},
			wantIDs: []string{"5", "3"}, wantLast: "3",
		},
		{
			name:     "returns a partial page without a token when the range is exhausted",
			statuses: map[string]entity.RequestStatus{"2": landed, "1": errored},
			reads:    []read{{query: window(3, basestorage.RequestReceiptCursor{}), receipts: r[4:6]}},
			wantIDs:  []string{"2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newListTestFixture(t)
			f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
			var calls []any
			for _, read := range tt.reads {
				calls = append(calls, f.receipts.EXPECT().List(gomock.Any(), read.query).Return(read.receipts, nil))
				for _, receipt := range read.receipts[:min(len(read.receipts), read.query.Limit-1)] {
					f.summaries.EXPECT().Get(gomock.Any(), receipt.RequestID).Return(listedSummary(receipt, tt.statuses[receipt.RequestID]), nil)
				}
			}
			gomock.InOrder(calls...)
			req := withStatuses(boundedListRequest("q", 100, 200), landed, landed)
			req.PageSize = 2

			result, err := f.controller.List(context.Background(), req)

			require.NoError(t, err)
			ids := make([]string, 0, len(result.Requests))
			for _, summary := range result.Requests {
				ids = append(ids, summary.RequestID)
			}
			assert.Equal(t, tt.wantIDs, ids)
			if tt.wantLast == "" {
				assert.Empty(t, result.NextPageToken)
				return
			}
			token, err := decodeListPageToken(result.NextPageToken)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLast, token.LastRequestID)
			assert.Equal(t, []string{"landed"}, token.Statuses)
		})
	}
}

func TestList_StatusFilterStopsAtReadBudget(t *testing.T) {
	f := newListTestFixture(t)
	f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
	next := int64(100 + maxFilteredListReceipts + 1)
	f.receipts.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, query basestorage.RequestReceiptRange) ([]entity.RequestReceipt, error) {
			batch := make([]entity.RequestReceipt, 0, query.Limit)
			for range query.Limit {
				batch = append(batch, listedReceipt(fmt.Sprint(next), next))
				next--
			}
			return batch, nil
		}).AnyTimes()
	f.summaries.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, id string) (entity.RequestSummary, error) {
			var receivedAtMs int64
			_, err := fmt.Sscan(id, &receivedAtMs)
			require.NoError(t, err)
			return listedSummary(listedReceipt(id, receivedAtMs), entity.RequestStatusError), nil
		}).Times(maxFilteredListReceipts)

	result, err := f.controller.List(context.Background(), withStatuses(boundedListRequest("q", 1, 10_000), entity.RequestStatusLanded))

	require.NoError(t, err)
	assert.Empty(t, result.Requests)
	require.NotEmpty(t, result.NextPageToken)
}

func TestList_ContinuationTakesTokenStatuses(t *testing.T) {
	f := newListTestFixture(t)
	f.queueConfigs.EXPECT().Get(gomock.Any(), "q").Return(entity.QueueConfig{}, nil)
	token := mustEncodeListPageToken(t, listPageToken{
		Version: listPageTokenVersion, Queue: "q", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200,
		LastReceivedAtMs: 150, LastRequestID: "5", Statuses: []string{"landed"},
	})
	receipts := []entity.RequestReceipt{listedReceipt("4", 140), listedReceipt("3", 130)}
	f.receipts.EXPECT().List(gomock.Any(), basestorage.RequestReceiptRange{
		ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200, Limit: 51,
		Before: basestorage.RequestReceiptCursor{ReceivedAtMs: 150, RequestID: "5"},
	}).Return(receipts, nil)
	f.summaries.EXPECT().Get(gomock.Any(), "4").Return(listedSummary(receipts[0], entity.RequestStatusError), nil)
	f.summaries.EXPECT().Get(gomock.Any(), "3").Return(listedSummary(receipts[1], entity.RequestStatusLanded), nil)

	result, err := f.controller.List(context.Background(), entity.ListRequest{Queue: "q", PageToken: token})

	require.NoError(t, err)
	require.Len(t, result.Requests, 1)
	assert.Equal(t, "3", result.Requests[0].RequestID)
	assert.Empty(t, result.NextPageToken)
}
