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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestGetRequestHistoryByID(t *testing.T) {
	const (
		queue     = "monorepo/main"
		requestID = "42"
	)
	backendErr := errors.New("backend unavailable")
	ordered := []entity.RequestLog{
		{ID: "state/1", RequestID: requestID, TimestampMs: 10, State: entity.RequestStateAccepted},
		{ID: "state/2", RequestID: requestID, TimestampMs: 20, State: entity.RequestStateProcessing},
	}
	equalTimestamp := []entity.RequestLog{
		{ID: "event/a", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildTriggered},
		{ID: "event/b", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildFinished},
	}
	duplicate := entity.RequestLog{ID: "event/a", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildTriggered}

	tests := []struct {
		name         string
		req          entity.GetRequestHistoryByIDRequest
		logs         []entity.RequestLog
		factoryErr   error
		listErr      error
		wantLogs     []entity.RequestLog
		wantInvalid  bool
		wantNotFound bool
		wantUser     bool
		wantCause    error
		wantLog      bool
	}{
		{name: "ordered passthrough", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID}, logs: ordered, wantLogs: ordered, wantLog: true},
		{name: "equal timestamp order preserved", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID}, logs: equalTimestamp, wantLogs: equalTimestamp, wantLog: true},
		{name: "duplicates preserved", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID}, logs: []entity.RequestLog{duplicate, duplicate}, wantLogs: []entity.RequestLog{duplicate, duplicate}, wantLog: true},
		{name: "empty queue", req: entity.GetRequestHistoryByIDRequest{ID: requestID}, wantInvalid: true, wantUser: true},
		{name: "oversized queue", req: entity.GetRequestHistoryByIDRequest{Queue: strings.Repeat("q", maxHistoryIdentifierBytes+1), ID: requestID}, wantInvalid: true, wantUser: true},
		{name: "empty request ID", req: entity.GetRequestHistoryByIDRequest{Queue: queue}, wantInvalid: true, wantUser: true},
		{name: "composite request ID", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: "request/monorepo/main/42"}, wantInvalid: true, wantUser: true},
		{name: "oversized request ID", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: strings.Repeat("r", maxHistoryIdentifierBytes+1)}, wantInvalid: true, wantUser: true},
		{name: "storage factory failure", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID}, factoryErr: backendErr, wantCause: backendErr},
		{name: "history not found", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID}, listErr: fmt.Errorf("query: %w", storage.ErrNotFound), wantNotFound: true, wantUser: true},
		{name: "log store failure", req: entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID}, listErr: backendErr, wantCause: backendErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mockCtrl)
			stores := storagemock.NewMockStorage(mockCtrl)
			logStore := storagemock.NewMockRequestLogStore(mockCtrl)
			summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
			if !tt.wantInvalid {
				factory.EXPECT().For(storage.Config{QueueName: tt.req.Queue}).Return(stores, tt.factoryErr)
				if tt.factoryErr == nil {
					stores.EXPECT().GetRequestLogStore().Return(logStore)
					logStore.EXPECT().List(gomock.Any(), tt.req.ID).Return(tt.logs, tt.listErr)
					if tt.listErr == nil {
						stores.EXPECT().GetRequestSummaryStore().Return(summaryStore)
						summaryStore.EXPECT().Get(gomock.Any(), tt.req.ID).Return(entity.RequestSummary{RequestID: requestID, Queue: queue}, nil)
					}
				}
			}

			core, observed := observer.New(zap.DebugLevel)
			scope := tally.NewTestScope("test", nil)
			controller := NewRequestHistoryController(zap.New(core).Sugar(), scope, factory)
			ctx := metrics.WithContextTags(context.Background(), metrics.NewTag("queue", "context-queue"))

			got, err := controller.GetRequestHistoryByID(ctx, tt.req)

			assert.Equal(t, tt.wantLogs, got.Events)
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			assert.Equal(t, tt.wantNotFound, IsRequestHistoryNotFound(err))
			assert.Equal(t, tt.wantUser, errs.IsUserError(err))
			if tt.wantNotFound {
				var notFound *RequestHistoryByIDNotFoundError
				require.ErrorAs(t, err, &notFound)
				assert.Equal(t, requestID, notFound.RequestID)
			}
			if tt.wantCause != nil {
				assert.ErrorIs(t, err, tt.wantCause)
			}
			if tt.wantLogs != nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			entries := observed.FilterMessage("request history retrieved").All()
			if tt.wantLog {
				require.Len(t, entries, 1)
				assert.Equal(t, requestID, entries[0].ContextMap()["request_id"])
				assert.Equal(t, queue, entries[0].ContextMap()["queue"])
				assert.Equal(t, int64(len(tt.wantLogs)), entries[0].ContextMap()["event_count"])
			} else {
				assert.Empty(t, entries)
			}

			snapshot := scope.Snapshot()
			start, ok := snapshot.Counters()["test.request_history_controller.get_by_id.start+queue=context-queue"]
			require.True(t, ok)
			assert.EqualValues(t, 1, start.Value())
			assertOperationFinishIncludesContextTag(t, snapshot, "get_by_id", err == nil)
		})
	}
}

func TestGetRequestHistoryByURI(t *testing.T) {
	const (
		queue     = "monorepo/main"
		uri       = "git://example.com/repo.git/commit/deadbeef"
		requestID = "42"
	)
	backendErr := errors.New("backend unavailable")
	logs := []entity.RequestLog{
		{ID: "state/1", RequestID: requestID, TimestampMs: 10, State: entity.RequestStateAccepted},
		{ID: "event/a", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildTriggered},
		{ID: "event/a", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildTriggered},
	}
	summary := entity.RequestSummary{RequestID: requestID, Queue: queue, URI: uri}
	wantHistory := []entity.RequestHistory{{RequestID: requestID, Events: logs, RequestSummary: summary}}

	tests := []struct {
		name         string
		req          entity.GetRequestHistoryByURIRequest
		mappedID     string
		factoryErr   error
		mappingErr   error
		listErr      error
		want         []entity.RequestHistory
		wantInvalid  bool
		wantNotFound bool
		wantCause    error
		wantLog      bool
	}{
		{name: "singleton history preserves log order and duplicates", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappedID: requestID, want: wantHistory, wantLog: true},
		{name: "empty queue", req: entity.GetRequestHistoryByURIRequest{URI: uri}, wantInvalid: true},
		{name: "oversized queue", req: entity.GetRequestHistoryByURIRequest{Queue: strings.Repeat("q", maxHistoryIdentifierBytes+1), URI: uri}, wantInvalid: true},
		{name: "empty URI", req: entity.GetRequestHistoryByURIRequest{Queue: queue}, wantInvalid: true},
		{name: "oversized URI", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: strings.Repeat("u", maxHistoryIdentifierBytes+1)}, wantInvalid: true},
		{name: "storage factory failure", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, factoryErr: backendErr, wantCause: backendErr},
		{name: "URI mapping not found", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappingErr: fmt.Errorf("lookup: %w", storage.ErrNotFound), wantNotFound: true},
		{name: "URI store failure", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappingErr: backendErr, wantCause: backendErr},
		{name: "mapped history absence is internal", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappedID: requestID, listErr: fmt.Errorf("query: %w", storage.ErrNotFound), wantCause: storage.ErrNotFound},
		{name: "log store failure", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappedID: requestID, listErr: backendErr, wantCause: backendErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mockCtrl)
			stores := storagemock.NewMockStorage(mockCtrl)
			uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
			logStore := storagemock.NewMockRequestLogStore(mockCtrl)
			summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
			if !tt.wantInvalid {
				factory.EXPECT().For(storage.Config{QueueName: tt.req.Queue}).Return(stores, tt.factoryErr)
				if tt.factoryErr == nil {
					stores.EXPECT().GetRequestURIStore().Return(uriStore)
					uriStore.EXPECT().GetIDByURI(gomock.Any(), tt.req.URI).Return(tt.mappedID, tt.mappingErr)
					if tt.mappingErr == nil {
						stores.EXPECT().GetRequestLogStore().Return(logStore)
						logStore.EXPECT().List(gomock.Any(), tt.mappedID).Return(logs, tt.listErr)
						if tt.listErr == nil {
							stores.EXPECT().GetRequestSummaryStore().Return(summaryStore)
							summaryStore.EXPECT().Get(gomock.Any(), tt.mappedID).Return(summary, nil)
						}
					}
				}
			}

			core, observed := observer.New(zap.DebugLevel)
			scope := tally.NewTestScope("test", nil)
			controller := NewRequestHistoryController(zap.New(core).Sugar(), scope, factory)
			ctx := metrics.WithContextTags(context.Background(), metrics.NewTag("queue", "context-queue"))

			got, err := controller.GetRequestHistoryByURI(ctx, tt.req)

			assert.Equal(t, tt.want, got)
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			assert.Equal(t, tt.wantNotFound, IsRequestHistoryNotFound(err))
			assert.Equal(t, tt.wantInvalid || tt.wantNotFound, errs.IsUserError(err))
			if tt.wantCause != nil {
				assert.ErrorIs(t, err, tt.wantCause)
			}
			if tt.want != nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if tt.wantNotFound {
				var notFound *RequestHistoryByURINotFoundError
				require.ErrorAs(t, err, &notFound)
				assert.Equal(t, uri, notFound.URI)
			}

			entries := observed.FilterMessage("request history retrieved by URI").All()
			if tt.wantLog {
				require.Len(t, entries, 1)
				assert.Equal(t, uri, entries[0].ContextMap()["uri"])
				assert.Equal(t, requestID, entries[0].ContextMap()["request_id"])
				assert.Equal(t, queue, entries[0].ContextMap()["queue"])
				assert.Equal(t, int64(len(logs)), entries[0].ContextMap()["event_count"])
			} else {
				assert.Empty(t, entries)
			}

			snapshot := scope.Snapshot()
			start, ok := snapshot.Counters()["test.request_history_controller.get_by_uri.start+queue=context-queue"]
			require.True(t, ok)
			assert.EqualValues(t, 1, start.Value())
			assertOperationFinishIncludesContextTag(t, snapshot, "get_by_uri", err == nil)
		})
	}
}

func TestRequestHistoryNotFoundErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		assert func(*testing.T, error)
	}{
		{
			name: "request ID",
			err:  fmt.Errorf("lookup failed: %w", &RequestHistoryByIDNotFoundError{RequestID: "1"}),
			assert: func(t *testing.T, err error) {
				var notFound *RequestHistoryByIDNotFoundError
				require.ErrorAs(t, err, &notFound)
				assert.Equal(t, "1", notFound.RequestID)
			},
		},
		{
			name: "URI",
			err:  fmt.Errorf("lookup failed: %w", &RequestHistoryByURINotFoundError{URI: "git://repo/commit/1"}),
			assert: func(t *testing.T, err error) {
				var notFound *RequestHistoryByURINotFoundError
				require.ErrorAs(t, err, &notFound)
				assert.Equal(t, "git://repo/commit/1", notFound.URI)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsRequestHistoryNotFound(tt.err))
			tt.assert(t, tt.err)
		})
	}
	assert.False(t, IsRequestHistoryNotFound(errors.New("other")))
}

func TestRequestHistoryCurrentSummary(t *testing.T) {
	const queue, requestID, uri = "monorepo/main", "42", "git://monorepo/main/commit"
	summary := entity.RequestSummary{
		RequestID: requestID, Queue: queue, URI: uri, BaseURI: "git://monorepo/main/base", State: entity.RequestStateSucceeded,
		StateTimestampMs: 200, AcceptedAtMs: 100, OutcomeReason: entity.RequestOutcomeReasonBuildSucceeded,
	}
	logs := []entity.RequestLog{{ID: "accepted", RequestID: requestID, TimestampMs: 100, State: entity.RequestStateAccepted}}
	readErr := errors.New("summary read failed")
	tests := []struct {
		name        string
		modify      func(*entity.RequestSummary)
		readErr     error
		consistency bool
		retryable   bool
	}{
		{name: "current summary is independent of retained events"},
		{name: "missing summary is retryable infrastructure failure", readErr: fmt.Errorf("summary: %w", storage.ErrNotFound), retryable: true},
		{name: "summary read failure", readErr: readErr},
		{name: "wrong request ID", modify: func(s *entity.RequestSummary) { s.RequestID = "43" }, consistency: true},
		{name: "wrong queue", modify: func(s *entity.RequestSummary) { s.Queue = "other/main" }, consistency: true},
	}
	for _, selector := range []string{"ID", "URI"} {
		for _, tt := range tests {
			t.Run(selector+"/"+tt.name, func(t *testing.T) {
				mockCtrl := gomock.NewController(t)
				factory := storagemock.NewMockFactory(mockCtrl)
				stores := storagemock.NewMockStorage(mockCtrl)
				logStore := storagemock.NewMockRequestLogStore(mockCtrl)
				summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
				factory.EXPECT().For(storage.Config{QueueName: queue}).Return(stores, nil)
				if selector == "URI" {
					uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
					stores.EXPECT().GetRequestURIStore().Return(uriStore)
					uriStore.EXPECT().GetIDByURI(gomock.Any(), uri).Return(requestID, nil)
				}
				stores.EXPECT().GetRequestLogStore().Return(logStore)
				logStore.EXPECT().List(gomock.Any(), requestID).Return(logs, nil)
				stores.EXPECT().GetRequestSummaryStore().Return(summaryStore)
				storedSummary := summary
				if tt.modify != nil {
					tt.modify(&storedSummary)
				}
				summaryStore.EXPECT().Get(gomock.Any(), requestID).Return(storedSummary, tt.readErr)
				controller := NewRequestHistoryController(zap.NewNop().Sugar(), tally.NoopScope, factory)
				var history entity.RequestHistory
				var err error
				if selector == "ID" {
					history, err = controller.GetRequestHistoryByID(context.Background(), entity.GetRequestHistoryByIDRequest{Queue: queue, ID: requestID})
				} else {
					var histories []entity.RequestHistory
					histories, err = controller.GetRequestHistoryByURI(context.Background(), entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri})
					if err == nil {
						require.Len(t, histories, 1)
						history = histories[0]
					}
				}
				assert.False(t, errs.IsUserError(err))
				assert.False(t, IsRequestHistoryNotFound(err))
				assert.Equal(t, tt.retryable, errs.IsRetryable(err))
				if tt.retryable {
					require.Error(t, err)
					assert.Equal(t, entity.RequestHistory{}, history)
					return
				}
				if tt.consistency {
					var consistency *RequestHistoryConsistencyError
					require.ErrorAs(t, err, &consistency)
					return
				}
				if tt.readErr != nil {
					require.ErrorIs(t, err, tt.readErr)
					assert.False(t, IsRequestHistoryNotFound(err))
					return
				}
				require.NoError(t, err)
				assert.Equal(t, requestID, history.RequestID)
				assert.Equal(t, logs, history.Events)
				assert.Equal(t, summary, history.RequestSummary)
			})
		}
	}
}

func TestRequestHistoryRejectsMismatchedSummaryURI(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	factory := storagemock.NewMockFactory(mockCtrl)
	stores := storagemock.NewMockStorage(mockCtrl)
	uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
	logStore := storagemock.NewMockRequestLogStore(mockCtrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(mockCtrl)
	factory.EXPECT().For(storage.Config{QueueName: "queue"}).Return(stores, nil)
	stores.EXPECT().GetRequestURIStore().Return(uriStore)
	uriStore.EXPECT().GetIDByURI(gomock.Any(), "uri").Return("1", nil)
	stores.EXPECT().GetRequestLogStore().Return(logStore)
	logStore.EXPECT().List(gomock.Any(), "1").Return([]entity.RequestLog{{ID: "accepted"}}, nil)
	stores.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), "1").Return(entity.RequestSummary{RequestID: "1", Queue: "queue", URI: "other-uri"}, nil)
	_, err := NewRequestHistoryController(zap.NewNop().Sugar(), tally.NoopScope, factory).GetRequestHistoryByURI(context.Background(), entity.GetRequestHistoryByURIRequest{Queue: "queue", URI: "uri"})
	var consistency *RequestHistoryConsistencyError
	require.ErrorAs(t, err, &consistency)
}

func assertOperationFinishIncludesContextTag(t *testing.T, snapshot tally.Snapshot, operation string, success bool) {
	t.Helper()
	wantResult := "error"
	if success {
		wantResult = "success"
	}
	for _, histogram := range snapshot.Histograms() {
		if histogram.Name() == "test.request_history_controller."+operation+".finish" {
			assert.Equal(t, "context-queue", histogram.Tags()["queue"])
			assert.Equal(t, wantResult, histogram.Tags()["result"])
			return
		}
	}
	require.Fail(t, "operation finish histogram not found")
}
