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
		requestID = "request/monorepo/main/42"
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
			if !tt.wantInvalid {
				factory.EXPECT().For(storage.Config{QueueName: tt.req.Queue}).Return(stores, tt.factoryErr)
				if tt.factoryErr == nil {
					stores.EXPECT().GetRequestLogStore().Return(logStore)
					logStore.EXPECT().List(gomock.Any(), tt.req.ID).Return(tt.logs, tt.listErr)
				}
			}

			core, observed := observer.New(zap.DebugLevel)
			scope := tally.NewTestScope("test", nil)
			controller := NewRequestHistoryController(zap.New(core).Sugar(), scope, factory)
			ctx := metrics.WithContextTags(context.Background(), metrics.NewTag("queue", "context-queue"))

			got, err := controller.GetRequestHistoryByID(ctx, tt.req)

			assert.Equal(t, tt.wantLogs, got)
			if tt.wantInvalid {
				assert.True(t, IsInvalidRequest(err))
			}
			assert.Equal(t, tt.wantNotFound, IsRequestHistoryNotFound(err))
			assert.Equal(t, tt.wantUser, errs.IsUserError(err))
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
		requestID = "request/monorepo/main/42"
	)
	backendErr := errors.New("backend unavailable")
	logs := []entity.RequestLog{
		{ID: "state/1", RequestID: requestID, TimestampMs: 10, State: entity.RequestStateAccepted},
		{ID: "event/a", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildTriggered},
		{ID: "event/a", RequestID: requestID, TimestampMs: 20, Event: entity.RequestEventBuildTriggered},
	}
	wantHistory := []entity.RequestHistory{{RequestID: requestID, Events: logs}}

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
		{name: "mapped history not found", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappedID: requestID, listErr: fmt.Errorf("query: %w", storage.ErrNotFound), wantNotFound: true},
		{name: "log store failure", req: entity.GetRequestHistoryByURIRequest{Queue: queue, URI: uri}, mappedID: requestID, listErr: backendErr, wantCause: backendErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			factory := storagemock.NewMockFactory(mockCtrl)
			stores := storagemock.NewMockStorage(mockCtrl)
			uriStore := storagemock.NewMockRequestURIStore(mockCtrl)
			logStore := storagemock.NewMockRequestLogStore(mockCtrl)
			if !tt.wantInvalid {
				factory.EXPECT().For(storage.Config{QueueName: tt.req.Queue}).Return(stores, tt.factoryErr)
				if tt.factoryErr == nil {
					stores.EXPECT().GetRequestURIStore().Return(uriStore)
					uriStore.EXPECT().GetIDByURI(gomock.Any(), tt.req.URI).Return(tt.mappedID, tt.mappingErr)
					if tt.mappingErr == nil {
						stores.EXPECT().GetRequestLogStore().Return(logStore)
						logStore.EXPECT().List(gomock.Any(), tt.mappedID).Return(logs, tt.listErr)
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
				var notFound *RequestHistoryNotFoundError
				require.ErrorAs(t, err, &notFound)
				assert.Empty(t, notFound.RequestID)
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

func TestRequestHistoryNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want RequestHistoryNotFoundError
	}{
		{name: "request ID", err: fmt.Errorf("lookup failed: %w", &RequestHistoryNotFoundError{RequestID: "request/queue/1"}), want: RequestHistoryNotFoundError{RequestID: "request/queue/1"}},
		{name: "URI", err: fmt.Errorf("lookup failed: %w", &RequestHistoryNotFoundError{URI: "git://repo/commit/1"}), want: RequestHistoryNotFoundError{URI: "git://repo/commit/1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsRequestHistoryNotFound(tt.err))
			var notFound *RequestHistoryNotFoundError
			require.ErrorAs(t, tt.err, &notFound)
			assert.Equal(t, tt.want, *notFound)
		})
	}
	assert.False(t, IsRequestHistoryNotFound(errors.New("other")))
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
