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
	"fmt"
	"sync"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

// controllerStorageFixture provides shared stateful storage behavior for gateway controller tests.
type controllerStorageFixture struct {
	storage        *storagemock.MockStorage
	summaryStore   *storagemock.MockRequestSummaryStore
	queueStore     *storagemock.MockRequestQueueSummaryStore
	uriStore       *storagemock.MockRequestURIStore
	logStore       *storagemock.MockRequestLogStore
	mu             sync.Mutex
	summaries      map[string]entity.RequestSummary
	queueSummaries map[string]entity.RequestQueueSummary
	logs           []entity.RequestLog
	logInsertErr   error
}

func newControllerStorageFixture(ctrl *gomock.Controller) *controllerStorageFixture {
	fixture := &controllerStorageFixture{
		storage:        storagemock.NewMockStorage(ctrl),
		summaryStore:   storagemock.NewMockRequestSummaryStore(ctrl),
		queueStore:     storagemock.NewMockRequestQueueSummaryStore(ctrl),
		uriStore:       storagemock.NewMockRequestURIStore(ctrl),
		logStore:       storagemock.NewMockRequestLogStore(ctrl),
		summaries:      make(map[string]entity.RequestSummary),
		queueSummaries: make(map[string]entity.RequestQueueSummary),
	}
	fixture.storage.EXPECT().GetRequestSummaryStore().Return(fixture.summaryStore).AnyTimes()
	fixture.storage.EXPECT().GetRequestQueueSummaryStore().Return(fixture.queueStore).AnyTimes()
	fixture.storage.EXPECT().GetRequestURIStore().Return(fixture.uriStore).AnyTimes()
	fixture.storage.EXPECT().GetRequestLogStore().Return(fixture.logStore).AnyTimes()

	fixture.summaryStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.RequestSummary) error {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if _, ok := fixture.summaries[summary.RequestID]; ok {
			return storage.ErrAlreadyExists
		}
		fixture.summaries[summary.RequestID] = summary
		return nil
	}).AnyTimes()
	fixture.summaryStore.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, requestID string) (entity.RequestSummary, error) {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		summary, ok := fixture.summaries[requestID]
		if !ok {
			return entity.RequestSummary{}, errs.ErrNotFound
		}
		return summary, nil
	}).AnyTimes()
	fixture.summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.RequestSummary, oldVersion, newVersion int32) error {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		current, ok := fixture.summaries[summary.RequestID]
		if !ok {
			return errs.ErrNotFound
		}
		if current.Version != oldVersion {
			return errs.ErrVersionMismatch
		}
		summary.Version = newVersion
		fixture.summaries[summary.RequestID] = summary
		return nil
	}).AnyTimes()

	fixture.queueStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.RequestQueueSummary) error {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		key := queueSummaryTestKey(summary.Queue, summary.ReceivedAtMs, summary.RequestID)
		if _, ok := fixture.queueSummaries[key]; ok {
			return storage.ErrAlreadyExists
		}
		fixture.queueSummaries[key] = summary
		return nil
	}).AnyTimes()
	fixture.queueStore.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, queue string, receivedAtMs int64, requestID string) (entity.RequestQueueSummary, error) {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		summary, ok := fixture.queueSummaries[queueSummaryTestKey(queue, receivedAtMs, requestID)]
		if !ok {
			return entity.RequestQueueSummary{}, errs.ErrNotFound
		}
		return summary, nil
	}).AnyTimes()
	fixture.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.RequestQueueSummary, oldVersion, newVersion int32) error {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		key := queueSummaryTestKey(summary.Queue, summary.ReceivedAtMs, summary.RequestID)
		current, ok := fixture.queueSummaries[key]
		if !ok {
			return errs.ErrNotFound
		}
		if current.Version != oldVersion {
			return errs.ErrVersionMismatch
		}
		summary.Version = newVersion
		fixture.queueSummaries[key] = summary
		return nil
	}).AnyTimes()

	fixture.uriStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fixture.logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.RequestLog) error {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if fixture.logInsertErr != nil {
			return fixture.logInsertErr
		}
		fixture.logs = append(fixture.logs, log)
		return nil
	}).AnyTimes()
	return fixture
}

func (f *controllerStorageFixture) setLogInsertError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logInsertErr = err
}

func (f *controllerStorageFixture) addSummary(summary entity.RequestSummary) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.summaries[summary.RequestID] = summary
	f.queueSummaries[queueSummaryTestKey(summary.Queue, summary.ReceivedAtMs, summary.RequestID)] = entity.RequestQueueSummary{
		RequestID: summary.RequestID, Queue: summary.Queue, ChangeURIs: summary.ChangeURIs, ReceivedAtMs: summary.ReceivedAtMs,
		Status: summary.Status, Version: summary.Version, LastError: summary.LastError, Metadata: summary.Metadata,
	}
}

func queueSummaryTestKey(queue string, receivedAtMs int64, requestID string) string {
	return fmt.Sprintf("%s\x00%d\x00%s", queue, receivedAtMs, requestID)
}
