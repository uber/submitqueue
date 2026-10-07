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

package storage

import (
	"sync"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	requestcore "github.com/uber/submitqueue/submitqueue/gateway/core/request"
)

func (s *StorageContractSuite) TestStorage_RequestReceiptListAndCursor() {
	const queue = "receipt-list"
	store := s.forGatewayQueue(queue).GetRequestReceiptStore()
	lower := entity.RequestReceipt{Queue: queue, ReceivedAtMs: 100, RequestID: "1"}
	ten := entity.RequestReceipt{Queue: queue, ReceivedAtMs: 200, RequestID: "10"}
	nine := entity.RequestReceipt{Queue: queue, ReceivedAtMs: 200, RequestID: "9"}
	upper := entity.RequestReceipt{Queue: queue, ReceivedAtMs: 300, RequestID: "2"}
	for _, receipt := range []entity.RequestReceipt{ten, upper, lower, nine} {
		require.NoError(s.T(), store.Create(s.ctx, receipt))
	}
	require.ErrorIs(s.T(), store.Create(s.ctx, nine), storage.ErrAlreadyExists)
	other := s.forGatewayQueue("receipt-list-other").GetRequestReceiptStore()
	require.NoError(s.T(), other.Create(s.ctx, entity.RequestReceipt{Queue: "receipt-list-other", ReceivedAtMs: nine.ReceivedAtMs, RequestID: nine.RequestID}))
	require.Error(s.T(), other.Create(s.ctx, nine))

	for _, tt := range []struct {
		name   string
		bounds storage.RequestReceiptRange
		want   []entity.RequestReceipt
	}{
		{"bounded string order", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 300, Limit: 10}, []entity.RequestReceipt{nine, ten, lower}},
		{"first page", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 300, Limit: 1}, []entity.RequestReceipt{nine}},
		{"continuation within timestamp tie", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 300, Before: storage.RequestReceiptCursor{ReceivedAtMs: 200, RequestID: "9"}, Limit: 1}, []entity.RequestReceipt{ten}},
		{"continuation across timestamps", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 300, Before: storage.RequestReceiptCursor{ReceivedAtMs: 200, RequestID: "10"}, Limit: 1}, []entity.RequestReceipt{lower}},
		{"exhausted", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 300, Before: storage.RequestReceiptCursor{ReceivedAtMs: 100, RequestID: "1"}, Limit: 1}, []entity.RequestReceipt{}},
		{"empty", storage.RequestReceiptRange{ReceivedBeforeMs: 100, Limit: 10}, []entity.RequestReceipt{}},
	} {
		s.Run(tt.name, func() {
			got, err := store.List(s.ctx, tt.bounds)
			require.NoError(s.T(), err)
			assert.Equal(s.T(), tt.want, got)
		})
	}
	got, err := other.List(s.ctx, storage.RequestReceiptRange{ReceivedBeforeMs: 300, Limit: 10})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []entity.RequestReceipt{{Queue: "receipt-list-other", ReceivedAtMs: nine.ReceivedAtMs, RequestID: nine.RequestID}}, got)
}

func (s *StorageContractSuite) TestStorage_RequestReceiptMaterialization() {
	const queue = "receipt-materialization"
	stores := s.forGatewayQueue(queue)
	summary := entity.RequestSummary{
		Queue: queue, RequestID: "1", ReceivedAtMs: 100, ChangeURIs: []string{"uri/receipt"},
		Status: entity.RequestStatusAccepting, StatusTimestampMs: 100, Version: 1,
	}
	require.NoError(s.T(), stores.GetRequestSummaryStore().Create(s.ctx, summary))
	materializer := requestcore.NewMaterializer(s.gatewayFactory)
	bounds := storage.RequestReceiptRange{ReceivedBeforeMs: 1000, Limit: 10}
	require.NoError(s.T(), materializer.PersistLog(s.ctx, entity.RequestLog{
		Queue: queue, RequestID: summary.RequestID, TimestampMs: 150,
		Type: entity.RequestLogTypeEvent, Event: entity.RequestEventBuilding,
	}))
	got, err := stores.GetRequestReceiptStore().List(s.ctx, bounds)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), got)

	logs := []entity.RequestLog{
		{Queue: queue, RequestID: summary.RequestID, Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusStarted, TimestampMs: 200},
		{Queue: queue, RequestID: summary.RequestID, Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusLanded, TimestampMs: 300, RequestVersion: 2},
	}
	var writes sync.WaitGroup
	results := make(chan error, len(logs))
	for _, log := range logs {
		writes.Add(1)
		go func() {
			defer writes.Done()
			results <- materializer.PersistLog(s.ctx, log)
		}()
	}
	writes.Wait()
	close(results)
	for err := range results {
		require.NoError(s.T(), err)
	}
	require.NoError(s.T(), materializer.PersistLog(s.ctx, entity.RequestLog{
		Queue: queue, RequestID: summary.RequestID, Type: entity.RequestLogTypeStatus,
		Status: entity.RequestStatusAccepted, TimestampMs: 400,
	}))
	got, err = stores.GetRequestReceiptStore().List(s.ctx, bounds)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []entity.RequestReceipt{{Queue: queue, RequestID: summary.RequestID, ReceivedAtMs: 100}}, got)
	current, err := stores.GetRequestSummaryStore().Get(s.ctx, summary.RequestID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), entity.RequestStatusLanded, current.Status)
	assert.Equal(s.T(), summary.ReceivedAtMs, current.ReceivedAtMs)
}
