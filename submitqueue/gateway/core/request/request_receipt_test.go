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

package request

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	gwstoragemock "github.com/uber/submitqueue/submitqueue/gateway/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type materializerReceiptFixture struct {
	materializer *Materializer
	summaries    *storagemock.MockRequestSummaryStore
	uris         *storagemock.MockRequestURIStore
	logs         *storagemock.MockRequestLogStore
	receipts     *storagemock.MockRequestReceiptStore
}

func newMaterializerReceiptFixture(ctrl *gomock.Controller) materializerReceiptFixture {
	f := materializerReceiptFixture{
		summaries: storagemock.NewMockRequestSummaryStore(ctrl),
		uris:      storagemock.NewMockRequestURIStore(ctrl), logs: storagemock.NewMockRequestLogStore(ctrl),
		receipts: storagemock.NewMockRequestReceiptStore(ctrl),
	}
	stores := gwstoragemock.NewMockStorage(ctrl)
	stores.EXPECT().GetRequestSummaryStore().Return(f.summaries).AnyTimes()
	stores.EXPECT().GetRequestURIStore().Return(f.uris).AnyTimes()
	stores.EXPECT().GetRequestLogStore().Return(f.logs).AnyTimes()
	stores.EXPECT().GetRequestReceiptStore().Return(f.receipts).AnyTimes()
	factory := gwstoragemock.NewMockFactory(ctrl)
	factory.EXPECT().For(gomock.Any()).Return(stores, nil).AnyTimes()
	f.materializer = NewMaterializer(factory)
	return f
}

func TestMaterializer_ActivatesReceiptMapping(t *testing.T) {
	for _, status := range []entity.RequestStatus{entity.RequestStatusAccepted, entity.RequestStatusStarted, entity.RequestStatusLanded} {
		t.Run(string(status), func(t *testing.T) {
			f := newMaterializerReceiptFixture(gomock.NewController(t))
			current := testRequestSummary()
			log := entity.RequestLog{Queue: current.Queue, RequestID: current.RequestID, Type: entity.RequestLogTypeStatus, Status: status, TimestampMs: 20}
			gomock.InOrder(
				f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil),
				f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, nil),
				f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil),
				f.uris.EXPECT().Create(gomock.Any(), entity.RequestURI{Queue: current.Queue, ChangeURI: current.ChangeURIs[0], ReceivedAtMs: current.ReceivedAtMs, RequestID: current.RequestID}).Return(nil),
				f.uris.EXPECT().Create(gomock.Any(), entity.RequestURI{Queue: current.Queue, ChangeURI: current.ChangeURIs[1], ReceivedAtMs: current.ReceivedAtMs, RequestID: current.RequestID}).Return(nil),
				f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(current)).Return(nil),
			)
			require.NoError(t, f.materializer.PersistLog(context.Background(), log))
		})
	}
}

func TestMaterializer_EnsuresReceiptForUnchangedSummary(t *testing.T) {
	for _, tt := range []struct {
		name      string
		log       entity.RequestLog
		createErr error
	}{
		{"late accepted", entity.RequestLog{Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusAccepted, TimestampMs: 30}, nil},
		{"duplicate mapping", entity.RequestLog{Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusLanded, TimestampMs: 20, RequestVersion: 2}, storage.ErrAlreadyExists},
		{"audit event", entity.RequestLog{Type: entity.RequestLogTypeEvent, Event: entity.RequestEventBuilt, TimestampMs: 30}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newMaterializerReceiptFixture(gomock.NewController(t))
			current := testRequestSummary()
			current.Status, current.RequestVersion, current.StatusTimestampMs, current.Version = entity.RequestStatusLanded, 2, 20, 2
			log := tt.log
			log.Queue, log.RequestID = current.Queue, current.RequestID
			f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil)
			f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, nil)
			f.expectURIMappings(current, storage.ErrAlreadyExists)
			f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(current)).Return(tt.createErr)
			require.NoError(t, f.materializer.PersistLog(context.Background(), log))
		})
	}
}

func TestMaterializer_RetriesReceiptMappingFailure(t *testing.T) {
	f := newMaterializerReceiptFixture(gomock.NewController(t))
	current := testRequestSummary()
	current.Status = entity.RequestStatusAccepted
	log := entity.RequestLog{Queue: current.Queue, RequestID: current.RequestID, Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusStarted, TimestampMs: 20}
	updated := current
	updated.Status, updated.StatusTimestampMs, updated.Version = log.Status, log.TimestampMs, 2
	f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil).Times(2)
	f.expectURIMappings(current, storage.ErrAlreadyExists).Times(4)
	writeErr := errors.New("receipt write failed")
	gomock.InOrder(
		f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, nil),
		f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil),
		f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(current)).Return(writeErr),
		f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(updated, nil),
		f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(current)).Return(nil),
	)
	require.ErrorIs(t, f.materializer.PersistLog(context.Background(), log), writeErr)
	require.NoError(t, f.materializer.PersistLog(context.Background(), log))
}

func TestMaterializer_DoesNotActivateAcceptingReceipts(t *testing.T) {
	for _, log := range []entity.RequestLog{
		{Type: entity.RequestLogTypeStatus, Status: entity.RequestStatusAccepting, TimestampMs: 20},
		{Type: entity.RequestLogTypeEvent, Event: entity.RequestEventBuilding, TimestampMs: 20},
	} {
		t.Run(string(log.Type), func(t *testing.T) {
			f := newMaterializerReceiptFixture(gomock.NewController(t))
			current := testRequestSummary()
			log.Queue, log.RequestID = current.Queue, current.RequestID
			f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil)
			f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, nil)
			require.NoError(t, f.materializer.PersistLog(context.Background(), log))
		})
	}
}

func TestMaterializer_DoesNotCreateReceiptAfterURIMappingFailure(t *testing.T) {
	f := newMaterializerReceiptFixture(gomock.NewController(t))
	current := testRequestSummary()
	current.Status = entity.RequestStatusAccepted
	log := entity.RequestLog{Queue: current.Queue, RequestID: current.RequestID, Type: entity.RequestLogTypeStatus, Status: current.Status, TimestampMs: current.StatusTimestampMs}
	f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil)
	f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, nil)
	writeErr := errors.New("URI write failed")
	f.uris.EXPECT().Create(gomock.Any(), gomock.Any()).Return(writeErr)
	require.ErrorIs(t, f.materializer.PersistLog(context.Background(), log), writeErr)
}

func receiptFromTestSummary(summary entity.RequestSummary) entity.RequestReceipt {
	return entity.RequestReceipt{Queue: summary.Queue, ReceivedAtMs: summary.ReceivedAtMs, RequestID: summary.RequestID}
}

func (f materializerReceiptFixture) expectURIMappings(summary entity.RequestSummary, err error) *gomock.Call {
	return f.uris.EXPECT().Create(gomock.Any(), gomock.Any()).Return(err).Times(len(summary.ChangeURIs))
}

func TestMaterializer_RetriesPartialURIActivation(t *testing.T) {
	f := newMaterializerReceiptFixture(gomock.NewController(t))
	current := testRequestSummary()
	current.Status = entity.RequestStatusAccepted
	log := entity.RequestLog{Queue: current.Queue, RequestID: current.RequestID, Type: entity.RequestLogTypeStatus, Status: current.Status, TimestampMs: current.StatusTimestampMs}
	f.logs.EXPECT().Insert(gomock.Any(), log).Return(nil).Times(2)
	f.summaries.EXPECT().Get(gomock.Any(), current.RequestID).Return(current, nil).Times(2)
	mapping := func(uri string) entity.RequestURI {
		return entity.RequestURI{Queue: current.Queue, ChangeURI: uri, ReceivedAtMs: current.ReceivedAtMs, RequestID: current.RequestID}
	}
	writeErr := errors.New("URI write failed")
	gomock.InOrder(
		f.uris.EXPECT().Create(gomock.Any(), mapping(current.ChangeURIs[0])).Return(nil),
		f.uris.EXPECT().Create(gomock.Any(), mapping(current.ChangeURIs[1])).Return(writeErr),
		f.uris.EXPECT().Create(gomock.Any(), mapping(current.ChangeURIs[0])).Return(storage.ErrAlreadyExists),
		f.uris.EXPECT().Create(gomock.Any(), mapping(current.ChangeURIs[1])).Return(nil),
		f.receipts.EXPECT().Create(gomock.Any(), receiptFromTestSummary(current)).Return(nil),
	)
	require.ErrorIs(t, f.materializer.PersistLog(context.Background(), log), writeErr)
	require.NoError(t, f.materializer.PersistLog(context.Background(), log))
}
