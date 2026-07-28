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

package log

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestController_Process(t *testing.T) {
	tests := []struct {
		name       string
		logEntry   *entity.RequestLog
		rawPayload []byte
		setupStore func(*gomock.Controller) *storagemock.MockStorage
		wantErr    bool
	}{
		{
			name:     "success",
			logEntry: newRequestLog("test-queue/1", entity.RequestStatusStarted, 1, "", nil),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				return newLogControllerStore(ctrl, nil, nil, nil, nil)
			},
		},
		{
			name:       "invalid JSON",
			rawPayload: []byte(`{"invalid": json"}`),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage { return storagemock.NewMockStorage(ctrl) },
			wantErr:    true,
		},
		{
			name:     "audit insert failure",
			logEntry: newRequestLog("test-queue/2", entity.RequestStatusError, 3, "merge conflict", nil),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				return newLogControllerStore(ctrl, fmt.Errorf("audit down"), nil, nil, nil)
			},
			wantErr: true,
		},
		{
			name:     "summary read failure",
			logEntry: newRequestLog("test-queue/2", entity.RequestStatusError, 3, "merge conflict", nil),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				return newLogControllerStore(ctrl, nil, fmt.Errorf("summary down"), nil, nil)
			},
			wantErr: true,
		},
		{
			name:     "summary update failure",
			logEntry: newRequestLog("test-queue/2", entity.RequestStatusError, 3, "merge conflict", nil),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				return newLogControllerStore(ctrl, nil, nil, fmt.Errorf("summary update down"), nil)
			},
			wantErr: true,
		},
		{
			name:     "queue projection failure",
			logEntry: newRequestLog("test-queue/2", entity.RequestStatusError, 3, "merge conflict", nil),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				return newLogControllerStore(ctrl, nil, nil, nil, fmt.Errorf("queue update down"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			payload := tt.rawPayload
			if tt.logEntry != nil {
				var err error
				payload, err = tt.logEntry.ToBytes()
				require.NoError(t, err)
			}
			controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, tt.setupStore(ctrl), topickey.TopicKeyLog, "gateway-log")
			msg := entityqueue.NewMessage("test-queue/1", payload, "test-queue", nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err := controller.Process(context.Background(), delivery)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func newLogControllerStore(ctrl *gomock.Controller, insertErr, getErr, updateErr, queueErr error) *storagemock.MockStorage {
	store := storagemock.NewMockStorage(ctrl)
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	queueStore := storagemock.NewMockRequestQueueSummaryStore(ctrl)
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
	store.EXPECT().GetRequestQueueSummaryStore().Return(queueStore).AnyTimes()
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(insertErr)
	if insertErr != nil {
		return store
	}
	summaryStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.RequestSummary{
		RequestID: "test-queue/2", Queue: "test-queue", ChangeURIs: []string{}, ReceivedAtMs: 1,
		Status: entity.RequestStatusAccepted, StatusTimestampMs: 1, Version: 1, Metadata: map[string]string{},
	}, getErr)
	if getErr != nil {
		return store
	}
	summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(updateErr)
	if updateErr != nil {
		return store
	}
	queueStore.EXPECT().Get(gomock.Any(), "test-queue", int64(1), "test-queue/2").Return(entity.RequestQueueSummary{
		RequestID: "test-queue/2", Queue: "test-queue", ChangeURIs: []string{}, ReceivedAtMs: 1,
		Status: entity.RequestStatusAccepted, Version: 1, Metadata: map[string]string{},
	}, nil)
	queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(queueErr)
	return store
}

func newRequestLog(requestID string, status entity.RequestStatus, requestVersion int32, lastError string, metadata map[string]string) *entity.RequestLog {
	log := entity.NewRequestLog(requestID, status, requestVersion, lastError, metadata)
	return &log
}
