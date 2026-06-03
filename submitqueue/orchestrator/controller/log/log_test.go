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
	"github.com/uber-go/tally/v4"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newTestController creates a controller with test dependencies.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	return NewController(logger, scope, storage.NewStaticFactory(store), consumer.TopicKeyLog, "orchestrator-log")
}

func TestController_Process(t *testing.T) {
	tests := []struct {
		name       string
		logEntry   *entity.RequestLog // nil means use rawPayload instead
		rawPayload []byte             // used when logEntry is nil (e.g. invalid JSON)
		setupStore func(*gomock.Controller) *storagemock.MockStorage
		wantErr    bool
	}{
		{
			name: "success",
			logEntry: newRequestLog(
				"test-queue/1", entity.RequestStatusStarted, 1, "", nil,
			),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockLogStore := storagemock.NewMockRequestLogStore(ctrl)
				mockLogStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil)
				store := storagemock.NewMockStorage(ctrl)
				store.EXPECT().GetRequestLogStore().Return(mockLogStore).AnyTimes()
				return store
			},
			wantErr: false,
		},
		{
			name:       "invalid JSON",
			rawPayload: []byte(`{"invalid": json"}`),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				return storagemock.NewMockStorage(ctrl)
			},
			wantErr: true,
		},
		{
			name: "storage failure",
			logEntry: newRequestLog(
				"test-queue/2", entity.RequestStatusError, 3, "merge conflict", map[string]string{"step": "merge"},
			),
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockLogStore := storagemock.NewMockRequestLogStore(ctrl)
				mockLogStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(fmt.Errorf("database connection failed"))
				store := storagemock.NewMockStorage(ctrl)
				store.EXPECT().GetRequestLogStore().Return(mockLogStore).AnyTimes()
				return store
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			var payload []byte
			if tt.logEntry != nil {
				var err error
				payload, err = tt.logEntry.ToBytes()
				require.NoError(t, err)
			} else {
				payload = tt.rawPayload
			}

			store := tt.setupStore(ctrl)
			controller := newTestController(t, ctrl, store)

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

// newRequestLog is a helper that returns a pointer to a RequestLog for use in test tables.
func newRequestLog(requestID string, status entity.RequestStatus, requestVersion int32, lastError string, metadata map[string]string) *entity.RequestLog {
	log := entity.NewRequestLog(requestID, status, requestVersion, lastError, metadata)
	return &log
}
