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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestReconcileTerminalState(t *testing.T) {
	tests := []struct {
		name           string
		request        entity.Request
		outcome        TerminalOutcome
		wantUpdate     bool
		wantPublish    bool
		wantReconciled bool
		wantStatus     entity.RequestStatus
		wantVersion    int32
		wantErr        bool
	}{
		{
			name:    "updates non-terminal request and publishes context",
			request: entity.Request{ID: "q/1", State: entity.RequestStateProcessing, Version: 3},
			outcome: TerminalOutcome{
				State:     entity.RequestStateError,
				LastError: "merge conflict",
				Metadata:  map[string]string{"reason_code": "merge_conflict"},
			},
			wantUpdate:     true,
			wantPublish:    true,
			wantReconciled: true,
			wantStatus:     entity.RequestStatusError,
			wantVersion:    4,
		},
		{
			name:           "same terminal state republishes log without CAS",
			request:        entity.Request{ID: "q/1", State: entity.RequestStateLanded, Version: 5},
			outcome:        TerminalOutcome{State: entity.RequestStateLanded},
			wantPublish:    true,
			wantReconciled: true,
			wantStatus:     entity.RequestStatusLanded,
			wantVersion:    5,
		},
		{
			name:    "different terminal outcome is preserved",
			request: entity.Request{ID: "q/1", State: entity.RequestStateCancelled, Version: 5},
			outcome: TerminalOutcome{State: entity.RequestStateLanded},
		},
		{
			name:    "non-terminal target is rejected",
			request: entity.Request{ID: "q/1", State: entity.RequestStateStarted, Version: 1},
			outcome: TerminalOutcome{State: entity.RequestStateValidated},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			requestStore := storagemock.NewMockRequestStore(ctrl)
			if tt.wantUpdate {
				requestStore.EXPECT().UpdateState(
					gomock.Any(),
					tt.request.ID,
					tt.request.Version,
					tt.request.Version+1,
					tt.outcome.State,
				).Return(nil)
			}

			registry := consumer.TopicRegistry{}
			if tt.wantPublish {
				registry = newTerminalTestRegistry(t, ctrl, func(log entity.RequestLog) {
					assert.Equal(t, tt.wantStatus, log.Status)
					assert.Equal(t, tt.wantVersion, log.RequestVersion)
					assert.Equal(t, tt.outcome.LastError, log.LastError)
					if tt.outcome.Metadata == nil {
						assert.Empty(t, log.Metadata)
					} else {
						assert.Equal(t, tt.outcome.Metadata, log.Metadata)
					}
				})
			}

			reconciled, err := ReconcileTerminalState(context.Background(), requestStore, registry, tt.request, tt.outcome)
			assert.Equal(t, tt.wantReconciled, reconciled)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func newTerminalTestRegistry(
	t *testing.T,
	ctrl *gomock.Controller,
	checkLog func(entity.RequestLog),
) consumer.TopicRegistry {
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, message entityqueue.Message) error {
			logEntry, err := entity.RequestLogFromBytes(message.Payload)
			require.NoError(t, err)
			checkLog(logEntry)
			return nil
		},
	)

	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher)

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{
		Key:   topickey.TopicKeyLog,
		Name:  "log",
		Queue: queue,
	}})
	require.NoError(t, err)
	return registry
}
