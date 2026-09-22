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

package record

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type dlqMocks struct {
	factory      *storagemock.MockFactory
	store        *storagemock.MockStorage
	requestStore *storagemock.MockRequestStore
	materializer *requestlogmock.MockMaterializer
	metricsScope tally.TestScope
}

func TestDLQControllerIdentity(t *testing.T) {
	c, _ := newDLQControllerForTest(t, gomock.NewController(t))

	assert.Equal(t, "record_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("record_dlq"), c.TopicKey())
	assert.Equal(t, "stovepipe-record-dlq", c.ConsumerGroup())
}

func TestDLQControllerRetainsAbandonedRecordHistory(t *testing.T) {
	tests := []struct {
		name       string
		failure    failure.Failure
		hasFailure bool
	}{
		{
			name:       "promotion failure",
			failure:    failure.Failure{Message: "failed to promote: permission denied"},
			hasFailure: true,
		},
		{
			name:       "other record failure",
			failure:    failure.Failure{Message: "hook publish failed"},
			hasFailure: true,
		},
		{
			name: "missing failure attribution",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newDLQControllerForTest(t, ctrl)
			expectDLQRequestLoad(m, requestWithState(entity.RequestStateSucceeded), nil)
			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).DoAndReturn(
				func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
					assert.Equal(t, entity.RequestEventRecordAbandoned, log.Event)
					assert.Equal(t, "event/record_abandoned/repository", log.ID)
					assert.Equal(t, testID, log.RequestID)
					assert.Empty(t, log.State)
					assert.Empty(t, log.Metadata)
					return nil
				},
			)

			require.NoError(t, c.Process(queueContext(), newDLQDelivery(t, ctrl, recordPayload(t, testID), testQueue, tt.failure, tt.hasFailure)))

			counterName := "record_dlq_controller.record_dlq.requests_abandoned+queue=monorepo/main"
			counter, ok := m.metricsScope.Snapshot().Counters()[counterName]
			require.True(t, ok)
			assert.EqualValues(t, 1, counter.Value())
		})
	}
}

func TestDLQControllerRetriesDurableStateFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(dlqMocks)
	}{
		{
			name: "request load",
			setup: func(m dlqMocks) {
				expectDLQRequestLoad(m, entity.Request{}, errors.New("db down"))
			},
		},
		{
			name: "history persistence",
			setup: func(m dlqMocks) {
				expectDLQRequestLoad(m, requestWithState(entity.RequestStateSucceeded), nil)
				m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(errors.New("db down"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newDLQControllerForTest(t, ctrl)
			tt.setup(m)

			err := c.Process(queueContext(), newDLQDelivery(t, ctrl, recordPayload(t, testID), testQueue, failure.Failure{}, false))
			require.Error(t, err)
		})
	}
}

func TestDLQControllerAcknowledgesMissingRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newDLQControllerForTest(t, ctrl)
	expectDLQRequestLoad(m, entity.Request{}, storage.ErrNotFound)

	require.NoError(t, c.Process(queueContext(), newDLQDelivery(t, ctrl, recordPayload(t, testID), testQueue, failure.Failure{}, false)))
}

func TestDLQControllerAcknowledgesUnresolvableMessages(t *testing.T) {
	otherQueuePayload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: testID, QueueName: "monorepo/other"})
	require.NoError(t, err)
	emptyIDPayload, err := stovepipemq.Marshal(&stovepipemq.Record{QueueName: testQueue})
	require.NoError(t, err)

	tests := []struct {
		name    string
		payload []byte
		tenant  string
		setup   func(dlqMocks)
	}{
		{name: "malformed payload", payload: []byte("not protobuf json"), tenant: testQueue},
		{name: "queue identity mismatch", payload: otherQueuePayload, tenant: testQueue},
		{name: "empty request id", payload: emptyIDPayload, tenant: testQueue},
		{
			name:    "unresolvable queue",
			payload: recordPayload(t, testID),
			tenant:  testQueue,
			setup: func(m dlqMocks) {
				m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, errors.New("unknown queue"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newDLQControllerForTest(t, ctrl)
			if tt.setup != nil {
				tt.setup(m)
			}

			require.NoError(t, c.Process(queueContext(), newDLQDelivery(t, ctrl, tt.payload, tt.tenant, failure.Failure{}, false)))
		})
	}
}

func newDLQControllerForTest(t *testing.T, ctrl *gomock.Controller) (*DLQController, dlqMocks) {
	t.Helper()
	scope := tally.NewTestScope("", nil)
	m := dlqMocks{
		factory:      storagemock.NewMockFactory(ctrl),
		store:        storagemock.NewMockStorage(ctrl),
		requestStore: storagemock.NewMockRequestStore(ctrl),
		materializer: requestlogmock.NewMockMaterializer(ctrl),
		metricsScope: scope,
	}
	m.store.EXPECT().GetRequestStore().Return(m.requestStore).AnyTimes()

	return NewDLQController(
		zap.NewNop().Sugar(),
		scope,
		m.factory,
		m.materializer,
		consumer.TopicKey("record_dlq"),
		"stovepipe-record-dlq",
	), m
}

func expectDLQRequestLoad(m dlqMocks, request entity.Request, err error) {
	m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.requestStore.EXPECT().Get(gomock.Any(), testID).Return(request, err)
}

func newDLQDelivery(
	t *testing.T,
	ctrl *gomock.Controller,
	payload []byte,
	tenant string,
	originalFailure failure.Failure,
	hasFailure bool,
) *consumermock.MockDelivery {
	t.Helper()
	delivery := consumermock.NewMockDelivery(ctrl)
	msg := entityqueue.NewMessage(testID, payload, testID, nil)
	msg.Tenant = tenant
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	delivery.EXPECT().Failure().Return(originalFailure, hasFailure).AnyTimes()
	return delivery
}
