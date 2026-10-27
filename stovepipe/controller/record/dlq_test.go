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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/stovepipe/core/hookevent"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
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
	factStore    *storagemock.MockValidationFactStore
	hooks        *hookRecorder
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
		state      entity.RequestState
		failure    failure.Failure
		hasFailure bool
	}{
		{
			name:       "promotion failure",
			state:      entity.RequestStateSucceeded,
			failure:    failure.Failure{Message: "failed to promote: permission denied"},
			hasFailure: true,
		},
		{
			name:       "other record failure",
			state:      entity.RequestStateSucceeded,
			failure:    failure.Failure{Message: "hook publish failed"},
			hasFailure: true,
		},
		{
			name:  "missing failure attribution",
			state: entity.RequestStateSucceeded,
		},
		{name: "cancelled request", state: entity.RequestStateCancelled},
		{name: "superseded request", state: entity.RequestStateSuperseded},
		{name: "nonterminal request", state: entity.RequestStateProcessing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newDLQControllerForTest(t, ctrl)
			expectDLQRequestLoad(m, requestWithState(tt.state), nil)
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

			assert.Empty(t, m.hooks.events)

			counterName := "record_dlq_controller.record_dlq.requests_abandoned+queue=monorepo/main"
			counter, ok := m.metricsScope.Snapshot().Counters()[counterName]
			require.True(t, ok)
			assert.EqualValues(t, 1, counter.Value())
		})
	}
}

func TestDLQControllerRecordsFailedRepository(t *testing.T) {
	for _, test := range []struct {
		name        string
		hookFailure bool
		existing    *entity.ValidationFact
	}{
		{name: "missing fact"},
		{name: "retry after hook publish failure", hookFailure: true},
		{name: "existing partial fact", existing: &entity.ValidationFact{Degree: entity.DegreePartial}},
		{name: "existing green fact", existing: &entity.ValidationFact{Degree: entity.DegreeGreen}},
		{name: "existing broken fact", existing: &entity.ValidationFact{Degree: entity.DegreeBroken}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newDLQControllerForTest(t, ctrl)
			request := requestWithState(entity.RequestStateFailed)
			m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil).Times(2)
			m.requestStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil).Times(2)
			var stored *entity.ValidationFact
			if test.existing != nil {
				fact := *test.existing
				fact.URI, fact.RequestID, fact.CreatedAt = testURI, testID, 1234
				stored = &fact
			}
			m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, fact entity.ValidationFact) error {
					assert.Equal(t, testURI, fact.URI)
					assert.Empty(t, fact.Project)
					assert.Equal(t, testID, fact.RequestID)
					assert.Equal(t, entity.DegreeBroken, fact.Degree)
					if stored != nil {
						return storage.ErrAlreadyExists
					}
					stored = &fact
					return nil
				}).Times(2)
			m.factStore.EXPECT().Get(gomock.Any(), testURI, "").DoAndReturn(
				func(context.Context, string, string) (entity.ValidationFact, error) {
					require.NotNil(t, stored)
					return *stored, nil
				}).AnyTimes()
			var logs []entity.RequestLog
			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).DoAndReturn(
				func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
					assert.Empty(t, log.State)
					logs = append(logs, log)
					return nil
				}).Times(4)

			for attempt := range 2 {
				m.hooks.err = nil
				if test.hookFailure && attempt == 0 {
					m.hooks.err = errors.New("publish unavailable")
				}
				err := c.Process(queueContext(), newDLQDelivery(t, ctrl, recordPayload(t, testID), testQueue, failure.Failure{}, false))
				if m.hooks.err != nil {
					require.ErrorIs(t, err, m.hooks.err)
				} else {
					require.NoError(t, err)
				}
			}

			require.NotNil(t, stored)
			if test.existing == nil {
				assert.Equal(t, entity.DegreeBroken, stored.Degree)
			} else {
				assert.Equal(t, test.existing.Degree, stored.Degree)
				assert.EqualValues(t, 1234, stored.CreatedAt)
			}
			require.Len(t, logs, 4)
			assert.Equal(t, entity.RequestEventRecordAbandoned, logs[0].Event)
			assert.Equal(t, entity.RequestEventValidationFactRecorded, logs[1].Event)
			assert.Equal(t, logs[0].ID, logs[2].ID)
			assert.Equal(t, logs[1], logs[3])
			assert.Equal(t, stored.CreatedAt, logs[1].TimestampMs)
			assert.Equal(t, strconv.FormatFloat(stored.Degree, 'g', -1, 64), logs[1].Metadata[requestlog.MetadataKeyFactDegree])
			if test.hookFailure {
				require.Len(t, m.hooks.events, 1)
			} else {
				require.Len(t, m.hooks.events, 2)
			}
			for _, event := range m.hooks.events {
				assert.Equal(t, string(hookevent.TypeValidationRepositoryRecorded), event.GetType())
				assert.Equal(t, hookevent.NewValidationRepositoryRecorded(request).GetId(), event.GetId())
			}
			assert.Equal(t, testQueue, m.hooks.tenant)
		})
	}
}

func TestDLQControllerRetriesFailedRepositoryRecording(t *testing.T) {
	for _, stage := range []string{"abandonment history", "fact create", "existing fact read", "fact history", "hook publish", "different fact owner"} {
		t.Run(stage, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newDLQControllerForTest(t, ctrl)
			expectDLQRequestLoad(m, requestWithState(entity.RequestStateFailed), nil)
			sentinel := errors.New("unavailable")
			if stage == "abandonment history" {
				m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(sentinel)
			} else {
				m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil)
				switch stage {
				case "fact create":
					m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(sentinel)
				case "existing fact read", "different fact owner":
					m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
					if stage == "existing fact read" {
						m.factStore.EXPECT().Get(gomock.Any(), testURI, "").Return(entity.ValidationFact{}, sentinel)
					} else {
						m.factStore.EXPECT().Get(gomock.Any(), testURI, "").Return(entity.ValidationFact{
							URI: testURI, RequestID: "another-request", Degree: entity.DegreeGreen,
						}, nil)
					}
				case "fact history", "hook publish":
					m.factStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
					if stage == "fact history" {
						m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(sentinel)
					} else {
						m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil)
						m.hooks.err = sentinel
					}
				}
			}
			err := c.Process(queueContext(), newDLQDelivery(t, ctrl, recordPayload(t, testID), testQueue, failure.Failure{}, false))
			if stage == "different fact owner" {
				require.Error(t, err)
			} else {
				require.ErrorIs(t, err, sentinel)
			}
			assert.Empty(t, m.hooks.events)
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
			name: "request queue mismatch", payload: recordPayload(t, testID), tenant: testQueue,
			setup: func(m dlqMocks) {
				request := requestWithState(entity.RequestStateFailed)
				request.Queue = "monorepo/other"
				expectDLQRequestLoad(m, request, nil)
			},
		},
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
		factStore:    storagemock.NewMockValidationFactStore(ctrl),
		hooks:        &hookRecorder{},
		materializer: requestlogmock.NewMockMaterializer(ctrl),
		metricsScope: scope,
	}
	m.store.EXPECT().GetRequestStore().Return(m.requestStore).AnyTimes()
	m.store.EXPECT().GetValidationFactStore().Return(m.factStore).AnyTimes()
	publisher := mqmock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			if m.hooks.err != nil {
				return m.hooks.err
			}
			event := &basehook.HookEvent{}
			if err := basehook.Unmarshal(msg.Payload, event); err != nil {
				return err
			}
			m.hooks.events = append(m.hooks.events, event)
			m.hooks.tenant = msg.Tenant
			return nil
		}).AnyTimes()
	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: basehook.TopicKeyHook, Name: "stovepipe-hook", Queue: queue},
	})
	require.NoError(t, err)

	return NewDLQController(
		zap.NewNop().Sugar(),
		scope,
		m.factory,
		m.materializer,
		registry,
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
