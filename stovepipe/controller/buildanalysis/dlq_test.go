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

package buildanalysis

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
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
	ctrl         *gomock.Controller
	factory      *storagemock.MockFactory
	store        *storagemock.MockStorage
	builds       *storagemock.MockBuildStore
	requests     *storagemock.MockRequestStore
	materializer *requestlogmock.MockMaterializer
}

func newDLQTestController(t *testing.T) (*DLQController, dlqMocks) {
	t.Helper()

	ctrl := gomock.NewController(t)
	m := dlqMocks{
		ctrl:         ctrl,
		factory:      storagemock.NewMockFactory(ctrl),
		store:        storagemock.NewMockStorage(ctrl),
		builds:       storagemock.NewMockBuildStore(ctrl),
		requests:     storagemock.NewMockRequestStore(ctrl),
		materializer: requestlogmock.NewMockMaterializer(ctrl),
	}
	m.store.EXPECT().GetBuildStore().Return(m.builds).AnyTimes()
	m.store.EXPECT().GetRequestStore().Return(m.requests).AnyTimes()

	c := NewDLQController(zap.NewNop().Sugar(), tally.NoopScope, m.factory, m.materializer,
		consumer.TopicKey("buildanalysis_dlq"), "stovepipe-buildanalysis-dlq")

	return c, m
}

func (m dlqMocks) expectLoad(request entity.Request) {
	m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
	m.builds.EXPECT().Get(gomock.Any(), testBuildID).
		Return(entity.Build{ID: testBuildID, RequestID: request.ID, Status: entity.BuildStatusRunning}, nil)
	m.requests.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
}

func (m dlqMocks) delivery(msg entityqueue.Message, hasFailure bool) *consumermock.MockDelivery {
	d := consumermock.NewMockDelivery(m.ctrl)
	d.EXPECT().Message().Return(msg)
	d.EXPECT().Failure().Return(failure.Failure{Message: "detector unavailable"}, hasFailure).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()

	return d
}

func TestDLQRetainsHistoryWithoutChangingValidation(t *testing.T) {
	for _, state := range []entity.RequestState{
		entity.RequestStateAccepted, entity.RequestStateProcessing, entity.RequestStateSucceeded,
		entity.RequestStateFailed, entity.RequestStateCancelled, entity.RequestStateSuperseded,
	} {
		t.Run(string(state), func(t *testing.T) {
			c, m := newDLQTestController(t)
			request := entity.Request{ID: "request/1", Queue: testQueue, State: state, Version: 3}
			m.expectLoad(request)
			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).DoAndReturn(
				func(_ context.Context, _ storage.Storage, log entity.RequestLog) error {
					assert.Equal(t, "event/build_analysis_abandoned/"+testBuildID, log.ID)
					assert.Equal(t, request.ID, log.RequestID)
					assert.Equal(t, testQueue, log.Queue)
					assert.Equal(t, entity.RequestEventBuildAnalysisAbandoned, log.Event)
					assert.Equal(t, map[string]string{requestlog.MetadataKeyBuildID: testBuildID}, log.Metadata)
					assert.Empty(t, log.State)
					assert.Zero(t, log.RequestVersion)
					assert.Empty(t, log.OutcomeReason)
					return nil
				},
			)

			require.NoError(t, c.Process(context.Background(), m.delivery(analysisMessage(t), state.IsTerminal())))
		})
	}
}

func TestDLQReturnsStateAndHistoryErrorsForRetry(t *testing.T) {
	for _, step := range []string{"load build", "load request", "retain history"} {
		t.Run(step, func(t *testing.T) {
			c, m := newDLQTestController(t)
			m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)

			if step == "load build" {
				m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, assert.AnError)
			} else {
				m.builds.EXPECT().Get(gomock.Any(), testBuildID).
					Return(entity.Build{ID: testBuildID, RequestID: "request/1", Status: entity.BuildStatusFailed}, nil)

				if step == "load request" {
					m.requests.EXPECT().Get(gomock.Any(), "request/1").Return(entity.Request{}, assert.AnError)
				} else {
					m.requests.EXPECT().Get(gomock.Any(), "request/1").
						Return(entity.Request{ID: "request/1", Queue: testQueue}, nil)
					m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(assert.AnError)
				}
			}

			err := c.Process(context.Background(), m.delivery(analysisMessage(t), false))
			require.ErrorIs(t, err, assert.AnError)
		})
	}
}

func TestDLQAcknowledgesUnusableMessages(t *testing.T) {
	for _, kind := range []string{
		"malformed payload", "empty queue", "empty tenant", "tenant mismatch", "metadata mismatch", "empty build ID",
		"unresolvable queue", "missing build", "build without request", "missing request", "request queue mismatch",
	} {
		t.Run(kind, func(t *testing.T) {
			c, m := newDLQTestController(t)
			msg := analysisMessage(t)

			switch kind {
			case "malformed payload":
				msg.Payload = []byte("{")
			case "empty queue":
				msg.Payload = []byte(`{"id":"build/42"}`)
			case "empty tenant":
				msg.Tenant = ""
			case "tenant mismatch":
				msg.Tenant = "other/main"
			case "metadata mismatch":
				msg.Metadata[entityqueue.MetadataKeyQueueName] = "other/main"
			case "empty build ID":
				msg.Payload = []byte(`{"queue_name":"validation/main"}`)
			case "unresolvable queue":
				m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, assert.AnError)
			default:
				m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
				switch kind {
				case "missing build":
					m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, storage.ErrNotFound)
				case "build without request":
					m.builds.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{ID: testBuildID}, nil)
				case "missing request":
					m.builds.EXPECT().Get(gomock.Any(), testBuildID).
						Return(entity.Build{ID: testBuildID, RequestID: "request/1"}, nil)
					m.requests.EXPECT().Get(gomock.Any(), "request/1").Return(entity.Request{}, storage.ErrNotFound)
				case "request queue mismatch":
					m.builds.EXPECT().Get(gomock.Any(), testBuildID).
						Return(entity.Build{ID: testBuildID, RequestID: "request/1"}, nil)
					m.requests.EXPECT().Get(gomock.Any(), "request/1").
						Return(entity.Request{ID: "request/1", Queue: "other/main"}, nil)
				}
			}

			require.NoError(t, c.Process(context.Background(), m.delivery(msg, false)))
		})
	}
}

func TestDLQDeduplicatesHistoryAcrossRetriesAndNotices(t *testing.T) {
	c, m := newDLQTestController(t)
	c.materializer = requestlog.NewMaterializer(tally.NoopScope)
	logStore := storagemock.NewMockRequestLogStore(m.ctrl)
	m.store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()
	retained := make(map[string]entity.RequestLog)
	failFirstWrite := true

	logStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, log entity.RequestLog) error {
			require.NoError(t, log.Validate())
			if _, exists := retained[log.ID]; exists {
				return storage.ErrAlreadyExists
			}

			retained[log.ID] = log
			if failFirstWrite {
				failFirstWrite = false
				return assert.AnError
			}

			return nil
		},
	).Times(4)
	logStore.EXPECT().Get(gomock.Any(), "request/1", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, id string) (entity.RequestLog, error) {
			return retained[id], nil
		},
	).Times(2)

	for i, buildID := range []string{testBuildID, testBuildID, testBuildID, "build/43"} {
		request := entity.Request{ID: "request/1", Queue: testQueue, State: entity.RequestStateProcessing, Version: int32(i + 1)}
		if i > 0 {
			request.State = entity.RequestStateSucceeded
		}

		m.factory.EXPECT().For(storage.Config{QueueName: testQueue}).Return(m.store, nil)
		m.builds.EXPECT().Get(gomock.Any(), buildID).Return(entity.Build{ID: buildID, RequestID: request.ID}, nil)
		m.requests.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
		msg := analysisMessage(t)
		msg.ID = fmt.Sprintf("notice-%d", i)
		msg.PartitionKey = buildID
		payload, err := stovepipemq.Marshal(&stovepipemq.BuildAnalysis{Id: buildID, QueueName: testQueue})
		require.NoError(t, err)
		msg.Payload = payload

		err = c.Process(context.Background(), m.delivery(msg, i != 0))
		if i == 0 {
			require.ErrorIs(t, err, assert.AnError)
		} else {
			require.NoError(t, err)
		}
	}

	require.Len(t, retained, 2)
	for _, buildID := range []string{testBuildID, "build/43"} {
		log, ok := retained["event/build_analysis_abandoned/"+buildID]
		require.True(t, ok)
		assert.Equal(t, buildID, log.Metadata[requestlog.MetadataKeyBuildID])
	}
}
