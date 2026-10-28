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

package process

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/hookevent"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	queueconfigdefault "github.com/uber/submitqueue/stovepipe/extension/queueconfig/default"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcecontrolmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue   = "monorepo/main"
	testID      = "7"
	testOlderID = "3"
	testURI     = "git://repo/monorepo/main/abc123"
)

func queueContext(queueName string) context.Context {
	ctx := entityqueue.WithQueueName(context.Background(), queueName)
	return metrics.WithContextTags(ctx, metrics.NewTag("queue", queueName))
}

type processMocks struct {
	reqStore      *storagemock.MockRequestStore
	queueStore    *storagemock.MockQueueStore
	sourceFactory *sourcecontrolmock.MockFactory
	sourceControl *sourcecontrolmock.MockSourceControl
	store         *storagemock.MockStorage
	materializer  *requestlogmock.MockMaterializer
	publisher     *mqmock.MockPublisher
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, processMocks) {
	t.Helper()
	return newControllerWithScope(t, ctrl, tally.NewTestScope("test", nil))
}

func newControllerWithScope(t *testing.T, ctrl *gomock.Controller, scope tally.Scope) (*Controller, processMocks) {
	t.Helper()
	m := processMocks{
		reqStore:      storagemock.NewMockRequestStore(ctrl),
		queueStore:    storagemock.NewMockQueueStore(ctrl),
		sourceFactory: sourcecontrolmock.NewMockFactory(ctrl),
		sourceControl: sourcecontrolmock.NewMockSourceControl(ctrl),
		store:         storagemock.NewMockStorage(ctrl),
		materializer:  requestlogmock.NewMockMaterializer(ctrl),
		publisher:     mqmock.NewMockPublisher(ctrl),
	}

	m.store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	m.store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyProcess, Name: "process", Queue: queue},
		{Key: stovepipemq.TopicKeyBuild, Name: "build", Queue: queue},
		{Key: basehook.TopicKeyHook, Name: "stovepipe-hook", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(
		zap.NewNop().Sugar(),
		scope,
		staticStorageFactory{store: m.store},
		m.materializer,
		queueconfigdefault.NewStore(),
		m.sourceFactory,
		registry,
		stovepipemq.TopicKeyProcess,
		"stovepipe-process",
	)
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	msg := entityqueue.NewMessage(testID, payload, testQueue, nil)
	msg.Tenant = testQueue
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func processPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return b
}

func TestProcessRejectsTenantPayloadQueueMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)
	msg := entityqueue.NewMessage(testID, processPayload(t, testID), testQueue, nil)
	msg.Tenant = "other-queue"
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()

	err := c.Process(queueContext(testQueue), d)

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func acceptedRequest(id string) entity.Request {
	return entity.Request{
		ID:      id,
		Queue:   testQueue,
		URI:     testURI,
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
}

func TestProcessBuildPublishRequiresRegisteredTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)
	c.registry = consumer.TopicRegistry{}

	request := entity.Request{
		ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
	}
	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
	expectProcessingLog(t, m, request)

	err := c.Process(queueContext(testQueue), delivery(t, ctrl, processPayload(t, testID)))

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func expectAdmit(t *testing.T, m processMocks, id string) {
	t.Helper()

	admitted, slotCall := expectAdmitWrites(t, m, id)
	expectProcessingLogAndHandoff(t, m, admitted).After(slotCall)
}

// expectAdmitWrites expects the full-build admit of id against an idle queue row at version 1:
// the request transition, then the slot count. It returns the admitted request and the count call.
func expectAdmitWrites(t *testing.T, m processMocks, id string) (entity.Request, *gomock.Call) {
	t.Helper()

	admitted, requestCall := expectRequestAdmit(t, m, id)
	slotCall := m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
		Name: testQueue, LatestRequestID: id, InFlightCount: 1, Version: 1,
	}, int32(1), int32(2)).Return(nil).After(requestCall)
	return admitted, slotCall
}

// expectRequestAdmit expects the full-build accepted→processing CAS of id at version 1 and returns
// the admitted request with its new version.
func expectRequestAdmit(t *testing.T, m processMocks, id string) (entity.Request, *gomock.Call) {
	t.Helper()

	admitted := acceptedRequest(id)
	admitted.State = entity.RequestStateProcessing
	admitted.BuildStrategy = entity.BuildStrategyFull
	requestCall := m.reqStore.EXPECT().Update(gomock.Any(), admitted, int32(1), int32(2)).Return(nil)
	admitted.Version = 2
	return admitted, requestCall
}

func expectStartAnnounceAndBuildPublish(t *testing.T, m processMocks, request entity.Request) {
	t.Helper()
	expectProcessingLogAndHandoff(t, m, request)
}

func expectProcessingLogAndHandoff(t *testing.T, m processMocks, request entity.Request) *gomock.Call {
	t.Helper()

	logCall := expectProcessingLog(t, m, request)
	hookCall := expectStartValidationAnnounce(t, m, request.ID)
	buildCall := expectBuildPublish(t, m, request.ID)
	hookCall.After(logCall)
	buildCall.After(hookCall)
	return logCall
}

func expectProcessingLog(t *testing.T, m processMocks, request entity.Request) *gomock.Call {
	t.Helper()
	return expectStateLog(t, m, request, entity.RequestOutcomeReasonUnknown)
}

func expectStateLog(t *testing.T, m processMocks, request entity.Request, reason entity.RequestOutcomeReason) *gomock.Call {
	t.Helper()
	return m.materializer.EXPECT().PersistLog(
		gomock.Any(),
		m.store,
		requestlog.NewRequestStateLog(request, reason),
	).Return(nil)
}

func expectStartValidationAnnounce(t *testing.T, m processMocks, id string) *gomock.Call {
	t.Helper()

	return m.publisher.EXPECT().
		Publish(gomock.Any(), "stovepipe-hook", gomock.AssignableToTypeOf(entityqueue.Message{})).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			assert.Equal(t, id, msg.PartitionKey)
			assert.Equal(t, testQueue, msg.Tenant)
			event := &basehook.HookEvent{}
			require.NoError(t, basehook.Unmarshal(msg.Payload, event))
			assert.Equal(t, string(hookevent.TypeValidationRepositoryStarted), event.GetType())
			assert.Equal(t, hookevent.Source, event.GetSource())
			fields := event.GetPayload().GetFields()
			assert.Equal(t, id, fields["request_id"].GetStringValue())
			assert.Equal(t, testQueue, fields["queue"].GetStringValue())
			return nil
		})
}

func expectBuildPublish(t *testing.T, m processMocks, id string) *gomock.Call {
	t.Helper()

	return m.publisher.EXPECT().
		Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			assert.Equal(t, id, msg.ID)
			assert.Equal(t, id, msg.PartitionKey)
			assert.Equal(t, testQueue, msg.Tenant)
			assert.Equal(t, testQueue, msg.Metadata[entityqueue.MetadataKeyQueueName])
			buildReq := &stovepipemq.BuildRequest{}
			require.NoError(t, stovepipemq.Unmarshal(msg.Payload, buildReq))
			assert.Equal(t, id, buildReq.Id)
			return nil
		})
}

func TestDeriveBuildStrategy(t *testing.T) {
	const lastGreenURI = "git://repo/monorepo/main/green"

	tests := []struct {
		name         string
		queue        entity.Queue
		setup        func(m processMocks)
		wantStrategy entity.BuildStrategy
		wantBaseURI  string
		wantErr      bool
	}{
		{
			name:         "cold start uses full build without source control",
			queue:        entity.Queue{Name: testQueue},
			wantStrategy: entity.BuildStrategyFull,
		},
		{
			name:  "ancestor uses incremental build",
			queue: entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI},
			setup: func(m processMocks) {
				m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(true, nil)
			},
			wantStrategy: entity.BuildStrategyIncrementalSinceGreen,
			wantBaseURI:  lastGreenURI,
		},
		{
			name:  "history rewrite uses full build",
			queue: entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI},
			setup: func(m processMocks) {
				m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(false, nil)
			},
			wantStrategy: entity.BuildStrategyFull,
		},
		{
			name:  "unknown ancestry uses full build",
			queue: entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI},
			setup: func(m processMocks) {
				m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(false, sourcecontrol.ErrNotFound)
			},
			wantStrategy: entity.BuildStrategyFull,
		},
		{
			name:  "ancestry error fails",
			queue: entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI},
			setup: func(m processMocks) {
				m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(false, errors.New("source control unavailable"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			if tt.setup != nil {
				tt.setup(m)
			}

			if tt.queue.LastGreenURI != "" {
				m.sourceFactory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sourceControl, nil)
			}
			strategy, baseURI, err := c.deriveBuildStrategy(queueContext(tt.queue.Name), tt.queue, acceptedRequest(testID))

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStrategy, strategy)
			assert.Equal(t, tt.wantBaseURI, baseURI)
		})
	}
}

func TestDeriveBuildStrategyEmitsSourceControlMetrics(t *testing.T) {
	const lastGreenURI = "git://repo/monorepo/main/green"

	tests := []struct {
		name        string
		ancestryErr error
		metricName  string
		metricTags  string
	}{
		{
			name:        "unknown ancestry records fallback",
			ancestryErr: sourcecontrol.ErrNotFound,
			metricName:  "strategy_fallbacks",
			metricTags:  "reason=unknown_ancestry",
		},
		{
			name:        "source control failure records error",
			ancestryErr: errors.New("source control unavailable"),
			metricName:  "source_control_errors",
			metricTags:  "stage=ancestry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			scope := tally.NewTestScope("test", nil)
			c, m := newControllerWithScope(t, ctrl, scope)
			m.sourceFactory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sourceControl, nil)
			m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(false, tt.ancestryErr)

			_, _, err := c.deriveBuildStrategy(
				queueContext(testQueue),
				entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI},
				acceptedRequest(testID),
			)

			if sourcecontrol.IsNotFound(tt.ancestryErr) {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			counter, ok := scope.Snapshot().Counters()["test.process_controller.process."+tt.metricName+"+queue="+testQueue+","+tt.metricTags]
			require.True(t, ok)
			assert.Equal(t, int64(1), counter.Value())
		})
	}
}

func TestProcessEmitsAdmittedStrategyMetric(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("test", nil)
	c, m := newControllerWithScope(t, ctrl, scope)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
		Name:            testQueue,
		LatestRequestID: testID,
		Version:         1,
	}, nil)
	expectAdmit(t, m, testID)

	require.NoError(t, c.Process(queueContext(testQueue), delivery(t, ctrl, processPayload(t, testID))))

	counter, ok := scope.Snapshot().Counters()["test.process_controller.process.admitted+queue=monorepo/main,strategy=full"]
	require.True(t, ok)
	assert.Equal(t, int64(1), counter.Value())
}

func TestProcessEmitsSourceControlResolutionMetric(t *testing.T) {
	const lastGreenURI = "git://repo/monorepo/main/green"

	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("test", nil)
	c, m := newControllerWithScope(t, ctrl, scope)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
		Name:            testQueue,
		LatestRequestID: testID,
		LastGreenURI:    lastGreenURI,
		Version:         1,
	}, nil)
	m.sourceFactory.EXPECT().
		For(sourcecontrol.Config{QueueName: testQueue}).
		Return(nil, errors.New("source control unavailable"))

	require.Error(t, c.Process(queueContext(testQueue), delivery(t, ctrl, processPayload(t, testID))))

	counter, ok := scope.Snapshot().Counters()["test.process_controller.process.source_control_errors+queue=monorepo/main,stage=resolve"]
	require.True(t, ok)
	assert.Equal(t, int64(1), counter.Value())
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		payload []byte
		setup   func(m processMocks)
		// wantHoldMs, when non-zero, expects the delivery to be held for the
		// gate wait with exactly this delay. Cases without it fail on any Hold.
		wantHoldMs int64
		wantErr    bool
		wantRetry  bool
	}{
		{
			name: "superseded redelivery repairs its state log",
			setup: func(m processMocks) {
				request := entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
				expectStateLog(t, m, request, entity.RequestOutcomeReasonSupersededByNewerHead)
			},
		},
		{
			name:    "superseded redelivery fails when log persistence fails",
			wantErr: true,
			setup: func(m processMocks) {
				request := entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonSupersededByNewerHead),
				).Return(errors.New("db down"))
			},
		},
		{
			name: "succeeded is no-op",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateSucceeded, Version: 2,
				}, nil)
			},
		},
		{
			name: "failed is no-op",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateFailed, Version: 2,
				}, nil)
			},
		},
		{
			name: "cancelled is no-op",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateCancelled, Version: 2,
				}, nil)
			},
		},
		{
			name: "processing redelivery re-announces the start and republishes to build",
			setup: func(m processMocks) {
				request := entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
				}
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
				expectStartAnnounceAndBuildPublish(t, m, request)
			},
		},
		{
			name:    "processing redelivery stops before handoff when log persistence fails",
			wantErr: true,
			setup: func(m processMocks) {
				request := entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
				}
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown),
				).Return(errors.New("db down"))
			},
		},
		{
			name: "unknown state is acked without retry",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateUnknown, Version: 1,
				}, nil)
			},
		},
		{
			name: "latest accepted head is admitted",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				expectAdmit(t, m, testID)
			},
		},
		{
			name:    "start announce failure leaves the request admitted for a redelivery to re-announce",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				admitted, slotCall := expectAdmitWrites(t, m, testID)
				logCall := expectProcessingLog(t, m, admitted).After(slotCall)
				m.publisher.EXPECT().
					Publish(gomock.Any(), "stovepipe-hook", gomock.AssignableToTypeOf(entityqueue.Message{})).
					Return(errors.New("queue unavailable")).
					After(logCall)
			},
		},
		{
			name:    "build publish failure retains admitted request and counted slot",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				admitted, slotCall := expectAdmitWrites(t, m, testID)
				logCall := expectProcessingLog(t, m, admitted).After(slotCall)
				hookCall := expectStartValidationAnnounce(t, m, testID)
				m.publisher.EXPECT().
					Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
					Return(errors.New("queue unavailable")).
					After(hookCall)
				hookCall.After(logCall)
			},
		},
		{
			name:    "processing log failure retains admitted request and counted slot",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				admitted, slotCall := expectAdmitWrites(t, m, testID)
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestStateLog(admitted, entity.RequestOutcomeReasonUnknown),
				).Return(errors.New("db down")).After(slotCall)
			},
		},
		{
			name:    "source control failure admits nothing",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					LastGreenURI:    "git://repo/monorepo/main/green",
					Version:         1,
				}, nil)
				m.sourceFactory.EXPECT().
					For(sourcecontrol.Config{QueueName: testQueue}).
					Return(nil, errors.New("source control unavailable"))
			},
		},
		{
			name: "accepted with empty latest pointer awaits ingest stamp",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:    testQueue,
					Version: 1,
				}, nil)
			},
		},
		{
			name:       "latest accepted head holds when gate closed",
			wantHoldMs: 5000,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					LastGreenURI:    "git://repo/monorepo/main/green",
					Version:         1,
				}, nil)
			},
		},
		{
			name: "slot count retries a queue version mismatch without re-gating or coalescing",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				admitted, requestCall := expectRequestAdmit(t, m, testID)
				firstCount := m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 1,
				}, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				// A closed gate and a newer head on the reload change nothing: the admit is already durable.
				reload := m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: "9", InFlightCount: 1, Version: 2,
				}, nil)
				retryCount := m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: "9", InFlightCount: 2, Version: 2,
				}, int32(2), int32(3)).Return(nil)
				gomock.InOrder(requestCall, firstCount, reload, retryCount)
				expectProcessingLogAndHandoff(t, m, admitted).After(retryCount)
			},
		},
		{
			name:    "slot count failure leaves the request admitted and uncounted",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				_, requestCall := expectRequestAdmit(t, m, testID)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 1,
				}, int32(1), int32(2)).Return(errors.New("db down")).After(requestCall)
			},
		},
		{
			name: "mark processing retry preserves derived strategy after accepted reload",
			setup: func(m processMocks) {
				const lastGreenURI = "git://repo/monorepo/main/green"

				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, LastGreenURI: lastGreenURI, Version: 1,
				}, nil)
				m.sourceFactory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sourceControl, nil)
				m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(true, nil)

				firstAttempt := acceptedRequest(testID)
				firstAttempt.State = entity.RequestStateProcessing
				firstAttempt.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
				firstAttempt.BaseURI = lastGreenURI
				firstCall := m.reqStore.EXPECT().Update(gomock.Any(), firstAttempt, int32(1), int32(2)).Return(storage.ErrVersionMismatch)

				reloaded := acceptedRequest(testID)
				reloaded.Version = 2
				reloadCall := m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(reloaded, nil)

				retry := reloaded
				retry.State = entity.RequestStateProcessing
				retry.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
				retry.BaseURI = lastGreenURI
				retryCall := m.reqStore.EXPECT().Update(gomock.Any(), retry, int32(2), int32(3)).Return(nil)
				slotCall := m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, LastGreenURI: lastGreenURI, Version: 1,
				}, int32(1), int32(2)).Return(nil)
				gomock.InOrder(firstCall, reloadCall, retryCall, slotCall)
				loggedRequest := retry
				loggedRequest.Version = 3
				expectProcessingLogAndHandoff(t, m, loggedRequest).After(slotCall)
			},
		},
		{
			name: "mark processing lost race acks without counting a slot",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				updatedReq := acceptedRequest(testID)
				updatedReq.State = entity.RequestStateProcessing
				updatedReq.BuildStrategy = entity.BuildStrategyFull
				m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
				}, nil)
			},
		},
		{
			name:    "mark processing failure returns an error without counting a slot",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				updatedReq := acceptedRequest(testID)
				updatedReq.State = entity.RequestStateProcessing
				updatedReq.BuildStrategy = entity.BuildStrategyFull
				m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(errors.New("db down"))
			},
		},
		{
			name: "older accepted head is superseded",
			id:   testOlderID,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testOlderID).Return(acceptedRequest(testOlderID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				updated := acceptedRequest(testOlderID)
				updated.State = entity.RequestStateSuperseded
				updateCall := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
				loggedRequest := updated
				loggedRequest.Version = 2
				expectStateLog(t, m, loggedRequest, entity.RequestOutcomeReasonSupersededByNewerHead).After(updateCall)
			},
		},
		{
			name:    "superseded log failure retries the coalesce step",
			id:      testOlderID,
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testOlderID).Return(acceptedRequest(testOlderID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				updated := acceptedRequest(testOlderID)
				updated.State = entity.RequestStateSuperseded
				updateCall := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
				request := entity.Request{
					ID: testOlderID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonSupersededByNewerHead),
				).Return(errors.New("db down")).After(updateCall)
			},
		},
		{
			name: "supersede retries on version mismatch",
			id:   testOlderID,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testOlderID).Return(acceptedRequest(testOlderID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				updated := acceptedRequest(testOlderID)
				updated.State = entity.RequestStateSuperseded
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				reloaded := entity.Request{
					ID: testOlderID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}
				reloadCall := m.reqStore.EXPECT().Get(gomock.Any(), testOlderID).Return(reloaded, nil)
				expectStateLog(t, m, reloaded, entity.RequestOutcomeReasonSupersededByNewerHead).After(reloadCall)
			},
		},
		{
			name:      "malformed latest_request_id is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: "request.99",
					Version:         1,
				}, nil)
			},
		},
		{
			name:      "request not found is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)
			},
		},
		{
			name:      "queue not found is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{}, storage.ErrNotFound)
			},
		},
		{
			name:      "request storage error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, errors.New("db down"))
			},
		},
		{
			name:      "malformed payload is not retryable",
			payload:   []byte("not-json"),
			wantErr:   true,
			wantRetry: false,
			setup:     func(m processMocks) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			if tt.setup != nil {
				tt.setup(m)
			}

			id := testID
			if tt.id != "" {
				id = tt.id
			}
			payload := tt.payload
			if payload == nil {
				payload = processPayload(t, id)
			}

			d := delivery(t, ctrl, payload)
			if tt.wantHoldMs > 0 {
				d.EXPECT().Hold(tt.wantHoldMs)
			}

			err := c.Process(queueContext(testQueue), d)

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestHoldForBuildSlotRequiresPositiveDelay pins the config guard: a non-positive
// gate wait delay would redeliver immediately and hot-loop the gate check, so it is
// rejected instead of held.
func TestHoldForBuildSlotRequiresPositiveDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)

	d := consumermock.NewMockDelivery(ctrl)
	// No Hold expectation: the guard must reject before recording a hold.

	err := c.holdForBuildSlot(queueContext(testQueue), d, acceptedRequest(testID), 1, 0)

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}
