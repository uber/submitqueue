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
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
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
	testID      = "request/monorepo/main/7"
	testOlderID = "request/monorepo/main/3"
	testURI     = "git://repo/monorepo/main/abc123"
)

// rescheduledMsg matches a gate-wait re-publish: same partition, but a fresh message id —
// re-publishing under the in-flight delivery's id would be silently deduped against its
// still-present message-store row and lost on ack.
func rescheduledMsg(msg entityqueue.Message) bool {
	// Fresh non-empty id, same queue.
	return msg.ID != testID && msg.ID != "" && msg.PartitionKey == testQueue
}

type processMocks struct {
	reqStore      *storagemock.MockRequestStore
	queueStore    *storagemock.MockQueueStore
	sourceFactory *sourcecontrolmock.MockFactory
	sourceControl *sourcecontrolmock.MockSourceControl
	publisher     *mqmock.MockPublisher
}

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
		publisher:     mqmock.NewMockPublisher(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyProcess, Name: "process", Queue: queue},
		{Key: stovepipemq.TopicKeyBuild, Name: "build", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(
		zap.NewNop().Sugar(),
		scope,
		store,
		queueconfigdefault.NewStore(),
		m.sourceFactory,
		registry,
		stovepipemq.TopicKeyProcess,
		"stovepipe-process",
	)
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := mqmock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, testQueue, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func processPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id})
	require.NoError(t, err)
	return b
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

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
		ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
	}, nil)

	err := c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID)))

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func expectAdmit(t *testing.T, m processMocks, id string) {
	t.Helper()

	updatedQueue := entity.Queue{
		Name:            testQueue,
		LatestRequestID: id,
		InFlightCount:   1,
		Version:         1,
	}
	m.queueStore.EXPECT().Update(gomock.Any(), updatedQueue, int32(1), int32(2)).Return(nil)

	updatedReq := acceptedRequest(id)
	updatedReq.State = entity.RequestStateProcessing
	updatedReq.BuildStrategy = entity.BuildStrategyFull
	m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(nil)

	expectBuildPublish(t, m, id)
}

func expectBuildPublish(t *testing.T, m processMocks, id string) {
	t.Helper()

	m.publisher.EXPECT().
		Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			assert.Equal(t, id, msg.ID)
			assert.Equal(t, id, msg.PartitionKey)
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

			var sc sourcecontrol.SourceControl
			if tt.queue.LastGreenURI != "" {
				sc = m.sourceControl
			}
			strategy, baseURI, err := c.deriveBuildStrategy(context.Background(), sc, tt.queue, acceptedRequest(testID))

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
			m.sourceControl.EXPECT().IsAncestor(gomock.Any(), lastGreenURI, testURI).Return(false, tt.ancestryErr)

			_, _, err := c.deriveBuildStrategy(
				context.Background(),
				m.sourceControl,
				entity.Queue{Name: testQueue, LastGreenURI: lastGreenURI},
				acceptedRequest(testID),
			)

			if sourcecontrol.IsNotFound(tt.ancestryErr) {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			counter, ok := scope.Snapshot().Counters()["test.process_controller.process."+tt.metricName+"+"+tt.metricTags]
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

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID))))

	counter, ok := scope.Snapshot().Counters()["test.process_controller.process.admitted+strategy=full"]
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

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID))))

	counter, ok := scope.Snapshot().Counters()["test.process_controller.process.source_control_errors+stage=resolve"]
	require.True(t, ok)
	assert.Equal(t, int64(1), counter.Value())
}

func TestProcessRederivesStrategyAfterQueueReload(t *testing.T) {
	const (
		initialLastGreen  = "git://repo/monorepo/main/green-old"
		reloadedLastGreen = "git://repo/monorepo/main/green-new"
	)

	tests := []struct {
		name             string
		initialLastGreen string
	}{
		{
			name:             "rederives against changed baseline",
			initialLastGreen: initialLastGreen,
		},
		{
			name: "resolves source control after baseline appears",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			request := acceptedRequest(testID)

			initialQueue := entity.Queue{
				Name:            testQueue,
				LatestRequestID: testID,
				LastGreenURI:    tt.initialLastGreen,
				Version:         1,
			}
			reloadedQueue := entity.Queue{
				Name:            testQueue,
				LatestRequestID: testID,
				LastGreenURI:    reloadedLastGreen,
				Version:         2,
			}
			initialClaim := initialQueue
			initialClaim.InFlightCount = 1
			claimedQueue := reloadedQueue
			claimedQueue.InFlightCount = 1

			updatedRequest := request
			updatedRequest.State = entity.RequestStateProcessing
			updatedRequest.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
			updatedRequest.BaseURI = reloadedLastGreen

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(initialQueue, nil)
			m.sourceFactory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sourceControl, nil)
			if tt.initialLastGreen != "" {
				m.sourceControl.EXPECT().IsAncestor(gomock.Any(), tt.initialLastGreen, testURI).Return(true, nil)
			}
			m.queueStore.EXPECT().Update(gomock.Any(), initialClaim, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(reloadedQueue, nil)
			m.sourceControl.EXPECT().IsAncestor(gomock.Any(), reloadedLastGreen, testURI).Return(true, nil)
			m.queueStore.EXPECT().Update(gomock.Any(), claimedQueue, int32(2), int32(3)).Return(nil)
			m.reqStore.EXPECT().Update(gomock.Any(), updatedRequest, int32(1), int32(2)).Return(nil)
			expectBuildPublish(t, m, testID)

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, processPayload(t, testID))))
		})
	}
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		payload   []byte
		setup     func(m processMocks)
		wantErr   bool
		wantRetry bool
	}{
		{
			name: "superseded is no-op",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}, nil)
			},
		},
		{
			name: "processing republishes to build",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
				}, nil)
				expectBuildPublish(t, m, testID)
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
			name:    "build publish failure retains admitted request and claimed slot",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				updatedQueue := entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					Version:         1,
				}
				m.queueStore.EXPECT().Update(gomock.Any(), updatedQueue, int32(1), int32(2)).Return(nil)
				updatedRequest := acceptedRequest(testID)
				updatedRequest.State = entity.RequestStateProcessing
				updatedRequest.BuildStrategy = entity.BuildStrategyFull
				m.reqStore.EXPECT().Update(gomock.Any(), updatedRequest, int32(1), int32(2)).Return(nil)
				m.publisher.EXPECT().
					Publish(gomock.Any(), "build", gomock.AssignableToTypeOf(entityqueue.Message{})).
					Return(errors.New("queue unavailable"))
			},
		},
		{
			name:    "source control failure does not claim a build slot",
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
			name: "latest accepted head reschedules when gate closed",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					LastGreenURI:    "git://repo/monorepo/main/green",
					Version:         1,
				}, nil)
				m.publisher.EXPECT().
					PublishAfter(gomock.Any(), "process", gomock.Cond(rescheduledMsg), int64(5000)).
					Return(nil)
			},
		},
		{
			name:      "gate reschedule publish error surfaces",
			wantErr:   true,
			wantRetry: false,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					Version:         1,
				}, nil)
				m.publisher.EXPECT().
					PublishAfter(gomock.Any(), "process", gomock.Cond(rescheduledMsg), int64(5000)).
					Return(errors.New("queue down"))
			},
		},
		{
			name: "gate closed after slot claim race reschedules",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					Version:         1,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					Version:         1,
				}, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name:            testQueue,
					LatestRequestID: testID,
					InFlightCount:   1,
					Version:         2,
				}, nil)
				m.publisher.EXPECT().
					PublishAfter(gomock.Any(), "process", gomock.Cond(rescheduledMsg), int64(5000)).
					Return(nil)
			},
		},
		{
			name: "claim slot retries on queue version mismatch then admits",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				// First claim CAS loses to a concurrent writer.
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 1,
				}, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				// Reload: still latest, slot still free (version advanced by an unrelated field).
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 2,
				}, nil)
				// Retry claim succeeds, then admit.
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 2,
				}, int32(2), int32(3)).Return(nil)
				updatedReq := acceptedRequest(testID)
				updatedReq.State = entity.RequestStateProcessing
				updatedReq.BuildStrategy = entity.BuildStrategyFull
				m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(nil)
				expectBuildPublish(t, m, testID)
			},
		},
		{
			name: "reload after claim mismatch supersedes a now-stale head",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 1,
				}, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				// Reload: ingest stamped a newer head — our head is no longer latest.
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: "request/monorepo/main/9", Version: 2,
				}, nil)
				superseded := acceptedRequest(testID)
				superseded.State = entity.RequestStateSuperseded
				m.reqStore.EXPECT().Update(gomock.Any(), superseded, int32(1), int32(2)).Return(nil)
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
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, LastGreenURI: lastGreenURI, Version: 1,
				}, int32(1), int32(2)).Return(nil)

				firstAttempt := acceptedRequest(testID)
				firstAttempt.State = entity.RequestStateProcessing
				firstAttempt.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
				firstAttempt.BaseURI = lastGreenURI
				m.reqStore.EXPECT().Update(gomock.Any(), firstAttempt, int32(1), int32(2)).Return(storage.ErrVersionMismatch)

				reloaded := acceptedRequest(testID)
				reloaded.Version = 2
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(reloaded, nil)

				retry := reloaded
				retry.State = entity.RequestStateProcessing
				retry.BuildStrategy = entity.BuildStrategyIncrementalSinceGreen
				retry.BaseURI = lastGreenURI
				m.reqStore.EXPECT().Update(gomock.Any(), retry, int32(2), int32(3)).Return(nil)
				expectBuildPublish(t, m, testID)
			},
		},
		{
			name: "mark processing lost race releases slot and skips admit",
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				// Claim succeeds.
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 1,
				}, int32(1), int32(2)).Return(nil)
				// markProcessing CAS loses, reload shows a concurrent writer already advanced it.
				updatedReq := acceptedRequest(testID)
				updatedReq.State = entity.RequestStateProcessing
				updatedReq.BuildStrategy = entity.BuildStrategyFull
				m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{
					ID: testID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2,
				}, nil)
				// Compensating decrement of the spurious slot.
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 2,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 0, Version: 2,
				}, int32(2), int32(3)).Return(nil)
			},
		},
		{
			name:    "mark processing error releases slot and returns error",
			wantErr: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(acceptedRequest(testID), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, Version: 1,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 1,
				}, int32(1), int32(2)).Return(nil)
				updatedReq := acceptedRequest(testID)
				updatedReq.State = entity.RequestStateProcessing
				updatedReq.BuildStrategy = entity.BuildStrategyFull
				m.reqStore.EXPECT().Update(gomock.Any(), updatedReq, int32(1), int32(2)).Return(errors.New("db down"))
				// Best-effort compensating decrement before the error propagates.
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 1, Version: 2,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, LatestRequestID: testID, InFlightCount: 0, Version: 2,
				}, int32(2), int32(3)).Return(nil)
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
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
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
				m.reqStore.EXPECT().Get(gomock.Any(), testOlderID).Return(entity.Request{
					ID: testOlderID, Queue: testQueue, State: entity.RequestStateSuperseded, Version: 2,
				}, nil)
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
					LatestRequestID: "request/other-queue/99",
					Version:         1,
				}, nil)
			},
		},
		{
			name:      "request not found is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m processMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)
			},
		},
		{
			name:      "queue not found is retryable",
			wantErr:   true,
			wantRetry: true,
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

			err := c.Process(context.Background(), delivery(t, ctrl, payload))

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRescheduleProcessRequiresPositiveDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)

	err := c.rescheduleProcess(context.Background(), acceptedRequest(testID), 1, 0)

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}
