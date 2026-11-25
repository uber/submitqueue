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

package periodicmetrics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestTaskReusesDeliveryAtMinuteCadence(t *testing.T) {
	for _, tt := range []struct {
		name           string
		elapsed        time.Duration
		observationErr error
		wantDelay      time.Duration
	}{
		{name: "success", wantDelay: time.Minute},
		{name: "slow observation", elapsed: 7 * time.Second, wantDelay: 53 * time.Second},
		{name: "failed observation", observationErr: errors.New("unavailable"), wantDelay: time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			observerController, mocks, scope := newTestController(t, ctrl)
			core, logs := observer.New(zap.ErrorLevel)
			worker := NewTask(zap.New(core).Sugar(), observerController, stovepipemq.TopicKeyPeriodicMetrics, "metrics-workers").(*task)
			now := time.UnixMilli(_testNowMs)
			worker.now = func() time.Time { return now }
			for range 2 {
				mocks.stores.EXPECT().For(gomock.Any()).Return(mocks.store, nil)
				mocks.store.EXPECT().GetQueueStore().Return(mocks.queueStore)
				mocks.queueStore.EXPECT().Get(gomock.Any(), _testQueue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
					now = now.Add(tt.elapsed)
					return entity.Queue{}, tt.observationErr
				})
				delivery := metricsDelivery(t, ctrl)
				delivery.EXPECT().Hold(tt.wantDelay.Milliseconds())
				require.NoError(t, worker.Process(context.Background(), delivery))
			}
			if tt.observationErr != nil {
				assert.Equal(t, 2, logs.Len())
			} else {
				assert.EqualValues(t, 2, scope.Snapshot().Counters()[_counterPrefix+"observed"+_queueTagSuffix].Value())
			}
		})
	}
}

func TestTaskObservationTimeoutKeepsRecurringTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		observerController, mocks, _ := newTestController(t, ctrl)
		mocks.stores.EXPECT().For(gomock.Any()).Return(mocks.store, nil)
		mocks.store.EXPECT().GetQueueStore().Return(mocks.queueStore)
		mocks.queueStore.EXPECT().Get(gomock.Any(), _testQueue).DoAndReturn(func(ctx context.Context, _ string) (entity.Queue, error) {
			<-ctx.Done()
			assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			return entity.Queue{}, ctx.Err()
		})
		delivery := metricsDelivery(t, ctrl)
		delivery.EXPECT().Hold(int64(15_000))
		worker := NewTask(zap.NewNop().Sugar(), observerController, stovepipemq.TopicKeyPeriodicMetrics, "metrics-workers")
		started := time.Now()
		require.NoError(t, worker.Process(context.Background(), delivery))
		assert.Equal(t, 45*time.Second, time.Since(started))
	})
}

func TestTaskShutdownPreservesDelivery(t *testing.T) {
	ctrl := gomock.NewController(t)
	observerController, mocks, _ := newTestController(t, ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mocks.stores.EXPECT().For(gomock.Any()).Return(mocks.store, nil)
	mocks.store.EXPECT().GetQueueStore().Return(mocks.queueStore)
	mocks.queueStore.EXPECT().Get(gomock.Any(), _testQueue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
		cancel()
		return entity.Queue{}, ctx.Err()
	})
	worker := NewTask(zap.NewNop().Sugar(), observerController, stovepipemq.TopicKeyPeriodicMetrics, "metrics-workers")
	require.ErrorIs(t, worker.Process(ctx, metricsDelivery(t, ctrl)), context.Canceled)
}

func TestTaskRejectsInvalidPayload(t *testing.T) {
	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{name: "invalid JSON", payload: []byte("{")},
		{name: "missing queue", payload: []byte(`{}`)},
		{name: "different tenant", payload: []byte(`{"queue_name":"other"}`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			observerController, _, _ := newTestController(t, ctrl)
			delivery := consumermock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(entityqueue.Message{Tenant: _testQueue, Payload: tt.payload})
			worker := NewTask(zap.NewNop().Sugar(), observerController, stovepipemq.TopicKeyPeriodicMetrics, "metrics-workers")
			require.Error(t, worker.Process(context.Background(), delivery))
		})
	}
}

func TestScheduleSharesTaskIdentityAcrossReplicas(t *testing.T) {
	ctrl := gomock.NewController(t)
	queue := queuemock.NewMockQueue(ctrl)
	publisher := queuemock.NewMockPublisher(ctrl)
	queue.EXPECT().Publisher().Return(publisher).Times(3)
	var messages []entityqueue.Message
	publisher.EXPECT().Publish(gomock.Any(), "periodicmetrics", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, message entityqueue.Message) error {
		messages = append(messages, message)
		return nil
	}).Times(3)
	longQueue := strings.Repeat("q", 255)
	for _, name := range []string{longQueue, longQueue, _testQueue} {
		registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: stovepipemq.TopicKeyPeriodicMetrics, Name: "periodicmetrics", Queue: queue}})
		require.NoError(t, err)
		require.NoError(t, Schedule(context.Background(), registry, stovepipemq.TopicKeyPeriodicMetrics, name))
	}
	assert.Equal(t, messages[0].ID, messages[1].ID)
	assert.Equal(t, messages[0].PartitionKey, messages[1].PartitionKey)
	assert.Equal(t, messages[0].Tenant, messages[1].Tenant)
	assert.NotEqual(t, messages[0].Tenant, messages[2].Tenant)
	for _, message := range messages {
		assert.NotEmpty(t, message.ID)
		assert.LessOrEqual(t, len(message.ID), 255)
		request := &stovepipemq.PeriodicMetrics{}
		require.NoError(t, stovepipemq.Unmarshal(message.Payload, request))
		assert.Equal(t, message.Tenant, message.PartitionKey)
		assert.Equal(t, message.Tenant, request.GetQueueName())
	}
}

func TestSchedulePublicationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	queue := queuemock.NewMockQueue(ctrl)
	publisher := queuemock.NewMockPublisher(ctrl)
	queue.EXPECT().Publisher().Return(publisher)
	publishErr := errors.New("unavailable")
	publisher.EXPECT().Publish(gomock.Any(), "periodicmetrics", gomock.Any()).Return(publishErr)
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{Key: stovepipemq.TopicKeyPeriodicMetrics, Name: "periodicmetrics", Queue: queue}})
	require.NoError(t, err)
	require.ErrorIs(t, Schedule(context.Background(), registry, stovepipemq.TopicKeyPeriodicMetrics, _testQueue), publishErr)
	require.Error(t, Schedule(context.Background(), registry, stovepipemq.TopicKeyPeriodicMetrics, ""))
}

func metricsDelivery(t *testing.T, ctrl *gomock.Controller) *consumermock.MockDelivery {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.PeriodicMetrics{QueueName: _testQueue})
	require.NoError(t, err)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(entityqueue.Message{Tenant: _testQueue, PartitionKey: _testQueue, Payload: payload})
	return delivery
}
