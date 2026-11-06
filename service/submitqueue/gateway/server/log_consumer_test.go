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

package main

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
	consumergatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/platform/lifecycle"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/entity"
	requestcore "github.com/uber/submitqueue/submitqueue/gateway/core/request"
	gwstorage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
	gwstoragemock "github.com/uber/submitqueue/submitqueue/gateway/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestLogConsumers_RetainFailedRecovery(t *testing.T) {
	for _, tt := range []struct {
		name       string
		topic      string
		malformed  bool
		wrongQueue bool
	}{
		{name: "primary rejects unclassified failure", topic: "log"},
		{name: "final retries unclassified failure", topic: "log_dlq"},
		{name: "final retains malformed payload", topic: "log_dlq", malformed: true},
		{name: "final retains payload identity mismatch", topic: "log_dlq", wrongQueue: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			primary := make(chan extqueue.Delivery, 1)
			final := make(chan extqueue.Delivery, 1)
			subscriber := queuemock.NewMockSubscriber(ctrl)
			subscriber.EXPECT().Subscribe(gomock.Any(), "log", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
				assert.Equal(t, "gateway-log", config.ConsumerGroup)
				assert.True(t, config.DLQ.Enabled)
				assert.Equal(t, "_dlq", config.DLQ.TopicSuffix)
				assert.Positive(t, config.Retry.MaxAttempts)
				return primary, nil
			})
			subscriber.EXPECT().Subscribe(gomock.Any(), "log_dlq", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
				assert.Equal(t, "gateway-log-dlq", config.ConsumerGroup)
				assert.False(t, config.DLQ.Enabled)
				assert.Zero(t, config.Retry.MaxAttempts)
				assert.Positive(t, config.Retry.MaxBackoffMs)
				return final, nil
			})
			queue := queuemock.NewMockQueue(ctrl)
			queue.EXPECT().Subscriber().Return(subscriber).Times(2)
			registry, err := newGatewayTopicRegistry(queue, "worker")
			require.NoError(t, err)
			factory := gwstoragemock.NewMockFactory(ctrl)
			if !tt.malformed && !tt.wrongQueue {
				factory.EXPECT().For(gwstorage.Config{QueueName: "queue-a"}).Return(nil, errors.New("storage unavailable"))
			}
			consumers, err := newLogConsumers(zap.NewNop().Sugar(), tally.NoopScope, registry, requestcore.NewMaterializer(factory), consumergatenoop.New())
			require.NoError(t, err)
			require.NoError(t, consumers.Start(context.Background()))
			t.Cleanup(func() { require.NoError(t, consumers.Stop(context.Background())) })

			log := entity.NewRequestStatusLog("queue-a", "1", entity.RequestStatusLanded, 3, "", nil)
			payload, err := sqmq.Marshal(sqmq.LogFromEntity(log))
			require.NoError(t, err)
			if tt.malformed {
				payload = []byte("not JSON")
			}
			msg := entityqueue.NewMessage("terminal-log", payload, "queue-a", nil)
			msg.Tenant = "queue-a"
			if tt.wrongQueue {
				msg.Tenant = "queue-b"
			}
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(100).AnyTimes()
			delivery.EXPECT().Metadata().Return(nil).AnyTimes()
			settled := make(chan struct{})
			settle := func(context.Context, failure.Failure) error { close(settled); return nil }
			if tt.topic == "log" {
				delivery.EXPECT().Reject(gomock.Any(), gomock.Any()).DoAndReturn(settle)
				primary <- delivery
			} else {
				delivery.EXPECT().Nack(gomock.Any(), gomock.Any()).DoAndReturn(settle)
				final <- delivery
			}
			<-settled
		})
	}
}

type lifecycleTestConsumer struct {
	consumer.Consumer
	name     string
	calls    *[]string
	startErr error
	stopErr  error
}

func (c lifecycleTestConsumer) Start(context.Context) error {
	*c.calls = append(*c.calls, "start:"+c.name)
	return c.startErr
}

func (c lifecycleTestConsumer) Stop(timeoutMs int64) error {
	*c.calls = append(*c.calls, "stop:"+c.name)
	return c.stopErr
}

func TestServeGateway_ConsumerCleanup(t *testing.T) {
	startErr := errors.New("subscribe unavailable")
	stopErr := errors.New("stop unavailable")
	for _, tt := range []struct {
		name        string
		primaryErr  error
		recoveryErr error
		stopErr     error
		cancelled   bool
		want        []string
	}{
		{name: "primary fails", primaryErr: startErr, want: []string{"start:primary"}},
		{name: "recovery fails and rollback fails", recoveryErr: startErr, stopErr: stopErr, want: []string{"start:primary", "start:recovery", "stop:primary"}},
		{name: "listener fails and both stop despite cleanup failure", stopErr: stopErr, want: []string{"start:primary", "start:recovery", "stop:recovery", "stop:primary"}},
		{name: "signal drains both", cancelled: true, want: []string{"start:primary", "start:recovery", "stop:recovery", "stop:primary"}},
		{name: "signal does not mask shutdown failure", cancelled: true, stopErr: stopErr, want: []string{"start:primary", "start:recovery", "stop:recovery", "stop:primary"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			group := lifecycle.NewGroup(
				logConsumerLifecycle{Consumer: lifecycleTestConsumer{name: "primary", calls: &calls, startErr: tt.primaryErr, stopErr: tt.stopErr}},
				logConsumerLifecycle{Consumer: lifecycleTestConsumer{name: "recovery", calls: &calls, startErr: tt.recoveryErr, stopErr: tt.stopErr}},
			)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			t.Setenv("PORT", "invalid-port")
			if tt.cancelled {
				t.Setenv("PORT", "127.0.0.1:0")
				cancel()
			}
			err := serveGateway(ctx, cancel, grpc.NewServer(), group)
			require.Error(t, err)
			if tt.primaryErr != nil || tt.recoveryErr != nil {
				assert.ErrorIs(t, err, startErr)
			}
			if tt.stopErr != nil {
				assert.ErrorIs(t, err, stopErr)
			}
			assert.Equal(t, tt.cancelled && tt.stopErr == nil, errors.Is(err, context.Canceled))
			assert.Equal(t, tt.want, calls)
		})
	}
}
