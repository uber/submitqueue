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

package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/base/failure"
	queue "github.com/uber/submitqueue/platform/base/messagequeue"
	hookext "github.com/uber/submitqueue/platform/extension/hook"
	hooknoop "github.com/uber/submitqueue/platform/extension/hook/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/platform/pipeline"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	orchstoragemock "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mock"
	"go.uber.org/mock/gomock"

	"go.uber.org/zap/zaptest"
)

type noopHooks struct{}

func (noopHooks) For(*basehook.HookEvent) []hookext.Hook {
	return []hookext.Hook{hooknoop.New()}
}

func hookStage(t *testing.T) pipeline.Stage[Deps] {
	t.Helper()
	for _, s := range Stages {
		if s.Key == basehook.TopicKeyHook {
			return s
		}
	}
	require.FailNow(t, "no stage is registered for the hook topic key")
	return pipeline.Stage[Deps]{}
}

func TestHookStage(t *testing.T) {
	deps := func(t *testing.T) Deps {
		return Deps{
			Logger: zaptest.NewLogger(t).Sugar(),
			Scope:  tally.NoopScope,
			Hooks:  noopHooks{},
		}
	}
	stageContext := func(key string) pipeline.StageContext {
		return pipeline.StageContext{
			TopicKey:      basehook.TopicKey(key),
			ConsumerGroup: "orchestrator",
		}
	}

	t.Run("the hook controller subscribes to the key the engine assigns", func(t *testing.T) {
		controller, err := hookStage(t).New(deps(t), stageContext("hook"))
		require.NoError(t, err)
		assert.Equal(t, basehook.TopicKeyHook, controller.TopicKey())
	})

	t.Run("the dead-letter controller subscribes to the key the engine derives", func(t *testing.T) {
		controller, err := hookStage(t).DLQ(deps(t), stageContext("hook_dlq"))
		require.NoError(t, err)
		assert.Equal(t, basehook.TopicKey("hook_dlq"), controller.TopicKey())
	})

	t.Run("a host that wires no hooks resolver fails to construct", func(t *testing.T) {
		d := deps(t)
		d.Hooks = nil
		_, err := hookStage(t).New(d, stageContext("hook"))
		require.Error(t, err)
	})
}

type concludeStorageFactory struct{ store orchstorage.Storage }

func (f concludeStorageFactory) For(orchstorage.Config) (orchstorage.Storage, error) {
	return f.store, nil
}

func TestConcludeFinalBinding(t *testing.T) {
	cases := []struct {
		name          string
		state         entity.BatchState
		payload       []byte
		tenant        string
		metadataQueue string
		batchQueue    string
		batchError    error
		recordError   error
		missingMember bool
		wantAck       bool
	}{
		{name: "succeeded replays member log", state: entity.BatchStateSucceeded, wantAck: true},
		{name: "missing batch retained", batchError: storage.ErrNotFound},
		{name: "missing member retained", state: entity.BatchStateFailed, missingMember: true},
		{name: "membership repair failure retained", state: entity.BatchStateCancelled, recordError: errors.New("membership unavailable")},
		{name: "malformed payload retained", payload: []byte("invalid")},
		{name: "empty batch ID retained", payload: []byte(`{"queue":"q"}`)},
		{name: "payload tenant mismatch retained", tenant: "other"},
		{name: "invalid envelope retained", metadataQueue: "other"},
		{name: "authoritative queue mismatch retained", batchQueue: "other"},
		{name: "unknown outcome retained", state: entity.BatchStateUnknown},
		{name: "creating retained", state: entity.BatchStateCreating},
		{name: "created retained", state: entity.BatchStateCreated},
		{name: "speculating retained", state: entity.BatchStateSpeculating},
		{name: "landing retained", state: entity.BatchStateLanding},
		{name: "cancelling retained", state: entity.BatchStateCancelling},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var stage pipeline.Stage[Deps]
			for _, candidate := range Stages {
				if candidate.Key == topickey.TopicKeyConclude {
					stage = candidate
				}
			}
			require.NotNil(t, stage.DLQ)
			batches := storagemock.NewMockBatchStore(ctrl)
			memberships := storagemock.NewMockQueueBatchStateStore(ctrl)
			requests := storagemock.NewMockRequestStore(ctrl)
			batchQueue := tt.batchQueue
			if batchQueue == "" {
				batchQueue = "q"
			}
			batch := entity.Batch{ID: "9", Queue: batchQueue, State: tt.state, Contains: []string{"1"}, Version: 2}
			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batches).AnyTimes()
			store.EXPECT().GetQueueBatchStateStore().Return(memberships).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
			if tt.payload == nil && tt.tenant == "" && tt.metadataQueue == "" {
				batches.EXPECT().Get(gomock.Any(), "9").Return(batch, tt.batchError)
				if tt.batchError == nil && tt.batchQueue == "" && tt.state.IsTerminal() {
					memberships.EXPECT().Put(gomock.Any(), entity.QueueBatchState{Queue: "q", State: tt.state, BatchID: "9"}).Return(tt.recordError)
					if tt.missingMember {
						requests.EXPECT().Get(gomock.Any(), "1").Return(entity.Request{}, storage.ErrNotFound)
					}
					if tt.wantAck {
						requests.EXPECT().Get(gomock.Any(), "1").Return(entity.Request{ID: "1", Queue: "q", State: entity.RequestStateLanded, Version: 3}, nil)
					}
				}
			}
			deliveries := make(chan extqueue.Delivery, 1)
			subscriber := queuemock.NewMockSubscriber(ctrl)
			subscriber.EXPECT().Subscribe(gomock.Any(), "conclude", gomock.Any()).Return(make(chan extqueue.Delivery), nil)
			subscriber.EXPECT().Subscribe(gomock.Any(), "conclude_dlq", gomock.Any()).DoAndReturn(
				func(_ context.Context, _ string, config extqueue.SubscriptionConfig) (<-chan extqueue.Delivery, error) {
					assert.Zero(t, config.Retry.MaxAttempts)
					assert.False(t, config.DLQ.Enabled)
					return deliveries, nil
				})
			backend := queuemock.NewMockQueue(ctrl)
			backend.EXPECT().Subscriber().Return(subscriber).Times(2)
			if tt.wantAck {
				publisher := queuemock.NewMockPublisher(ctrl)
				backend.EXPECT().Publisher().Return(publisher)
				publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).DoAndReturn(
					func(_ context.Context, _ string, message queue.Message) error {
						log, err := sqmq.UnmarshalRequestLog(message.Payload)
						require.NoError(t, err)
						assert.Equal(t, entity.RequestStatusLanded, log.Status)
						return nil
					})
			}
			deps := Deps{Logger: zaptest.NewLogger(t).Sugar(), Scope: tally.NoopScope, Storage: concludeStorageFactory{store}}
			component, err := pipeline.Construct(deps.Logger, deps.Scope, backend, "test", deps, []pipeline.Stage[Deps]{stage}, pipeline.PublishOnly(pipeline.PublishOnlyTopic{Key: topickey.TopicKeyLog, Name: "log"}))
			require.NoError(t, err)
			require.NoError(t, component.Start(context.Background()))
			t.Cleanup(func() { require.NoError(t, component.Stop(context.Background())) })
			payload := tt.payload
			if payload == nil {
				payload, err = sqmq.MarshalID(sqmq.TopicKeyConclude, "9", "q")
				require.NoError(t, err)
			}
			message := queue.NewMessage("conclude", payload, "q", map[string]string{queue.MetadataKeyQueueName: tt.metadataQueue})
			message.Tenant = tt.tenant
			if message.Tenant == "" {
				message.Tenant = "q"
			}
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(message).AnyTimes()
			delivery.EXPECT().Metadata().Return(nil).AnyTimes()
			delivery.EXPECT().Attempt().Return(20).AnyTimes()
			settled := make(chan struct{})
			if tt.wantAck {
				delivery.EXPECT().Ack(gomock.Any()).DoAndReturn(func(context.Context) error { close(settled); return nil })
			} else {
				delivery.EXPECT().Nack(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, failure.Failure) error { close(settled); return nil })
			}
			deliveries <- delivery
			<-settled
		})
	}
}
