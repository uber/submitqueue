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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/base/change"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	hookext "github.com/uber/submitqueue/platform/extension/hook"
	hooknoop "github.com/uber/submitqueue/platform/extension/hook/noop"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/platform/pipeline"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
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

type startStorageFactory struct{ store orchstorage.Storage }

func (f startStorageFactory) For(orchstorage.Config) (orchstorage.Storage, error) {
	return f.store, nil
}

func TestStartDLQRecoversOriginalRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	landRequest := entity.LandRequest{ID: "123", Queue: "test-queue", Change: change.Change{URIs: []string{"uri/one", "uri/two"}}}
	var persisted entity.Request
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request entity.Request) error {
		persisted = request
		return nil
	})
	requestStore.EXPECT().Get(gomock.Any(), landRequest.ID).DoAndReturn(func(_ context.Context, _ string) (entity.Request, error) {
		return persisted, nil
	})
	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	publisher := queuemock.NewMockPublisher(ctrl)
	gomock.InOrder(
		publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil),
		publisher.EXPECT().Publish(gomock.Any(), "validate", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			id, err := sqmq.UnmarshalRequestID(topickey.TopicKeyValidate, msg.Payload)
			require.NoError(t, err)
			assert.Equal(t, entity.RequestID{ID: landRequest.ID, Queue: landRequest.Queue}, id)
			return nil
		}),
	)
	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue},
		{Key: topickey.TopicKeyValidate, Name: "validate", Queue: queue},
	})
	require.NoError(t, err)
	for _, stage := range Stages {
		if stage.Key != topickey.TopicKeyStart {
			continue
		}
		controller, err := stage.DLQ(Deps{Logger: zaptest.NewLogger(t).Sugar(), Scope: tally.NoopScope, Storage: startStorageFactory{store}}, pipeline.StageContext{Registry: registry, TopicKey: "start_dlq", ConsumerGroup: "orchestrator-dlq"})
		require.NoError(t, err)
		assert.Equal(t, consumer.TopicKey("start_dlq"), controller.TopicKey())
		payload, err := sqmq.Marshal(sqmq.StartFromLandRequest(landRequest))
		require.NoError(t, err)
		msg := entityqueue.NewMessage(landRequest.ID, payload, landRequest.Queue, nil)
		msg.Tenant = landRequest.Queue
		delivery := consumermock.NewMockDelivery(ctrl)
		delivery.EXPECT().Message().Return(msg)
		delivery.EXPECT().Attempt().Return(1)
		require.NoError(t, controller.Process(context.Background(), delivery))
		assert.Equal(t, landRequest.Change, persisted.Change)
		assert.Equal(t, entity.RequestStateStarted, persisted.State)
		return
	}
	require.Fail(t, "start stage must be registered")
}
