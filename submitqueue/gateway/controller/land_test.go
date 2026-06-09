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

package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	countermock "github.com/uber/submitqueue/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// newTestRegistry builds a single-entry TopicRegistry for TopicKeyStart wired
// to a mock Queue/Publisher and returns both the registry and the publisher
// mock so callers can set EXPECT() on the publisher.
func newTestRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	t.Helper()
	pub := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyStart, Name: "start", Queue: q},
	})
	require.NoError(t, err)
	return registry, pub
}

// newTestRegistryWithNoopPublisher returns a registry whose publisher silently
// accepts any Publish call. Use for tests that don't care about publish behavior.
func newTestRegistryWithNoopPublisher(t *testing.T, ctrl *gomock.Controller) consumer.TopicRegistry {
	t.Helper()
	registry, pub := newTestRegistry(t, ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return registry
}

// noopStorage returns a storage.Storage whose RequestLogStore.Insert
// succeeds silently for any entityqueue.
func noopStorage(ctrl *gomock.Controller) storage.Storage {
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()
	return store
}

// noopQueueConfigStore returns a mock queueconfig.Store that always reports
// the queue as configured.
func noopQueueConfigStore(ctrl *gomock.Controller) *qcmock.MockStore {
	s := qcmock.NewMockStore(ctrl)
	s.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.QueueConfig{}, nil).AnyTimes()
	return s
}

func TestNewLandController(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(1), nil)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", resp.Sqid)
}

func TestLand_ReturnsErrorOnCounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}

func TestLand_CounterDomainIncludesQueue(t *testing.T) {
	var capturedDomain string

	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, domain string) (int64, error) {
			capturedDomain = domain
			return 1, nil
		},
	)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "my-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	_, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "request/my-queue", capturedDomain)
}

func TestLand_ReturnsErrorOnEmptyQueue(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsErrorOnEmptyChangeUri(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsErrorOnNilChange(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: nil,
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsUnrecognizedQueueWhenStoreReportsNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	qcs := qcmock.NewMockStore(ctrl)
	qcs.EXPECT().Get(gomock.Any(), "missing-queue").Return(entity.QueueConfig{}, queueconfig.ErrNotFound)

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), qcs, newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "missing-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsUnrecognizedQueue(err))
	assert.True(t, errs.IsUserError(err))
	assert.False(t, errs.IsRetryable(err))

	var typed *UnrecognizedQueueError
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, "missing-queue", typed.Queue)
}

func TestLand_PropagatesQueueConfigStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	qcs := qcmock.NewMockStore(ctrl)
	qcs.EXPECT().Get(gomock.Any(), "test-queue").Return(entity.QueueConfig{}, fmt.Errorf("config backend down"))

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), qcs, newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.False(t, IsUnrecognizedQueue(err))
	assert.False(t, IsInvalidRequest(err))
}

func TestLand_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage entityqueue.Message

	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(123), nil)

	registry, publisher := newTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			publishedTopic = topic
			publishedMessage = msg
			return nil
		},
	)

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), registry)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "test-queue",
		Change:   &pb.Change{Uris: []string{"github://uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98"}},
		Strategy: pb.Strategy_REBASE,
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", resp.Sqid)

	// Verify message was published to the topic registered under TopicKeyStart
	assert.Equal(t, "start", publishedTopic)
	assert.Equal(t, "test-queue/123", publishedMessage.ID)
	assert.Equal(t, "test-queue", publishedMessage.PartitionKey)

	// Verify payload can be deserialized
	deserializedReq, err := entity.LandRequestFromBytes(publishedMessage.Payload)
	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", deserializedReq.ID)
	assert.Equal(t, "test-queue", deserializedReq.Queue)
	assert.Equal(t, []string{"github://uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98"}, deserializedReq.Change.URIs)
	assert.Equal(t, entity.RequestLandStrategyRebase, deserializedReq.LandStrategy)
}

func TestLand_ContinuesWhenPublishFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(999), nil)

	registry, publisher := newTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), registry)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/service/pull/1/c3a4d5e6f7890123456789abcdef0123456789ab"}},
	}
	_, err := controller.Land(ctx, req)

	// Should fail if publish fails
	require.Error(t, err)
}
