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
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	countermock "github.com/uber/submitqueue/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// noopPublisher returns a mock publisher that succeeds silently.
func noopPublisher(ctrl *gomock.Controller) *queuemock.MockPublisher {
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return pub
}

// noopRequestLogStore returns a mock RequestLogStore that succeeds silently.
func noopRequestLogStore(ctrl *gomock.Controller) *storagemock.MockRequestLogStore {
	s := storagemock.NewMockRequestLogStore(ctrl)
	s.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s
}

func TestNewLandController(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(1), nil)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/abc123def"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", resp.Sqid)
}

func TestLand_ReturnsErrorOnCounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/abc123def"}},
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
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "my-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "request/my-queue", capturedDomain)
}

func TestLand_ReturnsErrorOnEmptyQueue(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsErrorOnEmptyChangeUri(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
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
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopPublisher(ctrl), noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: nil,
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage queue.Message

	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(123), nil)
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			publishedTopic = topic
			publishedMessage = msg
			return nil
		},
	)

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, publisher, noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "test-queue",
		Change:   &pb.Change{Uris: []string{"github://uber/backend/pull/456/fed987cba"}},
		Strategy: pb.Strategy_REBASE,
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", resp.Sqid)

	// Verify message was published
	assert.Equal(t, "request", publishedTopic)
	assert.Equal(t, "test-queue/123", publishedMessage.ID)
	assert.Equal(t, "test-queue", publishedMessage.PartitionKey)

	// Verify payload can be deserialized
	deserializedReq, err := entity.RequestFromBytes(publishedMessage.Payload)
	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", deserializedReq.ID)
	assert.Equal(t, "test-queue", deserializedReq.Queue)
	assert.Equal(t, []string{"github://uber/backend/pull/456/fed987cba"}, deserializedReq.Change.URIs)
	assert.Equal(t, entity.RequestLandStrategyRebase, deserializedReq.LandStrategy)
	assert.Equal(t, entity.RequestStateNew, deserializedReq.State)
	assert.Equal(t, int32(1), deserializedReq.Version)
}

func TestLand_ContinuesWhenPublishFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(999), nil)
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, publisher, noopRequestLogStore(ctrl), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/service/pull/1/abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	// Should fail if publish fails
	require.Error(t, err)
}
