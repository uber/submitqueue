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
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	pb "github.com/uber/submitqueue/api/stovepipe/gateway/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testGitURI    = "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/abcdef0123456789abcdef0123456789abcdef01"
	testQueueName = "stovepipe-monorepo"
)

// newIngestTestRegistry builds a TopicRegistry for TopicKeyStart wired to a mock
// Queue/Publisher and returns both so callers can set EXPECT() on the publisher.
func newIngestTestRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *queuemock.MockPublisher) {
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

// newIngestTestRegistryWithNoopPublisher returns a registry whose publisher
// silently accepts any Publish call.
func newIngestTestRegistryWithNoopPublisher(t *testing.T, ctrl *gomock.Controller) consumer.TopicRegistry {
	t.Helper()
	registry, pub := newIngestTestRegistry(t, ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return registry
}

func TestNewIngestController(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	require.NotNil(t, c)
}

func TestIngest_ReturnsSPID(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(1), nil)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	resp, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: &changepb.Change{Uris: []string{testGitURI}},
	})

	require.NoError(t, err)
	assert.Equal(t, "stovepipe-monorepo/1", resp.Spid)
}

func TestIngest_CounterDomainIncludesQueue(t *testing.T) {
	var capturedDomain string
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, domain string) (int64, error) {
			capturedDomain = domain
			return 1, nil
		},
	)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	_, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: &changepb.Change{Uris: []string{testGitURI}},
	})

	require.NoError(t, err)
	assert.Equal(t, "ingest/"+testQueueName, capturedDomain)
}

func TestIngest_ReturnsErrorOnCounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	_, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: &changepb.Change{Uris: []string{testGitURI}},
	})

	require.Error(t, err)
}

func TestIngest_ReturnsErrorOnEmptyQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	_, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  "",
		Change: &changepb.Change{Uris: []string{testGitURI}},
	})

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestIngest_ReturnsErrorOnNilChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	_, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: nil,
	})

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestIngest_ReturnsErrorOnEmptyChangeURIs(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, newIngestTestRegistryWithNoopPublisher(t, ctrl))
	_, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: &changepb.Change{Uris: []string{}},
	})

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestIngest_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage entityqueue.Message

	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(42), nil)

	registry, publisher := newIngestTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			publishedTopic = topic
			publishedMessage = msg
			return nil
		},
	)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, registry)
	resp, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: &changepb.Change{Uris: []string{testGitURI}},
	})

	require.NoError(t, err)
	assert.Equal(t, "stovepipe-monorepo/42", resp.Spid)
	assert.Equal(t, "start", publishedTopic)
	assert.Equal(t, "stovepipe-monorepo/42", publishedMessage.ID)
	assert.Equal(t, testQueueName, publishedMessage.PartitionKey)

	deserialized, err := entity.IngestRequestFromBytes(publishedMessage.Payload)
	require.NoError(t, err)
	assert.Equal(t, "stovepipe-monorepo/42", deserialized.ID)
	assert.Equal(t, testQueueName, deserialized.Queue)
	assert.Equal(t, []string{testGitURI}, deserialized.Change.URIs)
}

func TestIngest_ReturnsErrorOnPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(1), nil)

	registry, publisher := newIngestTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	c := NewIngestController(zap.NewNop().Sugar(), tally.NoopScope, cnt, registry)
	_, err := c.Ingest(context.Background(), &pb.IngestRequest{
		Queue:  testQueueName,
		Change: &changepb.Change{Uris: []string{testGitURI}},
	})

	require.Error(t, err)
}
