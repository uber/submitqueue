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

package dlq

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newPrioritizeRegistry wires a TopicRegistry exposing the prioritize topic
// backed by a mock publisher, returning both so tests can set expectations.
func newPrioritizeRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	pub := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyPrioritize, Name: "prioritize", Queue: q},
	})
	require.NoError(t, err)
	return registry, pub
}

func TestDLQQueueController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newPrioritizeRegistry(t, ctrl)

	c := NewDLQQueueController(zaptest.NewLogger(t).Sugar(), testScope(), registry, TopicKey(topickey.TopicKeyPrioritize), "orchestrator-prioritize-dlq")

	assert.Equal(t, "prioritize_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("prioritize_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-prioritize-dlq", c.ConsumerGroup())
}

// TestDLQQueueController_Process_RequeuesRound verifies the reconciler
// re-arms the queue: a fresh prioritize round is published for the queue,
// with a message ID derived from the dead-lettered message's ID (distinct
// from the original round so publish dedup does not swallow it, stable
// across DLQ redeliveries so they coalesce).
func TestDLQQueueController_Process_RequeuesRound(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newPrioritizeRegistry(t, ctrl)

	c := NewDLQQueueController(zaptest.NewLogger(t).Sugar(), testScope(), registry, TopicKey(topickey.TopicKeyPrioritize), "orchestrator-prioritize-dlq")

	payload, err := entity.QueueID{Name: "q"}.ToBytes()
	require.NoError(t, err)

	var published entityqueue.Message
	pub.EXPECT().Publish(gomock.Any(), "prioritize", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			published = msg
			return nil
		},
	)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))

	assert.Equal(t, "dlq-msg-1/dlq-requeue", published.ID, "requeued ID should derive from the DLQ message's ID")
	assert.Equal(t, "q", published.PartitionKey, "rounds are partitioned by queue name")
	qid, err := entity.QueueIDFromBytes(published.Payload)
	require.NoError(t, err)
	assert.Equal(t, "q", qid.Name)
}

// TestDLQQueueController_Process_PublishFailureNacks verifies a failed
// requeue publish surfaces as an error so the DLQ consumer redelivers and
// the re-arm is eventually made.
func TestDLQQueueController_Process_PublishFailureNacks(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, pub := newPrioritizeRegistry(t, ctrl)

	c := NewDLQQueueController(zaptest.NewLogger(t).Sugar(), testScope(), registry, TopicKey(topickey.TopicKeyPrioritize), "orchestrator-prioritize-dlq")

	payload, err := entity.QueueID{Name: "q"}.ToBytes()
	require.NoError(t, err)

	sentinel := errors.New("publish down")
	pub.EXPECT().Publish(gomock.Any(), "prioritize", gomock.Any()).Return(sentinel)

	delivery := newMockDelivery(ctrl, payload)
	require.ErrorIs(t, c.Process(context.Background(), delivery), sentinel)
}

func TestDLQQueueController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newPrioritizeRegistry(t, ctrl)

	c := NewDLQQueueController(zaptest.NewLogger(t).Sugar(), testScope(), registry, TopicKey(topickey.TopicKeyPrioritize), "orchestrator-prioritize-dlq")

	delivery := newMockDelivery(ctrl, []byte("garbage"))
	err := c.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestDLQQueueController_Process_EmptyNameFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newPrioritizeRegistry(t, ctrl)

	c := NewDLQQueueController(zaptest.NewLogger(t).Sugar(), testScope(), registry, TopicKey(topickey.TopicKeyPrioritize), "orchestrator-prioritize-dlq")

	payload, err := entity.QueueID{Name: ""}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	err = c.Process(context.Background(), delivery)
	require.Error(t, err)
}
