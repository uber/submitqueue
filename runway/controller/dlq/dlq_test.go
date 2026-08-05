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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testID           = "test-queue/1"
	testQueue        = "test-queue"
	testPartitionKey = "test-queue"
)

// publishedMsg captures a message published to the signal topic.
type publishedMsg struct {
	topic string
	msg   entityqueue.Message
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte, meta map[string]string) *consumermock.MockDelivery {
	t.Helper()
	msg := entityqueue.NewMessage(testID, payload, testPartitionKey, nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Metadata().Return(meta).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

// newRegistry builds a registry whose signal-topic publisher records every
// message it receives into the returned slice pointer.
func newRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *[]publishedMsg) {
	t.Helper()
	var published []publishedMsg
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			published = append(published, publishedMsg{topic: topic, msg: msg})
			return nil
		},
	).AnyTimes()

	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyMergeSignal, Name: "merge-signal", Queue: q},
	})
	require.NoError(t, err)
	return registry, &published
}

func newController(t *testing.T, registry consumer.TopicRegistry) *Controller {
	t.Helper()
	return NewController(Params{
		Logger:         zaptest.NewLogger(t).Sugar(),
		Scope:          tally.NoopScope,
		Registry:       registry,
		TopicKey:       TopicKey(runwaymq.TopicKeyMerge),
		SignalTopicKey: runwaymq.TopicKeyMergeSignal,
		ConsumerGroup:  "runway-merge-dlq",
	})
}

func TestProcess_DecodableRepublishesFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, published := newRegistry(t, ctrl)
	controller := newController(t, registry)

	req := &runwaymq.MergeRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.MergeStep{{StepId: "step-1"}},
	}
	payload, err := runwaymq.Marshal(req)
	require.NoError(t, err)

	meta := map[string]string{
		"dlq.last_error":     "boom: connection refused",
		"dlq.original_topic": "runway-merge",
	}
	delivery := newDelivery(t, ctrl, payload, meta)

	require.NoError(t, controller.Process(context.Background(), delivery))

	require.Len(t, *published, 1)
	got := (*published)[0]
	assert.Equal(t, "merge-signal", got.topic)

	result := &runwaymq.MergeResult{}
	require.NoError(t, runwaymq.Unmarshal(got.msg.Payload, result))
	assert.Equal(t, testID, result.Id)
	assert.Equal(t, runwaypb.Outcome_FAILED, result.Outcome)
	assert.Contains(t, result.Reason, "boom: connection refused")
}

func TestProcess_UndecodableAcksAndPublishesNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, published := newRegistry(t, ctrl)
	controller := newController(t, registry)

	delivery := newDelivery(t, ctrl, []byte("{bad"), map[string]string{"dlq.original_topic": "runway-merge"})

	require.NoError(t, controller.Process(context.Background(), delivery))
	assert.Empty(t, *published)
}
