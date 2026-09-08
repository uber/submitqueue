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

package publish

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"go.uber.org/mock/gomock"
)

const testKey consumer.TopicKey = "test-topic-key"

func newTestRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	t.Helper()

	publisher := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: testKey, Name: "test-topic", Queue: q}},
	)
	require.NoError(t, err)
	return registry, publisher
}

func TestMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newTestRegistry(t, ctrl)

	var published entityqueue.Message
	publisher.EXPECT().
		Publish(gomock.Any(), "test-topic", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			published = msg
			return nil
		})

	err := Message(context.Background(), registry, testKey, MessageParams{
		Tenant:       "tenant-1",
		ID:           "msg-1",
		Payload:      []byte("payload"),
		PartitionKey: "partition-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "tenant-1", published.Tenant)
	assert.Equal(t, "msg-1", published.ID)
	assert.Equal(t, []byte("payload"), published.Payload)
	assert.Equal(t, "partition-1", published.PartitionKey)
	assert.Equal(t, "tenant-1", published.Metadata[entityqueue.MetadataKeyQueueName])
}

func TestMessage_PropagatesTenantAsQueueName(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newTestRegistry(t, ctrl)

	var published entityqueue.Message
	publisher.EXPECT().
		Publish(gomock.Any(), "test-topic", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			published = msg
			return nil
		})

	require.NoError(t, Message(context.Background(), registry, testKey, MessageParams{
		Tenant:       "monorepo/main",
		ID:           "msg-1",
		Payload:      []byte("payload"),
		PartitionKey: "partition-1",
	}))
	assert.Equal(t, "monorepo/main", published.Metadata[entityqueue.MetadataKeyQueueName])
	assert.Equal(t, "monorepo/main", published.Tenant)
}

func TestMessage_MergesMetadataWithoutMutatingInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, publisher := newTestRegistry(t, ctrl)

	var published entityqueue.Message
	publisher.EXPECT().
		Publish(gomock.Any(), "test-topic", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			published = msg
			return nil
		})

	metadata := map[string]string{"failure_reason": "build failed"}
	require.NoError(t, Message(context.Background(), registry, testKey, MessageParams{
		Tenant:       "monorepo/main",
		ID:           "msg-1",
		Payload:      []byte("payload"),
		PartitionKey: "partition-1",
		Metadata:     metadata,
	}))
	assert.Equal(t, map[string]string{
		"failure_reason":                 "build failed",
		entityqueue.MetadataKeyQueueName: "monorepo/main",
	}, published.Metadata)
	assert.Equal(t, "monorepo/main", published.Tenant)
	assert.Equal(t, map[string]string{"failure_reason": "build failed"}, metadata)
}

func TestMessage_RejectsQueueNameDifferentFromTenant(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newTestRegistry(t, ctrl)

	metadata := map[string]string{entityqueue.MetadataKeyQueueName: "outbound"}
	err := Message(context.Background(), registry, testKey, MessageParams{
		Tenant:       "inbound",
		ID:           "msg-1",
		Payload:      []byte("payload"),
		PartitionKey: "partition-1",
		Metadata:     metadata,
	})
	require.Error(t, err)
}

func TestMessage_UnregisteredKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newTestRegistry(t, ctrl)

	err := Message(context.Background(), registry, "unregistered-key", MessageParams{
		Tenant:       "tenant-1",
		ID:           "msg-1",
		Payload:      []byte("payload"),
		PartitionKey: "partition-1",
	})
	require.Error(t, err)
}

func TestMessage_RequiresTenant(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := newTestRegistry(t, ctrl)

	err := Message(context.Background(), registry, testKey, MessageParams{
		ID:           "msg-1",
		Payload:      []byte("payload"),
		PartitionKey: "partition-1",
	})
	require.Error(t, err)
}

func TestIntentID(t *testing.T) {
	tests := []struct {
		name     string
		entityID string
		cause    []string
		want     string
	}{
		{
			name:     "no cause is the bare entity ID",
			entityID: "batch-1",
			want:     "batch-1",
		},
		{
			name:     "single cause",
			entityID: "batch-1",
			cause:    []string{"landed"},
			want:     "batch-1/landed",
		},
		{
			name:     "multiple causes join in order",
			entityID: "batch-1",
			cause:    []string{"build-signal", "build-9", "running"},
			want:     "batch-1/build-signal/build-9/running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IntentID(tt.entityID, tt.cause...))
		})
	}
}

// The convention only works if the same cause is repeatable and a different
// cause is distinguishable — the two properties every call site relies on.
func TestIntentID_StableAcrossCallsAndDistinctPerCause(t *testing.T) {
	assert.Equal(t, IntentID("batch-1", "landed"), IntentID("batch-1", "landed"))
	assert.NotEqual(t, IntentID("batch-1", "landed"), IntentID("batch-1"))
	assert.NotEqual(t, IntentID("batch-1", "landed"), IntentID("batch-1", "cancelling"))
}

func TestUniqueID(t *testing.T) {
	a := UniqueID("batch-1")
	b := UniqueID("batch-1")

	assert.True(t, strings.HasPrefix(a, "batch-1@"))
	assert.True(t, strings.HasPrefix(b, "batch-1@"))
	assert.NotEqual(t, a, b)
}
