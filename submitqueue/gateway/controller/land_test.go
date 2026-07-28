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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
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

// noopStorage returns stateful request storage whose writes succeed.
func noopStorage(ctrl *gomock.Controller) storage.Storage {
	return newControllerStorageFixture(ctrl).storage
}

// noopQueueConfigStore returns a mock queueconfig.Store that always reports
// the queue as configured.
func noopQueueConfigStore(ctrl *gomock.Controller) *qcmock.MockStore {
	s := qcmock.NewMockStore(ctrl)
	s.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.QueueConfig{}, nil).AnyTimes()
	return s
}

// testLandRequest returns a valid entity.LandRequest for the given queue. The ID
// is intentionally left empty — the controller assigns it.
func testLandRequest(queue string) entity.LandRequest {
	return entity.LandRequest{
		Queue:        queue,
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/test-repo/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
	}
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

	result, err := controller.Land(ctx, testLandRequest("test-queue"))

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", result.ID)
}

func TestLand_ReturnsErrorOnCounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	_, err := controller.Land(ctx, testLandRequest("test-queue"))

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

	_, err := controller.Land(ctx, testLandRequest("my-queue"))

	require.NoError(t, err)
	assert.Equal(t, "request/my-queue", capturedDomain)
}

func TestLand_ReturnsErrorOnEmptyQueue(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := testLandRequest("")
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ValidatesQueueLengthBeforeAllocatingSqid(t *testing.T) {
	tests := []struct {
		name      string
		queue     string
		wantError bool
	}{
		{name: "maximum length", queue: strings.Repeat("q", maxQueueIdentifierBytes)},
		{name: "over maximum", queue: strings.Repeat("q", maxQueueIdentifierBytes+1), wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cnt := countermock.NewMockCounter(ctrl)
			if !tt.wantError {
				cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(1), nil)
			}
			controller := NewLandController(
				zap.NewNop().Sugar(),
				tally.NoopScope,
				cnt,
				noopStorage(ctrl),
				noopQueueConfigStore(ctrl),
				newTestRegistryWithNoopPublisher(t, ctrl),
			)

			result, err := controller.Land(context.Background(), entity.LandRequest{
				Queue:  tt.queue,
				Change: change.Change{URIs: []string{"uri"}},
			})

			if tt.wantError {
				require.Error(t, err)
				assert.True(t, IsInvalidRequest(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.queue+"/1", result.ID)
		})
	}
}

func TestLand_ReturnsErrorOnEmptyChangeUri(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := entity.LandRequest{
		Queue:  "test-queue",
		Change: change.Change{URIs: []string{}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsErrorOnInvalidChangeURIs(t *testing.T) {
	tests := []struct {
		name string
		uris []string
	}{
		{name: "empty URI element", uris: []string{""}},
		{name: "URI exceeds storage limit", uris: []string{strings.Repeat("x", maxStorageIdentifierBytes+1)}},
		{name: "duplicate exact URI", uris: []string{"uri", "uri"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			controller := NewLandController(
				zap.NewNop().Sugar(),
				tally.NoopScope,
				countermock.NewMockCounter(ctrl),
				noopStorage(ctrl),
				noopQueueConfigStore(ctrl),
				newTestRegistryWithNoopPublisher(t, ctrl),
			)

			_, err := controller.Land(context.Background(), entity.LandRequest{
				Queue:  "test-queue",
				Change: change.Change{URIs: tt.uris},
			})

			require.Error(t, err)
			assert.True(t, IsInvalidRequest(err))
		})
	}
}

func TestLand_ReturnsErrorOnZeroValueChange(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, noopStorage(ctrl), noopQueueConfigStore(ctrl), newTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := entity.LandRequest{
		Queue:  "test-queue",
		Change: change.Change{},
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

	_, err := controller.Land(ctx, testLandRequest("missing-queue"))

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

	_, err := controller.Land(ctx, testLandRequest("test-queue"))

	require.Error(t, err)
	assert.False(t, IsUnrecognizedQueue(err))
	assert.False(t, IsInvalidRequest(err))
}

func TestLand_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage entityqueue.Message
	var receiptSummary entity.RequestSummary
	var materializedSummary entity.RequestSummary
	var persistedMapping entity.RequestURI
	var persistedQueueSummary entity.RequestQueueSummary
	var persistedLog entity.RequestLog

	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(123), nil)

	store := storagemock.NewMockStorage(ctrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	uriStore := storagemock.NewMockRequestURIStore(ctrl)
	queueStore := storagemock.NewMockRequestQueueSummaryStore(ctrl)
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
	store.EXPECT().GetRequestURIStore().Return(uriStore).AnyTimes()
	store.EXPECT().GetRequestQueueSummaryStore().Return(queueStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	registry, publisher := newTestRegistry(t, ctrl)
	gomock.InOrder(
		summaryStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, summary entity.RequestSummary) error {
				receiptSummary = summary
				return nil
			},
		),
		publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, topic string, msg entityqueue.Message) error {
				publishedTopic = topic
				publishedMessage = msg
				return nil
			},
		),
		logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, log entity.RequestLog) error {
				persistedLog = log
				return nil
			},
		),
		summaryStore.EXPECT().Get(gomock.Any(), "test-queue/123").DoAndReturn(
			func(context.Context, string) (entity.RequestSummary, error) {
				return receiptSummary, nil
			},
		),
		summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).DoAndReturn(
			func(_ context.Context, summary entity.RequestSummary, _, newVersion int32) error {
				summary.Version = newVersion
				materializedSummary = summary
				return nil
			},
		),
		queueStore.EXPECT().Get(gomock.Any(), "test-queue", gomock.Any(), "test-queue/123").DoAndReturn(
			func(context.Context, string, int64, string) (entity.RequestQueueSummary, error) {
				return entity.RequestQueueSummary{}, storage.ErrNotFound
			},
		),
		uriStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, mapping entity.RequestURI) error {
				persistedMapping = mapping
				return nil
			},
		),
		queueStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, summary entity.RequestQueueSummary) error {
				persistedQueueSummary = summary
				return nil
			},
		),
	)

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, store, noopQueueConfigStore(ctrl), registry)
	ctx := context.Background()

	req := entity.LandRequest{
		Queue:        "test-queue",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
	}
	result, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", result.ID)

	assert.Equal(t, entity.RequestSummary{
		RequestID:         "test-queue/123",
		Queue:             "test-queue",
		ChangeURIs:        []string{"github://github.example.com/uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98"},
		ReceivedAtMs:      receiptSummary.ReceivedAtMs,
		Status:            entity.RequestStatusAccepting,
		StatusTimestampMs: receiptSummary.ReceivedAtMs,
		Version:           1,
		Metadata:          map[string]string{},
	}, receiptSummary)
	assert.Positive(t, receiptSummary.ReceivedAtMs)
	assert.Equal(t, entity.RequestLog{
		RequestID:   "test-queue/123",
		TimestampMs: receiptSummary.ReceivedAtMs,
		Status:      entity.RequestStatusAccepted,
		Metadata:    map[string]string{},
	}, persistedLog)
	assert.Equal(t, entity.RequestStatusAccepted, materializedSummary.Status)
	assert.Equal(t, int32(2), materializedSummary.Version)
	assert.Equal(t, entity.RequestURI{
		ChangeURI:    "github://github.example.com/uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98",
		ReceivedAtMs: receiptSummary.ReceivedAtMs,
		RequestID:    "test-queue/123",
	}, persistedMapping)
	assert.Equal(t, entity.RequestQueueSummary{
		RequestID:    "test-queue/123",
		Queue:        "test-queue",
		ChangeURIs:   []string{"github://github.example.com/uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98"},
		ReceivedAtMs: receiptSummary.ReceivedAtMs,
		Status:       entity.RequestStatusAccepted,
		Version:      2,
		Metadata:     map[string]string{},
	}, persistedQueueSummary)

	// Verify message was published to the topic registered under TopicKeyStart
	assert.Equal(t, "start", publishedTopic)
	assert.Equal(t, "test-queue/123", publishedMessage.ID)
	assert.Equal(t, "test-queue", publishedMessage.PartitionKey)

	// Verify payload can be deserialized
	deserializedReq, err := entity.LandRequestFromBytes(publishedMessage.Payload)
	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", deserializedReq.ID)
	assert.Equal(t, "test-queue", deserializedReq.Queue)
	assert.Equal(t, []string{"github://github.example.com/uber/backend/pull/456/fedcba9876543210fedcba9876543210fedcba98"}, deserializedReq.Change.URIs)
	assert.Equal(t, mergestrategy.MergeStrategyRebase, deserializedReq.LandStrategy)
}

func TestLand_ReturnsErrorWhenPublishFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(999), nil)

	store := storagemock.NewMockStorage(ctrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.RequestSummary) error {
		assert.Equal(t, entity.RequestStatusAccepting, summary.Status)
		return nil
	})

	registry, publisher := newTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, cnt, store, noopQueueConfigStore(ctrl), registry)
	ctx := context.Background()

	_, err := controller.Land(ctx, testLandRequest("test-queue"))

	require.Error(t, err)
}

func TestLand_ReturnsSqidWhenAcceptedLogFailsAfterPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(999), nil)
	fixture := newControllerStorageFixture(ctrl)
	fixture.setLogInsertError(fmt.Errorf("log unavailable"))

	controller := NewLandController(
		zap.NewNop().Sugar(),
		tally.NoopScope,
		cnt,
		fixture.storage,
		noopQueueConfigStore(ctrl),
		newTestRegistryWithNoopPublisher(t, ctrl),
	)
	result, err := controller.Land(context.Background(), testLandRequest("test-queue"))

	require.NoError(t, err)
	assert.Equal(t, "test-queue/999", result.ID)
}
