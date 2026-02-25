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
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

type mockCounter struct {
	nextFunc func(ctx context.Context, domain string) (int64, error)
}

func (m *mockCounter) Next(ctx context.Context, domain string) (int64, error) {
	return m.nextFunc(ctx, domain)
}

type mockPublisher struct {
	publishFunc func(ctx context.Context, topic string, msg queue.Message) error
}

func (m *mockPublisher) Publish(ctx context.Context, topic string, msg queue.Message) error {
	return m.publishFunc(ctx, topic, msg)
}

func (m *mockPublisher) Close() error {
	return nil
}

// noopPublisher returns a mock publisher that succeeds silently.
func noopPublisher() *mockPublisher {
	return &mockPublisher{publishFunc: func(ctx context.Context, topic string, msg queue.Message) error {
		return nil
	}}
}

func TestNewLandController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/abc123def"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", resp.Sqid)
}

func TestLand_PassesCorrectParametersToStore(t *testing.T) {
	var capturedRequest entity.Request

	ctrl := gomock.NewController(t)
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request entity.Request) error {
			capturedRequest = request
			return nil
		},
	)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 42, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "my-queue",
		Change:   &pb.Change{Uris: []string{"github://uber/myservice/pull/1/abc111", "github://uber/myservice/pull/2/def222"}},
		Strategy: pb.Strategy_REBASE,
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "my-queue/42", capturedRequest.ID)
	assert.Equal(t, "my-queue", capturedRequest.Queue)
	assert.Equal(t, []string{"github://uber/myservice/pull/1/abc111", "github://uber/myservice/pull/2/def222"}, capturedRequest.Change.URIs)
	assert.Equal(t, entity.RequestLandStrategyRebase, capturedRequest.LandStrategy)
	assert.Equal(t, entity.RequestStateNew, capturedRequest.State)
	assert.Equal(t, int32(1), capturedRequest.Version)
	assert.Equal(t, "my-queue/42", resp.Sqid)
}

func TestLand_ReturnsErrorOnStorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("database connection failed"))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/test-repo/pull/123/abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}

func TestLand_ReturnsErrorOnCounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 0, fmt.Errorf("counter unavailable")
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
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
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		capturedDomain = domain
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
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
	store := storagemock.NewMockStorage(ctrl)

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
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
	store := storagemock.NewMockStorage(ctrl)

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
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
	store := storagemock.NewMockStorage(ctrl)

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
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
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 123, nil
	}}
	publisher := &mockPublisher{publishFunc: func(ctx context.Context, topic string, msg queue.Message) error {
		publishedTopic = topic
		publishedMessage = msg
		return nil
	}}

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, publisher, "request")
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
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 999, nil
	}}
	publisher := &mockPublisher{publishFunc: func(ctx context.Context, topic string, msg queue.Message) error {
		return fmt.Errorf("queue unavailable")
	}}

	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, publisher, "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Uris: []string{"github://uber/service/pull/1/abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	// Should fail if publish fails
	require.Error(t, err)
}
