package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entities"
	"github.com/uber/submitqueue/extensions/storage"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

type mockCounter struct {
	nextFunc func(ctx context.Context, domain string) (int64, error)
}

func (m *mockCounter) Next(ctx context.Context, domain string) (int64, error) {
	return m.nextFunc(ctx, domain)
}

type mockRequestStore struct {
	createFunc func(ctx context.Context, request entities.Request) error
}

func (m *mockRequestStore) Get(ctx context.Context, id string) (entities.Request, error) {
	return entities.Request{}, nil
}

func (m *mockRequestStore) Create(ctx context.Context, request entities.Request) error {
	return m.createFunc(ctx, request)
}

func (m *mockRequestStore) UpdateState(ctx context.Context, id string, version int32, newState entities.RequestState) error {
	return nil
}

type mockStoreFactory struct {
	requestStore storage.RequestStore
}

func (m *mockStoreFactory) GetRequestStore() storage.RequestStore {
	return m.requestStore
}

func (m *mockStoreFactory) Close() error {
	return nil
}

func TestNewLandController(t *testing.T) {
	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entities.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory, cnt)
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entities.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory, cnt)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", resp.Sqid)
}

func TestLand_PassesCorrectParametersToStore(t *testing.T) {
	var capturedRequest entities.Request

	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entities.Request) error {
			capturedRequest = request
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 42, nil
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory, cnt)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "my-queue",
		Change:   &pb.Change{Source: "github", Ids: []string{"pr-1", "pr-2"}},
		Strategy: pb.Strategy_REBASE,
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "my-queue/42", capturedRequest.ID)
	assert.Equal(t, "my-queue", capturedRequest.Queue)
	assert.Equal(t, "github", capturedRequest.Change.Source)
	assert.Equal(t, []string{"pr-1", "pr-2"}, capturedRequest.Change.IDs)
	assert.Equal(t, entities.RequestLandStrategyRebase, capturedRequest.LandStrategy)
	assert.Equal(t, entities.RequestStateNew, capturedRequest.State)
	assert.Equal(t, int32(1), capturedRequest.Version)
	assert.Equal(t, "my-queue/42", resp.Sqid)
}

func TestLand_ReturnsErrorOnStorageFailure(t *testing.T) {
	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entities.Request) error {
			return fmt.Errorf("database connection failed")
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory, cnt)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}

func TestLand_ReturnsErrorOnCounterFailure(t *testing.T) {
	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entities.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 0, fmt.Errorf("counter unavailable")
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory, cnt)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}

func TestLand_CounterDomainIncludesQueue(t *testing.T) {
	var capturedDomain string

	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entities.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		capturedDomain = domain
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory, cnt)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "my-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	_, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "request/my-queue", capturedDomain)
}
