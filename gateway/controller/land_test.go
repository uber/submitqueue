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

type mockRequestStore struct {
	createFunc func(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error)
}

func (m *mockRequestStore) Get(ctx context.Context, id string) (entities.Request, error) {
	return entities.Request{}, nil
}

func (m *mockRequestStore) Create(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
	return m.createFunc(ctx, queue, change, strategy, state)
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
		createFunc: func(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
			return entities.Request{}, nil
		},
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory)
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
			return entities.Request{
				Queue:        queue,
				Seq:          1,
				Change:       change,
				LandStrategy: strategy,
				State:        state,
				Version:      1,
			}, nil
		},
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory)
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
	var capturedQueue string
	var capturedChange entities.Change
	var capturedStrategy entities.RequestLandStrategy
	var capturedState entities.RequestState

	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
			capturedQueue = queue
			capturedChange = change
			capturedStrategy = strategy
			capturedState = state
			return entities.Request{
				Queue:        queue,
				Seq:          42,
				Change:       change,
				LandStrategy: strategy,
				State:        state,
				Version:      1,
			}, nil
		},
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "my-queue",
		Change:   &pb.Change{Source: "github", Ids: []string{"pr-1", "pr-2"}},
		Strategy: pb.Strategy_STRATEGY_REBASE,
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "my-queue", capturedQueue)
	assert.Equal(t, "github", capturedChange.Source)
	assert.Equal(t, []string{"pr-1", "pr-2"}, capturedChange.IDs)
	assert.Equal(t, entities.RequestLandStrategyRebase, capturedStrategy)
	assert.Equal(t, entities.RequestStateNew, capturedState)
	assert.Equal(t, "my-queue/42", resp.Sqid)
}

func TestLand_ReturnsErrorOnStorageFailure(t *testing.T) {
	factory := &mockStoreFactory{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
			return entities.Request{}, fmt.Errorf("database connection failed")
		},
	}}
	controller := NewLandController(zap.NewNop(), tally.NoopScope, factory)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}
