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
	"github.com/uber/submitqueue/extension/storage"
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
	createFunc func(ctx context.Context, request entity.Request) error
}

func (m *mockRequestStore) Get(ctx context.Context, id string) (entity.Request, error) {
	return entity.Request{}, nil
}

func (m *mockRequestStore) Create(ctx context.Context, request entity.Request) error {
	return m.createFunc(ctx, request)
}

func (m *mockRequestStore) UpdateState(ctx context.Context, id string, version int32, newState entity.RequestState) error {
	return nil
}

type mockChangeProviderStore struct {
	createFunc func(ctx context.Context, changeProvider entity.ChangeProvider) error
}

func (m *mockChangeProviderStore) Get(ctx context.Context, requestID string) ([]entity.ChangeProvider, error) {
	return nil, nil
}

func (m *mockChangeProviderStore) Create(ctx context.Context, changeProvider entity.ChangeProvider) error {
	return m.createFunc(ctx, changeProvider)
}

type mockBatchStore struct {
	createFunc      func(ctx context.Context, batch entity.Batch) error
	getFunc         func(ctx context.Context, id string) (entity.Batch, error)
	updateStateFunc func(ctx context.Context, id string, version int32, newState entity.BatchState) error
}

func (m *mockBatchStore) Get(ctx context.Context, id string) (entity.Batch, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, id)
	}
	return entity.Batch{}, nil
}

func (m *mockBatchStore) Create(ctx context.Context, batch entity.Batch) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, batch)
	}
	return nil
}

func (m *mockBatchStore) UpdateState(ctx context.Context, id string, version int32, newState entity.BatchState) error {
	if m.updateStateFunc != nil {
		return m.updateStateFunc(ctx, id, version, newState)
	}
	return nil
}

type mockBatchDependentStore struct {
	createFunc func(ctx context.Context, batchDependent entity.BatchDependent) error
	getFunc    func(ctx context.Context, batchID string) (entity.BatchDependent, error)
}

func (m *mockBatchDependentStore) Get(ctx context.Context, batchID string) (entity.BatchDependent, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, batchID)
	}
	return entity.BatchDependent{}, nil
}

func (m *mockBatchDependentStore) Create(ctx context.Context, batchDependent entity.BatchDependent) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, batchDependent)
	}
	return nil
}

type mockBuildStore struct {
	createFunc       func(ctx context.Context, build entity.Build) error
	getFunc          func(ctx context.Context, id string) (entity.Build, error)
	updateStatusFunc func(ctx context.Context, id string, newStatus entity.BuildStatus) error
}

func (m *mockBuildStore) Get(ctx context.Context, id string) (entity.Build, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, id)
	}
	return entity.Build{}, nil
}

func (m *mockBuildStore) Create(ctx context.Context, build entity.Build) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, build)
	}
	return nil
}

func (m *mockBuildStore) UpdateStatus(ctx context.Context, id string, newStatus entity.BuildStatus) error {
	if m.updateStatusFunc != nil {
		return m.updateStatusFunc(ctx, id, newStatus)
	}
	return nil
}

type mockSpeculationTreeStore struct {
	createFunc            func(ctx context.Context, speculationTree entity.SpeculationTree) error
	getFunc               func(ctx context.Context, batchID string) (entity.SpeculationTree, error)
	updateSpeculationsFunc func(ctx context.Context, batchID string, speculations []map[string]string) error
}

func (m *mockSpeculationTreeStore) Get(ctx context.Context, batchID string) (entity.SpeculationTree, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, batchID)
	}
	return entity.SpeculationTree{}, nil
}

func (m *mockSpeculationTreeStore) Create(ctx context.Context, speculationTree entity.SpeculationTree) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, speculationTree)
	}
	return nil
}

func (m *mockSpeculationTreeStore) UpdateSpeculations(ctx context.Context, batchID string, speculations []map[string]string) error {
	if m.updateSpeculationsFunc != nil {
		return m.updateSpeculationsFunc(ctx, batchID, speculations)
	}
	return nil
}

type mockStorage struct {
	requestStore          storage.RequestStore
	changeProviderStore   storage.ChangeProviderStore
	batchStore            storage.BatchStore
	batchDependentStore   storage.BatchDependentStore
	buildStore            storage.BuildStore
	speculationTreeStore  storage.SpeculationTreeStore
}

func (m *mockStorage) GetRequestStore() storage.RequestStore {
	return m.requestStore
}

func (m *mockStorage) GetChangeProviderStore() storage.ChangeProviderStore {
	return m.changeProviderStore
}

func (m *mockStorage) GetBatchStore() storage.BatchStore {
	return m.batchStore
}

func (m *mockStorage) GetBatchDependentStore() storage.BatchDependentStore {
	return m.batchDependentStore
}

func (m *mockStorage) GetBuildStore() storage.BuildStore {
	return m.buildStore
}

func (m *mockStorage) GetSpeculationTreeStore() storage.SpeculationTreeStore {
	return m.speculationTreeStore
}

func (m *mockStorage) Close() error {
	return nil
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
	store := &mockStorage{
		requestStore: &mockRequestStore{
			createFunc: func(ctx context.Context, request entity.Request) error {
				return nil
			},
		},
		changeProviderStore: &mockChangeProviderStore{
			createFunc: func(ctx context.Context, changeProvider entity.ChangeProvider) error {
				return nil
			},
		},
	}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	store := &mockStorage{
		requestStore: &mockRequestStore{
			createFunc: func(ctx context.Context, request entity.Request) error {
				return nil
			},
		},
		changeProviderStore: &mockChangeProviderStore{
			createFunc: func(ctx context.Context, changeProvider entity.ChangeProvider) error {
				return nil
			},
		},
	}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Uris: []string{"uber/test-repo/123@abc123def"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/1", resp.Sqid)
}

func TestLand_PassesCorrectParametersToStore(t *testing.T) {
	var capturedRequest entity.Request

	store := &mockStorage{
		requestStore: &mockRequestStore{
			createFunc: func(ctx context.Context, request entity.Request) error {
				capturedRequest = request
				return nil
			},
		},
		changeProviderStore: &mockChangeProviderStore{
			createFunc: func(ctx context.Context, changeProvider entity.ChangeProvider) error {
				return nil
			},
		},
	}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 42, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "my-queue",
		Change:   &pb.Change{Source: "github", Uris: []string{"uber/myservice/1@abc111", "uber/myservice/2@def222"}},
		Strategy: pb.Strategy_REBASE,
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "my-queue/42", capturedRequest.ID)
	assert.Equal(t, "my-queue", capturedRequest.Queue)
	assert.Equal(t, "github", capturedRequest.Change.Source)
	assert.Equal(t, []string{"uber/myservice/1@abc111", "uber/myservice/2@def222"}, capturedRequest.Change.URIs)
	assert.Equal(t, entity.RequestLandStrategyRebase, capturedRequest.LandStrategy)
	assert.Equal(t, entity.RequestStateNew, capturedRequest.State)
	assert.Equal(t, int32(1), capturedRequest.Version)
	assert.Equal(t, "my-queue/42", resp.Sqid)
}

func TestLand_ReturnsErrorOnStorageFailure(t *testing.T) {
	store := &mockStorage{
		requestStore: &mockRequestStore{
			createFunc: func(ctx context.Context, request entity.Request) error {
				return fmt.Errorf("database connection failed")
			},
		},
		changeProviderStore: &mockChangeProviderStore{
			createFunc: func(ctx context.Context, changeProvider entity.ChangeProvider) error {
				return nil
			},
		},
	}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Uris: []string{"uber/test-repo/123@abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}

func TestLand_ReturnsErrorOnCounterFailure(t *testing.T) {
	store := &mockStorage{
		requestStore: &mockRequestStore{
			createFunc: func(ctx context.Context, request entity.Request) error {
				return nil
			},
		},
		changeProviderStore: &mockChangeProviderStore{
			createFunc: func(ctx context.Context, changeProvider entity.ChangeProvider) error {
				return nil
			},
		},
	}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 0, fmt.Errorf("counter unavailable")
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Uris: []string{"uber/test-repo/123@abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
}

func TestLand_CounterDomainIncludesQueue(t *testing.T) {
	var capturedDomain string

	store := &mockStorage{
		requestStore: &mockRequestStore{
			createFunc: func(ctx context.Context, request entity.Request) error {
				return nil
			},
		},
		changeProviderStore: &mockChangeProviderStore{
			createFunc: func(ctx context.Context, changeProvider entity.ChangeProvider) error {
				return nil
			},
		},
	}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		capturedDomain = domain
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "my-queue",
		Change: &pb.Change{Source: "github", Uris: []string{"uber/test-repo/123@abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "request/my-queue", capturedDomain)
}

func TestLand_ReturnsErrorOnEmptyQueue(t *testing.T) {
	store := &mockStorage{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entity.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "",
		Change: &pb.Change{Source: "github", Uris: []string{"uber/test-repo/123@abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsErrorOnEmptyChangeSource(t *testing.T) {
	store := &mockStorage{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entity.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "", Uris: []string{"uber/test-repo/123@abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_ReturnsErrorOnNilChange(t *testing.T) {
	store := &mockStorage{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entity.Request) error {
			return nil
		},
	}}
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

func TestLand_ReturnsErrorOnEmptyChangeIDs(t *testing.T) {
	store := &mockStorage{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entity.Request) error {
			return nil
		},
	}}
	cnt := &mockCounter{nextFunc: func(ctx context.Context, domain string) (int64, error) {
		return 1, nil
	}}
	controller := NewLandController(zap.NewNop().Sugar(), tally.NoopScope, store, cnt, noopPublisher(), "request")
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Uris: []string{}},
	}
	_, err := controller.Land(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestLand_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage queue.Message

	store := &mockStorage{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entity.Request) error {
			return nil
		},
	}}
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
		Change:   &pb.Change{Source: "github", Uris: []string{"uber/backend/456@fed987cba"}},
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
	assert.Equal(t, "github", deserializedReq.Change.Source)
	assert.Equal(t, []string{"uber/backend/456@fed987cba"}, deserializedReq.Change.URIs)
	assert.Equal(t, entity.RequestLandStrategyRebase, deserializedReq.LandStrategy)
	assert.Equal(t, entity.RequestStateNew, deserializedReq.State)
	assert.Equal(t, int32(1), deserializedReq.Version)
}

func TestLand_ContinuesWhenPublishFails(t *testing.T) {
	store := &mockStorage{requestStore: &mockRequestStore{
		createFunc: func(ctx context.Context, request entity.Request) error {
			return nil
		},
	}}
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
		Change: &pb.Change{Source: "github", Uris: []string{"uber/service/1@abc123def"}},
	}
	_, err := controller.Land(ctx, req)

	// Should fail if publish fails
	require.Error(t, err)
}
