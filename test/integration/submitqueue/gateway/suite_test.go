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

package gateway

// Gateway Integration Tests
//
// These tests use docker-compose from service/submitqueue/gateway/server/docker-compose.yml.
// They are hermetic: the gateway image is built from a staged context whose
// inputs (Bazel-built Linux binary, Dockerfile, queues.yaml) are all declared
// data dependencies of the test target.
//
// Run with:
//   make integration-test-submitqueue-gateway

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	corerequest "github.com/uber/submitqueue/submitqueue/gateway/core/request"
	"github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage/mysql"
	orchrequest "github.com/uber/submitqueue/submitqueue/orchestrator/core/request"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GatewayIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.SubmitQueueGatewayClient
	db      *sql.DB // App database
	queueDB *sql.DB // Queue database
}

func TestGatewayIntegration(t *testing.T) {
	suite.Run(t, new(GatewayIntegrationSuite))
}

// The container boundary leaves the request-summary RPC as the only signal that
// the gateway consumer persisted a log entry.
const persistPollInterval = 500 * time.Millisecond

func (s *GatewayIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Gateway integration test suite using docker-compose")

	// Use docker-compose from service/submitqueue/gateway/server, resolved
	// from the test runfiles. The gateway image is built from a staged build
	// context assembled entirely from declared data dependencies.
	composeFile := testutil.Runfile("service/submitqueue/gateway/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-submitqueue-gateway",
		testutil.WithBuildContext(map[string]string{
			".docker-bin/gateway":                            "service/submitqueue/gateway/server/gateway_linux",
			"service/submitqueue/gateway/server/Dockerfile":  "service/submitqueue/gateway/server/Dockerfile",
			"service/submitqueue/gateway/server/queues.yaml": "service/submitqueue/gateway/server/queues.yaml",
		}))

	// Start the compose stack (Gateway + 2 MySQL DBs)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to application database
	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err, "failed to connect to MySQL")

	// Connect to queue database
	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	// Apply schemas programmatically to application database
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/gateway/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// Connect to Gateway gRPC service
	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("gateway-service", 8080)
	require.NoError(t, err, "failed to connect to gateway")
	s.client = pb.NewSubmitQueueGatewayClient(conn)

	s.log.Logf("Gateway integration test suite ready")
}

func (s *GatewayIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Gateway integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// TestPingAPI tests the Gateway Ping API
func (s *GatewayIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Gateway Ping failed")
	assert.Equal(t, "gateway", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)
}

func (s *GatewayIntegrationSuite) TestListQueuesAPI() {
	t := s.T()
	resp, err := s.client.ListQueues(s.ctx, &pb.ListQueuesRequest{})
	require.NoError(t, err)
	names := make([]string, 0, len(resp.GetQueues()))
	for _, queue := range resp.GetQueues() {
		names = append(names, queue.GetName())
	}
	assert.Equal(t, []string{
		"demo-queue",
		"e2e-cancel-queue",
		"e2e-chain-queue",
		"e2e-conflict-error-queue",
		"e2e-git-queue",
		"e2e-redelivery-queue",
		"e2e-respeculate-queue",
		"e2e-strand-queue",
		"e2e-test-queue",
		"file-overlap-queue",
		"test-queue",
	}, names)
}

// TestLandAPI tests the Gateway Land API with queue publishing
func (s *GatewayIntegrationSuite) TestLandAPI() {
	t := s.T()

	req := &pb.LandRequest{
		Queue:    "test-queue",
		Change:   &changepb.Change{Uris: []string{"github://github.example.com/uber/integration-test/pull/123/abcdef0123456789abcdef0123456789abcdef01"}},
		Strategy: mergestrategypb.Strategy_REBASE,
	}

	s.log.Logf("Sending Land request for queue=%s", req.Queue)
	resp, err := s.client.Land(s.ctx, req)
	require.NoError(t, err, "Land request failed")
	require.NotEmpty(t, resp.Sqid, "SQID should not be empty")

	s.log.Logf("Land request succeeded: sqid=%s", resp.Sqid)

	// Verify message published to queue
	var msgCount int
	err = s.queueDB.QueryRow("SELECT COUNT(*) FROM queue_messages WHERE tenant = ? AND id = ?", req.Queue, resp.Sqid).Scan(&msgCount)
	require.NoError(t, err, "failed to query queue messages")
	assert.Equal(t, 1, msgCount, "should have 1 message in queue")

	summary, err := s.client.GetRequestSummaryByID(s.ctx, &pb.GetRequestSummaryByIDRequest{Sqid: resp.Sqid, Queue: req.Queue})
	require.NoError(t, err)
	listed, err := s.client.List(s.ctx, &pb.ListRequest{
		Queue: req.Queue, ReceivedAtOrAfterMs: summary.Request.ReceivedAtMs, ReceivedBeforeMs: summary.Request.ReceivedAtMs + 1,
	})
	require.NoError(t, err)
	assert.Contains(t, listed.Requests, summary.Request)
}

func (s *GatewayIntegrationSuite) TestListAPI() {
	t := s.T()
	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err)
	materializer := corerequest.NewMaterializer(mysqlFactory{backend: store})
	queueStore, err := store.For("test-queue")
	require.NoError(t, err)
	for _, summary := range []entity.RequestSummary{
		{RequestID: "901", Queue: "test-queue", ChangeURIs: []string{"uri/1"}, ReceivedAtMs: 100, Status: entity.RequestStatusAccepted, StatusTimestampMs: 100, Version: 1, Metadata: map[string]string{}},
		{RequestID: "902", Queue: "test-queue", ChangeURIs: []string{"uri/2"}, ReceivedAtMs: 200, Status: entity.RequestStatusAccepted, StatusTimestampMs: 200, Version: 1, Metadata: map[string]string{}},
		{RequestID: "903", Queue: "test-queue", ChangeURIs: []string{"uri/3"}, ReceivedAtMs: 200, Status: entity.RequestStatusLanded, StatusTimestampMs: 200, Version: 1, Metadata: map[string]string{}},
	} {
		publicStatus := summary.Status
		summary.Status = entity.RequestStatusAccepting
		require.NoError(t, queueStore.GetRequestSummaryStore().Create(s.ctx, summary))
		require.NoError(t, materializer.PersistLog(s.ctx, entity.RequestLog{
			RequestID:   summary.RequestID,
			Queue:       summary.Queue,
			TimestampMs: summary.StatusTimestampMs,
			Type:        entity.RequestLogTypeStatus,
			Status:      publicStatus,
			Metadata:    map[string]string{},
		}))
	}
	oldOnly := entity.RequestSummary{RequestID: "905", Queue: "test-queue", ReceivedAtMs: 150, Status: entity.RequestStatusAccepted, Version: 1}
	require.NoError(t, queueStore.GetRequestSummaryStore().Create(s.ctx, oldOnly))
	require.NoError(t, queueStore.GetRequestQueueSummaryStore().Create(s.ctx, entity.RequestQueueSummary{
		RequestID: oldOnly.RequestID, Queue: oldOnly.Queue, ReceivedAtMs: oldOnly.ReceivedAtMs, Status: oldOnly.Status, Version: oldOnly.Version,
	}))
	require.NoError(t, queueStore.GetRequestSummaryStore().Create(s.ctx, entity.RequestSummary{
		RequestID: "904", Queue: "test-queue", ReceivedAtMs: 180, Status: entity.RequestStatusAccepting, Version: 1,
	}))

	resp, err := s.client.List(s.ctx, &pb.ListRequest{Queue: "test-queue", ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, resp.Requests, 1)
	assert.Equal(t, "903", resp.Requests[0].Sqid)
	assert.Equal(t, string(entity.RequestStatusLanded), resp.Requests[0].Status)
	require.NotEmpty(t, resp.NextPageToken)

	// Simulate a newer authoritative write before the legacy projection catches up.
	current, err := queueStore.GetRequestSummaryStore().Get(s.ctx, "902")
	require.NoError(t, err)
	updated := current
	updated.Status = entity.RequestStatusError
	updated.StatusTimestampMs = 300
	updated.LastError = "build failed"
	updated.Metadata = map[string]string{"build": "url"}
	require.NoError(t, queueStore.GetRequestSummaryStore().Update(s.ctx, updated, current.Version, current.Version+1))
	legacy, err := queueStore.GetRequestQueueSummaryStore().Get(s.ctx, 200, "902")
	require.NoError(t, err)
	assert.Equal(t, entity.RequestStatusAccepted, legacy.Status)

	resp, err = s.client.List(s.ctx, &pb.ListRequest{Queue: "test-queue", ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, PageSize: 1, PageToken: resp.NextPageToken})
	require.NoError(t, err)
	require.Len(t, resp.Requests, 1)
	assert.Equal(t, "902", resp.Requests[0].Sqid)
	summary, err := s.client.GetRequestSummaryByID(s.ctx, &pb.GetRequestSummaryByIDRequest{Sqid: "902", Queue: "test-queue"})
	require.NoError(t, err)
	assert.Equal(t, summary.Request, resp.Requests[0])
	assert.Equal(t, string(entity.RequestStatusError), resp.Requests[0].Status)
	assert.Equal(t, "build failed", resp.Requests[0].LastError)
	assert.Equal(t, map[string]string{"build": "url"}, resp.Requests[0].Metadata)
	require.NotEmpty(t, resp.NextPageToken)

	resp, err = s.client.List(s.ctx, &pb.ListRequest{Queue: "test-queue", ReceivedAtOrAfterMs: 50, ReceivedBeforeMs: 250, PageSize: 1, PageToken: resp.NextPageToken})
	require.NoError(t, err)
	require.Len(t, resp.Requests, 1)
	assert.Equal(t, "901", resp.Requests[0].Sqid)
	assert.Equal(t, string(entity.RequestStatusAccepted), resp.Requests[0].Status)
	assert.Empty(t, resp.NextPageToken)

	resp, err = s.client.List(s.ctx, &pb.ListRequest{Queue: "test-queue", ReceivedAtOrAfterMs: 100, ReceivedBeforeMs: 200})
	require.NoError(t, err)
	require.Len(t, resp.Requests, 1)
	assert.Equal(t, "901", resp.Requests[0].Sqid)
}

// TestReadAPIErrorCodes verifies controller error classes reach stable gRPC codes.
func (s *GatewayIntegrationSuite) TestReadAPIErrorCodes() {
	t := s.T()

	_, err := s.client.GetRequestSummaryByID(s.ctx, &pb.GetRequestSummaryByIDRequest{Sqid: "1", Queue: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = s.client.List(s.ctx, &pb.ListRequest{
		Queue:               "missing-queue",
		ReceivedAtOrAfterMs: 1,
		ReceivedBeforeMs:    2,
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err)
	const overflowChangeURI = "uri/read-api-overflow"
	overflowStore, err := store.For("overflow")
	require.NoError(t, err)
	for i := 1; i <= 101; i++ {
		require.NoError(t, overflowStore.GetRequestURIStore().Create(s.ctx, entity.RequestURI{
			ChangeURI:    overflowChangeURI,
			Queue:        "overflow",
			ReceivedAtMs: int64(i),
			RequestID:    fmt.Sprintf("%d", i),
		}))
	}

	_, err = s.client.GetRequestSummaryByChangeURI(s.ctx, &pb.GetRequestSummaryByChangeURIRequest{ChangeUri: overflowChangeURI, Queue: "overflow"})
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	_, err = s.client.GetRequestHistoryByChangeURI(s.ctx, &pb.GetRequestHistoryByChangeURIRequest{ChangeUri: overflowChangeURI, Queue: "overflow"})
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	const inconsistentChangeURI = "uri/read-api-inconsistent"
	inconsistentStore, err := store.For("missing-summary")
	require.NoError(t, err)
	require.NoError(t, inconsistentStore.GetRequestURIStore().Create(s.ctx, entity.RequestURI{
		ChangeURI:    inconsistentChangeURI,
		Queue:        "missing-summary",
		ReceivedAtMs: 1,
		RequestID:    "1",
	}))
	_, err = s.client.GetRequestSummaryByChangeURI(s.ctx, &pb.GetRequestSummaryByChangeURIRequest{ChangeUri: inconsistentChangeURI, Queue: "missing-summary"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))

	receiptStore, err := store.For("test-queue")
	require.NoError(t, err)
	require.NoError(t, receiptStore.GetRequestReceiptStore().Create(s.ctx, entity.RequestReceipt{
		Queue: "test-queue", RequestID: "999", ReceivedAtMs: 500,
	}))
	_, err = s.client.List(s.ctx, &pb.ListRequest{Queue: "test-queue", ReceivedAtOrAfterMs: 500, ReceivedBeforeMs: 501})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// TestRequestLogConsumer verifies the gateway's log-topic consumer in isolation:
// no orchestrator runs in this stack, so the test itself publishes a request log
// entry to the log topic exactly as the orchestrator does in production (via
// submitqueue/orchestrator/core/request.PublishLog). The gateway is the sole writer of the
// request log; this asserts its consumer drains the log topic and persists the
// entry to storage, observable through the request-summary RPC.
func (s *GatewayIntegrationSuite) TestRequestLogConsumer() {
	t := s.T()
	const sqid = "900"
	const logQueue = "test-queue"

	// Build a publisher against the shared queue database. NewQueue only wires up
	// stores; nothing consumes until a subscriber is started, so this publish-only
	// use does not interfere with the gateway container's consumer.
	queue, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           s.queueDB,
		Logger:       zap.NewNop(),
		MetricsScope: tally.NoopScope,
		Tenants:      []string{logQueue},
	})
	require.NoError(t, err, "failed to create queue publisher")
	defer queue.Close()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue},
	})
	require.NoError(t, err, "failed to create topic registry")

	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err)
	logQueueStore, err := store.For(logQueue)
	require.NoError(t, err)
	summary := entity.RequestSummary{
		RequestID: sqid, Queue: logQueue, ChangeURIs: []string{}, ReceivedAtMs: 1,
		Status: entity.RequestStatusAccepting, StatusTimestampMs: 1, Version: 1, Metadata: map[string]string{},
	}
	require.NoError(t, logQueueStore.GetRequestSummaryStore().Create(s.ctx, summary))
	logEntry := entity.NewRequestStatusLog(logQueue, sqid, entity.RequestStatusStarted, 1, "", nil)
	require.NoError(t, orchrequest.PublishLog(entityqueue.WithQueueName(s.ctx, logQueue), registry, logEntry, sqid, ""),
		"failed to publish request log to log topic")

	s.log.Logf("Published 'started' log for sqid=%s; waiting for gateway consumer to persist it", sqid)

	ticker := time.NewTicker(persistPollInterval)
	defer ticker.Stop()
	for {
		resp, statusErr := s.client.GetRequestSummaryByID(s.ctx, &pb.GetRequestSummaryByIDRequest{Sqid: sqid, Queue: logQueue})
		if statusErr != nil {
			require.Equal(t, codes.NotFound, status.Code(statusErr), "unexpected request-summary lookup error: %v", statusErr)
		}
		if statusErr == nil && resp.Request != nil && resp.Request.Status == string(entity.RequestStatusStarted) {
			break
		}
		<-ticker.C
	}

	s.log.Logf("Request log consumer test passed: entry persisted and readable via GetRequestSummaryByID")
}

// mysqlFactory adapts the MySQL storage backend's queue binding to the
// storage.Factory seam, mirroring the host wiring.
type mysqlFactory struct {
	backend *mysqlstorage.Storage
}

// For returns the queue-scoped store aggregate bound to the queue named in config.
func (f mysqlFactory) For(config storage.Config) (storage.Storage, error) {
	return f.backend.For(config.QueueName)
}
