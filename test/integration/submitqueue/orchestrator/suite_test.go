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

package orchestrator

// Orchestrator Integration Tests
//
// These tests use docker-compose from service/submitqueue/orchestrator/server/docker-compose.yml.
// They are hermetic: the orchestrator image is built from a staged context
// whose inputs (Bazel-built Linux binary, Dockerfile) are all declared data
// dependencies of the test target.
//
// Run with:
//   make integration-test-submitqueue-orchestrator

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	consumergatenoop "github.com/uber/submitqueue/platform/extension/consumergate/noop"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	"github.com/uber/submitqueue/platform/pipeline"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"github.com/uber/submitqueue/submitqueue/orchestrator"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mysql"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type OrchestratorIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.SubmitQueueOrchestratorClient
	db      *sql.DB // App database
	queueDB *sql.DB // Queue database
}

func TestOrchestratorIntegration(t *testing.T) {
	suite.Run(t, new(OrchestratorIntegrationSuite))
}

func (s *OrchestratorIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Orchestrator integration test suite using docker-compose")

	// Use docker-compose from service/submitqueue/orchestrator/server,
	// resolved from the test runfiles. The orchestrator image is built from a
	// staged build context assembled entirely from declared data dependencies.
	composeFile := testutil.Runfile("service/submitqueue/orchestrator/server/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-submitqueue-orchestrator",
		testutil.WithBuildContext(map[string]string{
			".docker-bin/orchestrator":                           "service/submitqueue/orchestrator/server/orchestrator_linux",
			"service/submitqueue/orchestrator/server/Dockerfile": "service/submitqueue/orchestrator/server/Dockerfile",
		}))

	// Start the compose stack (Orchestrator + 2 MySQL DBs)
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
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/orchestrator/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))

	// Apply schemas programmatically to queue database
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	s.log.Logf("Schemas applied successfully")

	// Connect to Orchestrator gRPC service
	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("orchestrator-service", 8080)
	require.NoError(t, err, "failed to connect to orchestrator")
	s.client = pb.NewSubmitQueueOrchestratorClient(conn)

	s.log.Logf("Orchestrator integration test suite ready")
}

func (s *OrchestratorIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Orchestrator integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// TestPingAPI tests the Orchestrator Ping API
func (s *OrchestratorIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Orchestrator Ping failed")
	assert.Equal(t, "orchestrator", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)
}

// No validate subscriber runs on these topics: the test controls the pre-enrollment window, not a race against Landing.
func (s *OrchestratorIntegrationSuite) TestCancelBeforeStartRetainsIntent() {
	for _, tc := range []struct {
		name   string
		id     string
		dlq    bool
		state  entity.RequestState
		status entity.RequestStatus
	}{
		{name: "primary", id: "912", state: entity.RequestStateCancelled, status: entity.RequestStatusCancelled},
		{name: "retained-dlq", id: "913", dlq: true, state: entity.RequestStateError, status: entity.RequestStatusError},
	} {
		s.T().Run(tc.name, func(t *testing.T) {
			queue, err := queueMySQL.NewQueue(queueMySQL.Params{DB: s.queueDB, Logger: zap.NewNop(), MetricsScope: tally.NoopScope, Tenants: []string{"test-queue"}})
			require.NoError(t, err)
			defer queue.Close()
			backend, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
			require.NoError(t, err)
			stores := cancellationStorageFactory{backend}
			subscription := extqueue.SingleMessageSubscriptionConfig("early-cancel", "early-cancel")
			subscription.Retry.MaxAttempts = 3
			cancelKey := topickey.TopicKeyCancel
			cancelSubscription := subscription
			if tc.dlq {
				cancelKey = "cancel_dlq"
				cancelSubscription = extqueue.DLQSubscriptionConfig("early-cancel", "early-cancel")
			}
			cancelTopic := "early-cancel-" + tc.name
			registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
				{Key: topickey.TopicKeyStart, Name: "early-start-" + tc.name, Queue: queue, Subscription: subscription},
				{Key: cancelKey, Name: cancelTopic, Queue: queue, Subscription: cancelSubscription},
				{Key: topickey.TopicKeyLog, Name: "early-log-" + tc.name, Queue: queue},
				{Key: topickey.TopicKeyValidate, Name: "early-validate-" + tc.name, Queue: queue},
			})
			require.NoError(t, err)
			deps := orchestrator.Deps{Logger: zap.NewNop().Sugar(), Scope: tally.NoopScope, Storage: stores}
			for _, stage := range orchestrator.Stages {
				if stage.Key != topickey.TopicKeyStart && stage.Key != topickey.TopicKeyCancel {
					continue
				}
				constructor, key := stage.New, stage.Key
				processor := errs.NewClassifierProcessor()
				if stage.Key == topickey.TopicKeyCancel && tc.dlq {
					constructor, key = stage.DLQ, cancelKey
					processor = errs.AlwaysRetryableProcessor
				}
				controller, err := constructor(deps, pipeline.StageContext{Registry: registry, TopicKey: key, ConsumerGroup: subscription.ConsumerGroup})
				require.NoError(t, err)
				worker := consumer.New(zap.NewNop().Sugar(), tally.NoopScope, registry, processor, consumergatenoop.New())
				require.NoError(t, worker.Register(controller))
				require.NoError(t, worker.Start(s.ctx))
				defer func() { require.NoError(t, worker.Stop(10000)) }()
			}
			cancelRequest := entity.CancelRequest{ID: tc.id, Queue: "test-queue", Reason: "cancel before admission"}
			payload, err := sqmq.Marshal(sqmq.CancelFromEntity(cancelRequest))
			require.NoError(t, err)
			msg := entityqueue.NewMessage("cancel-"+tc.id, payload, tc.id, nil)
			msg.Tenant = cancelRequest.Queue
			require.NoError(t, queue.Publisher().Publish(s.ctx, cancelTopic, msg))

			// Observe committed postponements beyond the finite primary budget before releasing start.
			s.waitForCancelPostponements(t, cancelTopic, subscription.Retry.MaxAttempts+1)
			requestStore := mysqlstorage.NewRequestStore(s.db, tally.NoopScope, cancelRequest.Queue)
			_, err = requestStore.Get(s.ctx, tc.id)
			require.ErrorIs(t, err, storage.ErrNotFound)
			var terminalCount int
			require.NoError(t, s.queueDB.QueryRowContext(s.ctx, "SELECT COUNT(*) FROM queue_messages WHERE topic IN (?, ?)", "early-log-"+tc.name, cancelTopic+"_dlq").Scan(&terminalCount))
			assert.Zero(t, terminalCount, "waiting must neither publish a terminal log nor spend the primary failure budget")

			landRequest := entity.LandRequest{ID: tc.id, Queue: "test-queue", Change: change.Change{URIs: []string{"github://github.example.com/org/repo/pull/11/abcdef0123456789abcdef0123456789abcdef01"}}, LandStrategy: mergestrategy.MergeStrategyRebase}
			payload, err = sqmq.Marshal(sqmq.StartFromLandRequest(landRequest))
			require.NoError(t, err)
			msg = entityqueue.NewMessage("start-"+tc.id, payload, landRequest.Queue, nil)
			msg.Tenant = landRequest.Queue
			require.NoError(t, queue.Publisher().Publish(s.ctx, "early-start-"+tc.name, msg))
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			var terminalLog entity.RequestLog
			for terminalLog.Status != tc.status {
				rows, err := s.queueDB.QueryContext(s.ctx, "SELECT payload FROM queue_messages WHERE topic = ?", "early-log-"+tc.name)
				require.NoError(t, err)
				for rows.Next() {
					var payload []byte
					require.NoError(t, rows.Scan(&payload))
					entry, err := sqmq.UnmarshalRequestLog(payload)
					require.NoError(t, err)
					if entry.Status == tc.status {
						terminalLog = entry
					}
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				<-ticker.C
			}
			if !tc.dlq {
				assert.Equal(t, cancelRequest.Reason, terminalLog.Metadata["reason"])
			}
			request, err := requestStore.Get(s.ctx, tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.state, request.State)
		})
	}
}

type cancellationStorageFactory struct{ backend *mysqlstorage.Storage }

func (f cancellationStorageFactory) For(config orchstorage.Config) (orchstorage.Storage, error) {
	return f.backend.For(config.QueueName)
}

func (s *OrchestratorIntegrationSuite) waitForCancelPostponements(t *testing.T, topic string, count int) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var previousDelay int64
	for observed := 0; observed < count; {
		var acked, postponed bool
		var retryCount int
		var invisibleUntil int64
		err := s.queueDB.QueryRowContext(s.ctx, `SELECT acked, postponed, retry_count, invisible_until FROM queue_delivery_state
			WHERE topic = ? AND consumer_group = 'early-cancel'`, topic).Scan(&acked, &postponed, &retryCount, &invisibleUntil)
		if err != sql.ErrNoRows {
			require.NoError(t, err)
			require.False(t, acked, "the missing request must retain its original delivery")
			if postponed && invisibleUntil > previousDelay {
				assert.Zero(t, retryCount)
				previousDelay = invisibleUntil
				observed++
			}
		}
		<-ticker.C
	}
}
