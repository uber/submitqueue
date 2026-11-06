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
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	pb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
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

	// Schemas and retained work must exist before the service starts.
	err := s.stack.Up("mysql-app", "mysql-queue")
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

	requestStore := mysqlstorage.NewRequestStore(s.db, tally.NoopScope, "test-queue")
	_, err = requestStore.Get(s.ctx, "911")
	require.ErrorIs(t, err, storage.ErrNotFound)
	queue, err := queueMySQL.NewQueue(queueMySQL.Params{DB: s.queueDB, Logger: zap.NewNop(), MetricsScope: tally.NoopScope})
	require.NoError(t, err)
	payload, err := sqmq.Marshal(sqmq.StartFromLandRequest(retainedStartRequest()))
	require.NoError(t, err)
	msg := entityqueue.NewMessage("retained-start", payload, "test-queue", nil)
	msg.Tenant = "test-queue"
	require.NoError(t, queue.Publisher().Publish(s.ctx, "start_dlq", msg))
	require.NoError(t, queue.Close())
	require.NoError(t, s.stack.Up("orchestrator-service"))

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

func retainedStartRequest() entity.LandRequest {
	return entity.LandRequest{ID: "911", Queue: "test-queue", Change: change.Change{URIs: []string{"github://github.example.com/org/repo/pull/11/abcdef0123456789abcdef0123456789abcdef01"}}, LandStrategy: mergestrategy.MergeStrategyRebase}
}

func (s *OrchestratorIntegrationSuite) TestRetainedStartCreatesRequestAndHandsOffToValidate() {
	t := s.T()
	landRequest := retainedStartRequest()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	// Watermarks survive GC; complete both consumers before removing their transient evidence.
	for {
		var completed int
		err := s.queueDB.QueryRowContext(s.ctx, `SELECT COUNT(*) FROM queue_offsets
			WHERE tenant = ? AND partition_key = ? AND offset_acked > 0
			AND ((topic = 'start_dlq' AND consumer_group = 'orchestrator-dlq')
			OR (topic = 'validate' AND consumer_group = 'orchestrator'))`, landRequest.Queue, landRequest.Queue).Scan(&completed)
		require.NoError(t, err)
		if completed == 2 {
			break
		}
		<-ticker.C
	}
	for _, table := range []string{"queue_delivery_state", "queue_messages"} {
		_, err := s.queueDB.ExecContext(s.ctx, "DELETE FROM "+table+" WHERE tenant = ? AND partition_key = ? AND topic IN ('start_dlq', 'validate')", landRequest.Queue, landRequest.Queue)
		require.NoError(t, err)
	}
	request, err := mysqlstorage.NewRequestStore(s.db, tally.NoopScope, landRequest.Queue).Get(s.ctx, landRequest.ID)
	require.NoError(t, err)
	assert.Equal(t, landRequest.ID, request.ID)
	assert.Equal(t, landRequest.Queue, request.Queue)
	assert.Equal(t, landRequest.Change, request.Change)
	assert.Equal(t, landRequest.LandStrategy, request.LandStrategy)
	var payload []byte
	// This fixture has no Runway consumer, so the cross-service hand-off remains observable.
	require.NoError(t, s.queueDB.QueryRowContext(s.ctx, `SELECT payload FROM queue_messages WHERE tenant = ? AND topic = 'merge-conflict-check' AND id = ?`, landRequest.Queue, landRequest.ID).Scan(&payload))
	check := &runwaymq.MergeRequest{}
	require.NoError(t, runwaymq.Unmarshal(payload, check))
	assert.Equal(t, request.ID, check.GetId())
	assert.Equal(t, request.Queue, check.GetQueueName())
	require.Len(t, check.GetSteps(), 1)
	assert.Equal(t, request.ID, check.GetSteps()[0].GetStepId())
	assert.Equal(t, request.Change.URIs, check.GetSteps()[0].GetChange().GetUris())
	assert.Equal(t, mergestrategypb.Strategy_REBASE, check.GetSteps()[0].GetStrategy())
}
