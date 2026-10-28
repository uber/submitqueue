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

package stovepipe

// Stovepipe integration tests
//
// These tests use compose from service/stovepipe/docker-compose.yml. They are
// hermetic: the stovepipe image is built from a staged context whose inputs
// (Bazel-built Linux binary, Dockerfile) are all declared data dependencies of
// the test target. The stack runs the Stovepipe gRPC service plus a storage
// MySQL (request, request_uri) and a queue MySQL (process stage).
//
// Run with:
//   make integration-test
// or only this package:
//   bazel test //test/integration/stovepipe:stovepipe_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	"github.com/uber/submitqueue/test/testutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type StovepipeIntegrationSuite struct {
	suite.Suite
	ctx     context.Context
	log     *testutil.TestLogger
	stack   *testutil.ComposeStack
	client  pb.StovepipeClient
	db      *sql.DB // storage database (request, request_uri)
	queueDB *sql.DB // queue database
}

func TestStovepipeIntegration(t *testing.T) {
	suite.Run(t, new(StovepipeIntegrationSuite))
}

func (s *StovepipeIntegrationSuite) SetupSuite() {
	t := s.T()
	s.ctx = context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting Stovepipe integration test suite using compose")

	// Compose file and image build inputs come from the test runfiles; the
	// stovepipe image is built from a staged build context assembled entirely
	// from declared data dependencies.
	composeFile := testutil.Runfile("service/stovepipe/docker-compose.yml")
	s.stack = testutil.NewComposeStack(t, s.log, s.ctx, composeFile, "svc-stovepipe",
		testutil.WithBuildContext(map[string]string{
			".docker-bin/stovepipe":               "service/stovepipe/server/stovepipe_linux",
			"service/stovepipe/server/Dockerfile": "service/stovepipe/server/Dockerfile",
		}))

	err := s.stack.Up("mysql-app", "mysql-queue")
	require.NoError(t, err, "failed to start compose stack")

	s.db, err = s.stack.ConnectMySQLService("mysql-app")
	require.NoError(t, err, "failed to connect to storage MySQL")

	s.queueDB, err = s.stack.ConnectMySQLService("mysql-queue")
	require.NoError(t, err, "failed to connect to queue MySQL")

	// Queue enablement is registered at service startup, so its schema must exist first.
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("platform/extension/counter/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("stovepipe/extension/storage/mysql/schema"))
	testutil.ApplySchema(t, s.log, s.queueDB, testutil.SchemaDir("platform/extension/messagequeue/mysql/schema"))

	err = s.stack.Up()
	require.NoError(t, err, "failed to start service after applying schemas")

	var conn *grpc.ClientConn
	conn, err = s.stack.ConnectGRPC("stovepipe-service", 8080)
	require.NoError(t, err, "failed to connect to stovepipe service")
	s.client = pb.NewStovepipeClient(conn)
}

func (s *StovepipeIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down Stovepipe integration test suite")
}

func (s *StovepipeIntegrationSuite) TestPingAPI() {
	t := s.T()

	resp, err := s.client.Ping(s.ctx, &pb.PingRequest{Message: "integration test"})
	require.NoError(t, err, "Stovepipe Ping failed")
	assert.Equal(t, "stovepipe", resp.ServiceName)
	assert.NotEmpty(t, resp.Message)
	assert.NotZero(t, resp.Timestamp)
}

// TestIngestAPI exercises the full ingest path: the controller resolves the head
// URI via the (fake) SourceControl, persists the Request and its (queue, URI)
// mapping, and publishes the request id to the process stage. A second ingest of
// the same queue resolves the same head and dedups to the same id.
func (s *StovepipeIntegrationSuite) TestIngestAPI() {
	t := s.T()

	const queue = "monorepo/main"

	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue, Enablement: pb.QueueEnablement_QUEUE_ENABLEMENT_ENABLED})
	require.NoError(t, err, "Ingest failed")
	require.NotEmpty(t, resp.Id, "minted request id should not be empty")
	id := resp.Id

	// Request persisted.
	var reqCount int
	require.NoError(t, s.db.QueryRow("SELECT COUNT(*) FROM request WHERE queue = ? AND id = ?", queue, id).Scan(&reqCount))
	assert.Equal(t, 1, reqCount, "request row should be persisted")

	// (queue, URI) mapping persisted and points at the minted id.
	var mappedID string
	require.NoError(t, s.db.QueryRow("SELECT request_id FROM request_uri WHERE queue = ?", queue).Scan(&mappedID))
	assert.Equal(t, id, mappedID, "URI mapping should point at the minted request id")

	// Message published to the process topic.
	var msgCount int
	require.NoError(t, s.queueDB.QueryRow("SELECT COUNT(*) FROM queue_messages WHERE tenant = ? AND topic = ? AND id = ?", queue, "process", id).Scan(&msgCount))
	assert.Equal(t, 1, msgCount, "should have published one process message")

	// Re-ingesting the same queue resolves the same head URI and dedups.
	resp2, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err, "second Ingest failed")
	assert.Equal(t, id, resp2.Id, "re-ingest of the same head should dedup to the same id")

	// latest_request_id on the queue row.
	var latestRequestID string
	require.NoError(t, s.db.QueryRow("SELECT latest_request_id FROM queue WHERE name = ?", queue).Scan(&latestRequestID))
	assert.Equal(t, id, latestRequestID, "queue latest_request_id should point at the minted request")
}

// TestIngestEmptyQueue verifies the request-validation error surfaces over gRPC.
func (s *StovepipeIntegrationSuite) TestIngestEmptyQueue() {
	t := s.T()

	_, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: ""})
	require.Error(t, err, "Ingest with empty queue should fail")
}

func (s *StovepipeIntegrationSuite) TestRequestHistoryAPIs() {
	t := s.T()
	const (
		queue     = "history-api/main"
		requestID = "7"
		uri       = "git://history-api/main/abc123"
	)

	_, err := s.db.Exec(
		"INSERT INTO request_uri (queue, uri, request_id, version) VALUES (?, ?, ?, ?)",
		queue, uri, requestID, 1,
	)
	require.NoError(t, err)
	_, err = s.db.Exec(
		`INSERT INTO request_log
			(queue, request_id, log_id, timestamp_ms, state, event, request_version, outcome_reason, metadata)
		 VALUES
			(?, ?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?, ?),
			(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		queue, requestID, "occurrence/001", 1000, "accepted", "", 1, "", `{}`,
		queue, requestID, "occurrence/002", 2000, "", "build_triggered", 0, "", `{}`,
		queue, requestID, "occurrence/003", 2000, "succeeded", "", 3, "build_succeeded", `{}`,
	)
	require.NoError(t, err)

	var requestRows int
	require.NoError(t, s.db.QueryRow("SELECT COUNT(*) FROM request WHERE queue = ? AND id = ?", queue, requestID).Scan(&requestRows))
	require.Zero(t, requestRows)

	byID, err := s.client.GetRequestHistoryByID(s.ctx, &pb.GetRequestHistoryByIDRequest{Queue: queue, RequestId: requestID})
	require.Error(t, err)
	assert.Nil(t, byID)

	byURI, err := s.client.GetRequestHistoryByURI(s.ctx, &pb.GetRequestHistoryByURIRequest{Queue: queue, Uri: uri})
	require.Error(t, err)
	assert.Nil(t, byURI)

	const baseURI = "git://history-api/main/base"
	_, err = s.db.Exec(`INSERT INTO request_summary
		(queue, request_id, uri, base_uri, state, request_version, state_timestamp_ms, version, accepted_at_ms, outcome_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		queue, requestID, uri, baseURI, "succeeded", 3, 2000, 1, 1000, "build_succeeded")
	require.NoError(t, err)
	byID, err = s.client.GetRequestHistoryByID(s.ctx, &pb.GetRequestHistoryByIDRequest{Queue: queue, RequestId: requestID})
	require.NoError(t, err)
	byURI, err = s.client.GetRequestHistoryByURI(s.ctx, &pb.GetRequestHistoryByURIRequest{Queue: queue, Uri: uri})
	require.NoError(t, err)
	require.Len(t, byURI.Histories, 1)
	wantSummary := &pb.RequestSummary{
		RequestId: requestID, Queue: queue, ChangeUri: uri, BaseUri: baseURI, RequestState: "succeeded",
		StateUpdatedAtMs: 2000, AcceptedAtMs: 1000, OutcomeReason: "build_succeeded",
	}
	assert.True(t, proto.Equal(wantSummary, byID.RequestSummary))
	assert.True(t, proto.Equal(wantSummary, byURI.Histories[0].RequestSummary))
	assert.Equal(t, requestID, byURI.Histories[0].RequestId)
	assertHistoryEvents(t, byID.Events)
	assertHistoryEvents(t, byURI.Histories[0].Events)
}

func (s *StovepipeIntegrationSuite) TestRequestHistoryAbsence() {
	t := s.T()
	const (
		queue      = "history-api-absence/main"
		mappedID   = "1"
		mappedURI  = "git://history-api-absence/main/mapped"
		scopedID   = "2"
		scopedURI  = "git://history-api-absence/main/scoped"
		missingID  = "999"
		missingURI = "git://history-api-absence/main/missing"
		wrongQueue = "history-api-absence/other"
	)

	_, err := s.db.Exec(
		"INSERT INTO request_uri (queue, uri, request_id, version) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
		queue, mappedURI, mappedID, 1, queue, scopedURI, scopedID, 1,
	)
	require.NoError(t, err)
	_, err = s.db.Exec(
		`INSERT INTO request_log
			(queue, request_id, log_id, timestamp_ms, state, event, request_version, outcome_reason, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		queue, scopedID, "occurrence/001", 1000, "accepted", "", 1, "", `{}`,
	)
	require.NoError(t, err)

	_, err = s.db.Exec(`INSERT INTO request_summary
		(queue, request_id, uri, base_uri, state, request_version, state_timestamp_ms, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, queue, missingID, missingURI, "", "accepted", 1, 1000, 1)
	require.NoError(t, err)

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "missing request ID",
			call: func() error {
				_, err := s.client.GetRequestHistoryByID(s.ctx, &pb.GetRequestHistoryByIDRequest{Queue: queue, RequestId: missingID})
				return err
			},
		},
		{
			name: "missing URI mapping",
			call: func() error {
				_, err := s.client.GetRequestHistoryByURI(s.ctx, &pb.GetRequestHistoryByURIRequest{Queue: queue, Uri: missingURI})
				return err
			},
		},
		{
			name: "mapped URI without retained logs",
			call: func() error {
				_, err := s.client.GetRequestHistoryByURI(s.ctx, &pb.GetRequestHistoryByURIRequest{Queue: queue, Uri: mappedURI})
				return err
			},
		},
		{
			name: "request ID in wrong queue",
			call: func() error {
				_, err := s.client.GetRequestHistoryByID(s.ctx, &pb.GetRequestHistoryByIDRequest{Queue: wrongQueue, RequestId: scopedID})
				return err
			},
		},
		{
			name: "URI mapping in wrong queue",
			call: func() error {
				_, err := s.client.GetRequestHistoryByURI(s.ctx, &pb.GetRequestHistoryByURIRequest{Queue: wrongQueue, Uri: scopedURI})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, tt.call())
		})
	}
}

func (s *StovepipeIntegrationSuite) TestProjectStatusAPI() {
	t := s.T()
	const (
		queue = "project-status-api/main"
		id    = "7"
		uri   = "git://project-status-api/main/commit"
	)
	_, err := s.db.Exec(`INSERT INTO request_summary
		(queue, request_id, uri, base_uri, state, request_version, state_timestamp_ms, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, queue, id, uri, "git://project-status-api/main/base", "succeeded", 3, 1000, 1)
	require.NoError(t, err)
	_, err = s.db.Exec("INSERT INTO request_uri (queue, uri, request_id, version) VALUES (?, ?, ?, ?)", queue, uri, id, 1)
	require.NoError(t, err)
	_, err = s.db.Exec(`INSERT INTO validation_fact (queue, uri, project, degree, request_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)`,
		queue, uri, "", 0.0, id, 2000, queue, uri, "project-a", 0.5, id, 3000)
	require.NoError(t, err)

	byURI, err := s.client.GetProjectStatusByURI(s.ctx, &pb.GetProjectStatusByURIRequest{
		Queue: queue, ChangeUri: uri, Projects: []string{"project-a", "missing"}, PageSize: 1,
	})
	require.NoError(t, err)
	byID, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
		Queue: queue, RequestId: id, Projects: []string{"project-a", "missing"}, PageSize: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, byURI.GetRequestId(), byID.GetRequestId())
	assert.Equal(t, queue, byID.GetQueue())
	assert.Equal(t, uri, byID.GetChangeUri())
	assert.Equal(t, byURI.GetBaseUri(), byID.GetBaseUri())
	assert.Equal(t, "succeeded", byID.GetRequestState())
	assert.Equal(t, int64(3000), byID.GetUpdatedAtMs())
	assert.IsType(t, &pb.GetProjectStatusByIDResponse_RepositoryBreakageDegree{}, byID.GetRepositoryResult())
	assert.Zero(t, byID.GetRepositoryBreakageDegree())
	require.Len(t, byID.Projects, 1)
	assert.Equal(t, "project-a", byID.Projects[0].GetProject())
	assert.Equal(t, 0.5, byID.Projects[0].GetBreakageDegree())
	assert.Equal(t, byURI.GetNextPageToken(), byID.GetNextPageToken())
	require.NotEmpty(t, byID.GetNextPageToken())
	lastPage, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
		Queue: queue, RequestId: id, Projects: []string{"project-a", "missing"}, PageSize: 1, PageToken: byURI.GetNextPageToken(),
	})
	require.NoError(t, err)
	assert.Empty(t, lastPage.Projects)
	assert.Empty(t, lastPage.NextPageToken)

	_, err = s.db.Exec(`INSERT INTO validation_fact (queue, uri, project, degree, request_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)`,
		queue, uri, "project-green", 0.0, id, 4000, queue, uri, "project-b", 1.0, id, 5000)
	require.NoError(t, err)
	indexStore := mysqlstorage.NewProjectFactByDegreeStore(s.db, tally.NoopScope, queue)
	for _, fact := range []entity.ValidationFact{
		{URI: uri, Project: "project-a", Degree: 0.5},
		{URI: uri, Project: "project-b", Degree: 1},
		{URI: uri, Project: "project-green", Degree: 0},
	} {
		entry, err := entity.NewProjectFactByDegree(fact)
		require.NoError(t, err)
		require.NoError(t, indexStore.Create(s.ctx, entry))
	}
	firstPage, err := s.client.GetProjectStatusByURI(s.ctx, &pb.GetProjectStatusByURIRequest{
		Queue: queue, ChangeUri: uri, PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, firstPage.Projects, 1)
	assert.Equal(t, "project-b", firstPage.Projects[0].GetProject())
	assert.Equal(t, 1.0, firstPage.Projects[0].GetBreakageDegree())
	assert.Equal(t, int64(5000), firstPage.GetUpdatedAtMs())
	require.NotEmpty(t, firstPage.NextPageToken)
	remaining, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
		Queue: queue, RequestId: id, PageToken: firstPage.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, remaining.Projects, 2)
	assert.Equal(t, "project-a", remaining.Projects[0].GetProject())
	assert.Equal(t, 0.5, remaining.Projects[0].GetBreakageDegree())
	assert.Equal(t, "project-green", remaining.Projects[1].GetProject())
	assert.Zero(t, remaining.Projects[1].GetBreakageDegree())
	assert.Equal(t, int64(4000), remaining.GetUpdatedAtMs())
	assert.Empty(t, remaining.NextPageToken)
	assert.False(t, remaining.ProjectResultsComplete)
	completion := requestlog.NewRequestEventLog(entity.Request{Queue: queue, ID: id}, entity.RequestEventProjectFactsRecorded, "all", map[string]string{requestlog.MetadataKeyProjectFactCount: "3", requestlog.MetadataKeyBrokenProjectFactCount: "2"})
	completion.TimestampMs = 6000
	backend, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err)
	stores, err := backend.For(queue)
	require.NoError(t, err)
	require.NoError(t, requestlog.NewMaterializer(tally.NoopScope).PersistLog(s.ctx, stores, completion))
	completed, err := s.client.GetProjectStatusByURI(s.ctx, &pb.GetProjectStatusByURIRequest{
		Queue: queue, ChangeUri: uri, PageSize: 1,
	})
	require.NoError(t, err)
	assert.True(t, completed.ProjectResultsComplete)
	assert.Equal(t, int64(6000), completed.UpdatedAtMs)
	assert.NotEmpty(t, completed.NextPageToken)
	completedRemainder, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
		Queue: queue, RequestId: id, PageToken: completed.NextPageToken,
	})
	require.NoError(t, err)
	assert.True(t, completedRemainder.ProjectResultsComplete)
	assert.Equal(t, int64(6000), completedRemainder.UpdatedAtMs)
	assert.Empty(t, completedRemainder.NextPageToken)

	for _, selector := range []struct{ queue, id string }{{queue, "999"}, {"project-status-api/other", id}} {
		_, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
			Queue: selector.queue, RequestId: selector.id, Projects: []string{"project-a"},
		})
		require.Error(t, err)
	}
}

func (s *StovepipeIntegrationSuite) TestUnindexedFactsRemainAvailableByExplicitProject() {
	t := s.T()
	const queue, id, uri = "fresh-index/main", "9", "git://fresh-index/main/older-commit"
	_, err := s.db.Exec(`INSERT INTO request_summary
		(queue, request_id, uri, base_uri, state, request_version, state_timestamp_ms, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, queue, id, uri, "", "succeeded", 1, 1000, 1)
	require.NoError(t, err)
	_, err = s.db.Exec(`INSERT INTO validation_fact (queue, uri, project, degree, request_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, queue, uri, "old-project", 0.5, id, 2000)
	require.NoError(t, err)

	listed, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
		Queue: queue, RequestId: id,
	})
	require.NoError(t, err)
	assert.Empty(t, listed.Projects)
	assert.Empty(t, listed.NextPageToken)

	explicit, err := s.client.GetProjectStatusByID(s.ctx, &pb.GetProjectStatusByIDRequest{
		Queue: queue, RequestId: id, Projects: []string{"old-project"},
	})
	require.NoError(t, err)
	require.Len(t, explicit.Projects, 1)
	assert.Equal(t, "old-project", explicit.Projects[0].GetProject())
	assert.Equal(t, 0.5, explicit.Projects[0].GetBreakageDegree())
}

func assertHistoryEvents(t *testing.T, events []*pb.HistoryEvent) {
	t.Helper()
	require.Len(t, events, 3)

	assert.Equal(t, "occurrence/001", events[0].EventId)
	assert.Equal(t, int64(1000), events[0].TimestampMs)
	assert.Equal(t, "accepted", events[0].GetRequestState())
	assert.Empty(t, events[0].GetEvent())

	assert.Equal(t, "occurrence/002", events[1].EventId)
	assert.Equal(t, int64(2000), events[1].TimestampMs)
	assert.Equal(t, "build_triggered", events[1].GetEvent())
	assert.Empty(t, events[1].GetRequestState())

	assert.Equal(t, "occurrence/003", events[2].EventId)
	assert.Equal(t, int64(2000), events[2].TimestampMs)
	assert.Equal(t, "succeeded", events[2].GetRequestState())
	assert.Equal(t, "build_succeeded", events[2].OutcomeReason)
}
