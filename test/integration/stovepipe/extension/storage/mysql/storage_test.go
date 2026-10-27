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

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
	storagesuite "github.com/uber/submitqueue/test/integration/stovepipe/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

func TestMySQLStorage(t *testing.T) {
	ctx := context.Background()
	log := testutil.NewTestLogger(t)
	stack := testutil.NewComposeStack(
		t,
		log,
		ctx,
		"docker-compose.yml",
		"ext-stovepipe-storage-mysql",
	)

	err := stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	db, err := stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	schemaDir := testutil.SchemaDir("stovepipe/extension/storage/mysql/schema")
	testutil.ApplySchema(t, log, db, schemaDir)

	backend, err := mysqlstorage.NewStorage(db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")
	factory := mysqlFactory{backend: backend}

	t.Run("QueueStateTransitions", func(t *testing.T) {
		resetStorage(t, db)
		testQueueStateTransitions(t, ctx, db, backend)
	})

	t.Run("QueueSummaryStores", func(t *testing.T) {
		resetStorage(t, db)
		testQueueSummaryStores(t, ctx, backend)
	})

	t.Run("QueueSummaryKeyIsolation", func(t *testing.T) {
		resetStorage(t, db)
		testQueueSummaryKeyIsolation(t, ctx, backend)
	})

	t.Run("RequestStore", func(t *testing.T) {
		resetStorage(t, db)
		bound, err := backend.For("monorepo/main")
		require.NoError(t, err)
		suite.Run(t, &MySQLRequestStoreSuite{
			ctx:      ctx,
			backend:  backend,
			db:       db,
			store:    bound.GetRequestStore(),
			uriStore: bound.GetRequestURIStore(),
		})
	})

	t.Run("QueueStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLQueueStoreSuite)
		testSuite.SetContext(ctx)
		testSuite.SetFactory(factory)
		testSuite.SetLogger(testutil.NewTestLogger(t))
		suite.Run(t, testSuite)
	})

	t.Run("BuildIssueStore", func(t *testing.T) {
		resetStorage(t, db)
		testBuildIssueStore(t, ctx, db, factory)
	})

	t.Run("RequestProjectsByIssue", func(t *testing.T) {
		resetStorage(t, db)
		testRequestProjectsByIssue(t, ctx, backend)
	})

	t.Run("RequestProjectsByIssueKeyLimits", func(t *testing.T) {
		resetStorage(t, db)
		testRequestProjectsByIssueKeyLimits(t, ctx, backend)
	})

	t.Run("RequestIssuesByProject", func(t *testing.T) {
		resetStorage(t, db)
		testRequestIssuesByProject(t, ctx, backend)
	})

	t.Run("RequestIssuesByProjectKeyLimits", func(t *testing.T) {
		resetStorage(t, db)
		testRequestIssuesByProjectKeyLimits(t, ctx, backend)
	})

	t.Run("BuildIssueList", func(t *testing.T) {
		resetStorage(t, db)
		testBuildIssueList(t, ctx, factory)
	})

	t.Run("BuildIssueUpdate", func(t *testing.T) {
		resetStorage(t, db)
		testBuildIssueUpdate(t, ctx, db, factory)
	})

	t.Run("IssueStateStore", func(t *testing.T) {
		resetStorage(t, db)
		testIssueStateStore(t, ctx, factory)
	})

	t.Run("IssueSummaryStore", func(t *testing.T) {
		resetStorage(t, db)
		testIssueSummaryStore(t, ctx, factory)
	})

	t.Run("OpenIssueStore", func(t *testing.T) {
		resetStorage(t, db)
		testOpenIssueStore(t, ctx, factory)
	})

	t.Run("OpenIssueMaterialization", func(t *testing.T) {
		resetStorage(t, db)
		testOpenIssueMaterialization(t, ctx, factory)
	})

	t.Run("IssueStateList", func(t *testing.T) {
		resetStorage(t, db)
		testIssueStateList(t, ctx, factory)
	})

	t.Run("IssueTrackingStore", func(t *testing.T) {
		resetStorage(t, db)
		testIssueTrackingStore(t, ctx, factory)
	})

	t.Run("RequestIssueSummaryStore", func(t *testing.T) {
		testRequestIssueSummaryStore(t, ctx, factory)
	})

	t.Run("RequestIssueSummaryMaterializer", func(t *testing.T) {
		testRequestIssueSummaryMaterializer(t, ctx, db, factory)
	})

	t.Run("ReportedIssueLog", func(t *testing.T) {
		testReportedIssueLog(t, ctx, factory)
	})

	t.Run("IssueLogStore", func(t *testing.T) {
		resetStorage(t, db)
		testIssueLogStore(t, ctx, factory)
	})

	t.Run("BuildStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLBuildStoreSuite)
		testSuite.SetContext(ctx)
		bound, err := backend.For("contract/queue")
		require.NoError(t, err)
		testSuite.SetBuildStore(bound.GetBuildStore())
		testSuite.SetLogger(testutil.NewTestLogger(t))
		suite.Run(t, testSuite)
	})

	t.Run("ValidationFactStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLValidationFactStoreSuite)
		testSuite.SetContext(ctx)
		testSuite.SetFactory(factory)
		testSuite.SetLogger(testutil.NewTestLogger(t))
		suite.Run(t, testSuite)
	})

	t.Run("ProjectFactByDegree", func(t *testing.T) {
		resetStorage(t, db)
		testProjectFactByDegree(t, ctx, backend)
	})

	t.Run("RequestLogStore", func(t *testing.T) {
		resetStorage(t, db)
		testSuite := new(MySQLRequestLogStoreSuite)
		testSuite.SetContext(ctx)
		testSuite.SetFactory(factory)
		suite.Run(t, testSuite)
	})

	t.Run("RequestSummaryStore", func(t *testing.T) {
		resetStorage(t, db)
		bound, err := backend.For("monorepo/main")
		require.NoError(t, err)
		suite.Run(t, &MySQLRequestSummaryStoreSuite{
			ctx:   ctx,
			store: bound.GetRequestSummaryStore(),
		})
	})

	t.Run("RequestAcceptanceStore", func(t *testing.T) {
		resetStorage(t, db)
		testRequestAcceptanceStore(t, ctx, factory)
	})

	t.Run("RequestAcceptanceProjection", func(t *testing.T) {
		resetStorage(t, db)
		testRequestAcceptanceProjection(t, ctx, factory)
	})
}

func resetStorage(t *testing.T, db *sql.DB) {
	t.Helper()

	for _, statement := range []string{
		"TRUNCATE TABLE issue_state",
		"TRUNCATE TABLE issue_summary",
		"TRUNCATE TABLE open_issue",
		"TRUNCATE TABLE issue_log",
		"TRUNCATE TABLE issue_tracking",
		"TRUNCATE TABLE queue_log",
		"TRUNCATE TABLE queue_summary",
		"TRUNCATE TABLE request_log",
		"TRUNCATE TABLE request_summary",
		"TRUNCATE TABLE request_acceptance",
		"TRUNCATE TABLE request_uri",
		"TRUNCATE TABLE request",
		"TRUNCATE TABLE build",
		"TRUNCATE TABLE build_issue",
		"TRUNCATE TABLE queue",
		"TRUNCATE TABLE validation_fact",
		"TRUNCATE TABLE project_fact_by_degree",
		"TRUNCATE TABLE request_projects_by_issue",
		"TRUNCATE TABLE request_issues_by_project",
	} {
		_, err := db.ExecContext(context.Background(), statement)
		require.NoError(t, err)
	}
}

// MySQLRequestSummaryStoreSuite exercises summary projection storage against MySQL.
type MySQLRequestSummaryStoreSuite struct {
	suite.Suite
	ctx   context.Context
	store storage.RequestSummaryStore
}

func (s *MySQLRequestSummaryStoreSuite) TestCreateGetAndUpdate() {
	summary := entity.RequestSummary{
		RequestID: "1", Queue: "monorepo/main", URI: "git://repo/head",
		State: entity.RequestStateAccepted, RequestVersion: 1, StateTimestampMs: 1000, Version: 1,
		AcceptedAtMs: 1000,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, summary))

	got, err := s.store.Get(s.ctx, summary.RequestID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), summary, got)

	updated := summary
	updated.BaseURI = "git://repo/base"
	updated.State = entity.RequestStateFailed
	updated.OutcomeReason = entity.RequestOutcomeReasonProcessingFailed
	updated.RequestVersion = 2
	updated.StateTimestampMs = 2000
	updated.ProjectResultsCompletedAtMs = 2100
	updated.ProjectCount = 5
	updated.BrokenProjectCount = 2
	require.NoError(s.T(), s.store.Update(s.ctx, updated, 1, 2))
	updated.Version = 2

	got, err = s.store.Get(s.ctx, summary.RequestID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), updated, got)
	require.ErrorIs(s.T(), s.store.Update(s.ctx, updated, 1, 2), storage.ErrVersionMismatch)
}

func (s *MySQLRequestSummaryStoreSuite) TestGetNotFound() {
	_, err := s.store.Get(s.ctx, "999")
	require.True(s.T(), storage.IsNotFound(err))
}

// MySQLRequestStoreSuite exercises the MySQL-backed RequestStore against a real MySQL instance.
type MySQLRequestStoreSuite struct {
	suite.Suite
	ctx      context.Context
	backend  *mysqlstorage.Storage
	db       *sql.DB
	store    storage.RequestStore
	uriStore storage.RequestURIStore
}

func (s *MySQLRequestStoreSuite) TestCreateAndGet() {
	req := entity.Request{
		ID:      "1",
		Queue:   "monorepo/main",
		URI:     "git://remote/monorepo/main/aaaa1111",
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), req, got)
}

func (s *MySQLRequestStoreSuite) TestCreateAndGetWithProcessFields() {
	req := entity.Request{
		ID:            "3",
		Queue:         "monorepo/main",
		URI:           "git://remote/monorepo/main/cccc3333",
		State:         entity.RequestStateProcessing,
		BuildStrategy: entity.BuildStrategyIncrementalSinceGreen,
		BaseURI:       "git://remote/monorepo/main/green-bbbb",
		BuildID:       "build/3",
		Version:       2,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), req, got)
}

func (s *MySQLRequestStoreSuite) TestGetNotFound() {
	_, err := s.store.Get(s.ctx, "999")
	require.True(s.T(), storage.IsNotFound(err))
}

func (s *MySQLRequestStoreSuite) TestUpdateCAS() {
	req := entity.Request{
		ID:      "4",
		Queue:   "monorepo/main",
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	// Successful CAS: stored version (1) matches oldVersion; advance to processing with strategy.
	updated := req
	updated.URI = "git://remote/monorepo/main/resolved"
	updated.State = entity.RequestStateProcessing
	updated.BuildStrategy = entity.BuildStrategyFull
	updated.BuildID = "build/4"
	require.NoError(s.T(), s.store.Update(s.ctx, updated, 1, 2))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "git://remote/monorepo/main/resolved", got.URI)
	require.Equal(s.T(), entity.RequestStateProcessing, got.State)
	require.Equal(s.T(), entity.BuildStrategyFull, got.BuildStrategy)
	require.Equal(s.T(), "build/4", got.BuildID)
	require.Equal(s.T(), int32(2), got.Version)

	// Stale CAS: oldVersion 1 no longer matches the stored version (2).
	err = s.store.Update(s.ctx, updated, 1, 2)
	require.ErrorIs(s.T(), err, storage.ErrVersionMismatch)

	failed := updated
	failed.State = entity.RequestStateFailed
	failed.OutcomeReason = entity.RequestOutcomeReasonBuildPollingExhausted
	require.NoError(s.T(), s.store.Update(s.ctx, failed, 2, 3))
	failed.Version = 3

	got, err = s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), failed, got)
}

func (s *MySQLRequestStoreSuite) TestUpdateNotFoundIsVersionMismatch() {
	missing := entity.Request{ID: "999", Queue: "monorepo/main", State: entity.RequestStateAccepted}
	err := s.store.Update(s.ctx, missing, 1, 2)
	require.ErrorIs(s.T(), err, storage.ErrVersionMismatch)
}

func (s *MySQLRequestStoreSuite) TestCreateDuplicateID() {
	req := entity.Request{
		ID:      "2",
		Queue:   "monorepo/main",
		State:   entity.RequestStateAccepted,
		Version: 1,
	}
	require.NoError(s.T(), s.store.Create(s.ctx, req))

	err := s.store.Create(s.ctx, req)
	require.ErrorIs(s.T(), err, storage.ErrAlreadyExists)
}

func (s *MySQLRequestStoreSuite) TestURIMappingCreateAndGet() {
	const (
		queue = "monorepo/main"
		uri   = "git://remote/monorepo/main/bbbb2222"
		id    = "3"
	)
	require.NoError(s.T(), s.uriStore.Create(s.ctx, uri, id))

	got, err := s.uriStore.GetIDByURI(s.ctx, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), id, got)
}

func (s *MySQLRequestStoreSuite) TestGetIDByURINotFound() {
	_, err := s.uriStore.GetIDByURI(s.ctx, "git://remote/monorepo/main/unmapped")
	require.True(s.T(), storage.IsNotFound(err))
}

func (s *MySQLRequestStoreSuite) TestURIMappingDuplicate() {
	const (
		queue = "monorepo/main"
		uri   = "git://remote/monorepo/main/cccc3333"
	)
	require.NoError(s.T(), s.uriStore.Create(s.ctx, uri, "4"))

	// A second request claiming the same (queue, uri) is rejected — the dedup signal.
	err := s.uriStore.Create(s.ctx, uri, "5")
	require.ErrorIs(s.T(), err, storage.ErrAlreadyExists)
}

func (s *MySQLRequestStoreSuite) TestURIMappingDistinctAcrossQueues() {
	const uri = "git://remote/monorepo/shared/dddd4444"
	boundA, err := s.backend.For("queue-a")
	require.NoError(s.T(), err)
	boundB, err := s.backend.For("queue-b")
	require.NoError(s.T(), err)
	require.NoError(s.T(), boundA.GetRequestURIStore().Create(s.ctx, uri, "1"))
	require.NoError(s.T(), boundB.GetRequestURIStore().Create(s.ctx, uri, "1"))

	idA, err := boundA.GetRequestURIStore().GetIDByURI(s.ctx, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "1", idA)

	idB, err := boundB.GetRequestURIStore().GetIDByURI(s.ctx, uri)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "1", idB)

	// The other queue's mapping is invisible through this queue's binding.
	_, err = boundA.GetRequestURIStore().GetIDByURI(s.ctx, "git://remote/monorepo/shared/only-b")
	require.True(s.T(), storage.IsNotFound(err))
}

// MySQLQueueStoreSuite exercises the MySQL-backed QueueStore by embedding the shared contract suite.
type MySQLQueueStoreSuite struct {
	storagesuite.QueueStoreContractSuite
}

// MySQLValidationFactStoreSuite exercises the MySQL-backed ValidationFactStore by embedding the
// shared contract suite.
type MySQLValidationFactStoreSuite struct {
	storagesuite.ValidationFactStoreContractSuite
}

// MySQLBuildStoreSuite exercises the MySQL-backed BuildStore by embedding the shared contract suite.
type MySQLBuildStoreSuite struct {
	storagesuite.BuildStoreContractSuite
}

// MySQLRequestLogStoreSuite exercises the MySQL-backed RequestLogStore against a real MySQL instance.
type MySQLRequestLogStoreSuite struct {
	storagesuite.RequestLogStoreContractSuite
}

// mysqlFactory adapts the MySQL storage backend's queue binding to the
// storage.Factory seam for the contract suite, mirroring the host wiring.
type mysqlFactory struct {
	backend *mysqlstorage.Storage
}

// For returns the queue-scoped store aggregate bound to the queue named in config.
func (f mysqlFactory) For(config storage.Config) (storage.Storage, error) {
	return f.backend.For(config.QueueName)
}

func (s *MySQLRequestStoreSuite) TestMetadataUsesRequestVersion() {
	req := entity.Request{Queue: "monorepo/main", ID: "metadata", State: entity.RequestStateAccepted, Version: 1,
		Metadata: map[string]json.RawMessage{"other": json.RawMessage(`{ "z": 1, "keep": true }`)}}
	require.NoError(s.T(), s.store.Create(s.ctx, req))
	initial, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	initial.Metadata["notification"] = json.RawMessage(`{"thread":"123.456"}`)
	require.NoError(s.T(), s.store.Update(s.ctx, initial, initial.Version, initial.Version+1))
	require.Equal(s.T(), int32(1), initial.Version)

	stale := req
	stale.State = entity.RequestStateProcessing
	require.ErrorIs(s.T(), s.store.Update(s.ctx, stale, 1, 2), storage.ErrVersionMismatch)
	current, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), int32(2), current.Version)
	require.Equal(s.T(), entity.RequestStateAccepted, current.State)
	current.State = entity.RequestStateProcessing
	require.NoError(s.T(), s.store.Update(s.ctx, current, current.Version, current.Version+1))

	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), int32(3), got.Version)
	require.Equal(s.T(), entity.RequestStateProcessing, got.State)
	require.JSONEq(s.T(), `{"thread":"123.456"}`, string(got.Metadata["notification"]))
	require.JSONEq(s.T(), `{"keep":true,"z":1}`, string(got.Metadata["other"]))
	require.ErrorIs(s.T(), s.store.Update(s.ctx, initial, 1, 2), storage.ErrVersionMismatch)

	otherQueue, err := s.backend.For("different/queue")
	require.NoError(s.T(), err)
	got.Queue = "different/queue"
	require.ErrorIs(s.T(), otherQueue.GetRequestStore().Update(s.ctx, got, 3, 4), storage.ErrVersionMismatch)
}

func (s *MySQLRequestStoreSuite) TestMetadataLegacyNullAndConcurrentClaims() {
	req := entity.Request{Queue: "monorepo/main", ID: "metadata-legacy", State: entity.RequestStateAccepted, Version: 1}
	require.NoError(s.T(), s.store.Create(s.ctx, req))
	_, err := s.db.ExecContext(s.ctx, "UPDATE request SET metadata = NULL WHERE queue = ? AND id = ?", req.Queue, req.ID)
	require.NoError(s.T(), err)
	current, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Empty(s.T(), current.Metadata)
	var winners atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 10; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			claim := current
			claim.Metadata = map[string]json.RawMessage{"claim": json.RawMessage(`true`)}
			err := s.store.Update(s.ctx, claim, current.Version, current.Version+1)
			if err == nil {
				winners.Add(1)
			} else {
				assert.ErrorIs(s.T(), err, storage.ErrVersionMismatch)
			}
		}()
	}
	workers.Wait()
	require.Equal(s.T(), int32(1), winners.Load())
	got, err := s.store.Get(s.ctx, req.ID)
	require.NoError(s.T(), err)
	require.Equal(s.T(), req.Version+1, got.Version)
	require.Equal(s.T(), req.State, got.State)
	require.JSONEq(s.T(), `true`, string(got.Metadata["claim"]))
}
