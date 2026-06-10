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
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	mysqlstorage "github.com/uber/submitqueue/submitqueue/extension/storage/mysql"
	storagesuite "github.com/uber/submitqueue/test/integration/submitqueue/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// MySQLStorageIntegrationSuite tests the MySQL storage implementation
// by embedding the shared contract suite.
type MySQLStorageIntegrationSuite struct {
	storagesuite.StorageContractSuite
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestMySQLStorageIntegration(t *testing.T) {
	suite.Run(t, new(MySQLStorageIntegrationSuite))
}

func (s *MySQLStorageIntegrationSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	s.log.Logf("Starting MySQL Storage integration test suite using docker-compose")

	// Use docker-compose to start MySQL (schema applied programmatically)
	s.stack = testutil.NewComposeStack(
		t,
		s.log,
		ctx,
		"docker-compose.yml",
		"ext-submitqueue-storage-mysql", // Test context for meaningful container names
	)

	// Start the compose stack (MySQL only, no schema)
	err := s.stack.Up()
	require.NoError(t, err, "failed to start compose stack")

	s.log.Logf("Compose stack started successfully")

	// Connect to MySQL for schema application
	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")

	// Apply schemas programmatically from directory
	schemaDir := testutil.SchemaDir("submitqueue/extension/storage/mysql/schema")
	testutil.ApplySchema(t, s.log, s.db, schemaDir)

	s.log.Logf("Schemas applied successfully")

	// Create storage instance using the existing database connection
	store, err := mysqlstorage.NewStorage(s.db, tally.NoopScope)
	require.NoError(t, err, "failed to create storage")

	// Provide the storage instance to the contract suite
	s.SetContext(ctx)
	s.SetStorage(store)
	s.SetLogger(s.log)

	t.Cleanup(func() {
		if s.db != nil {
			s.log.Logf("Closing MySQL connection")
			s.db.Close()
		}
	})

	s.log.Logf("MySQL Storage integration test suite ready")
}

func (s *MySQLStorageIntegrationSuite) TearDownSuite() {
	s.log.Logf("Tearing down MySQL Storage integration test suite")
	// Cleanup handled automatically by testutil.ComposeStack
}

// countActiveBatchRows returns the number of active_batch membership rows for the
// given (queue, batch_id) — index state internal to the MySQL impl, not visible
// through the storage.Storage contract.
func (s *MySQLStorageIntegrationSuite) countActiveBatchRows(queue, batchID string) int {
	t := s.T()
	var n int
	err := s.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM active_batch WHERE queue = ? AND batch_id = ?",
		queue, batchID,
	).Scan(&n)
	require.NoError(t, err)
	return n
}

// TestActiveBatch_SelfHealsTerminalMembership verifies that ListActive deletes the
// membership row of a batch that has reached a terminal state.
func (s *MySQLStorageIntegrationSuite) TestActiveBatch_SelfHealsTerminalMembership() {
	t := s.T()
	ctx := context.Background()
	const queue = "bq-selfheal-terminal"
	const id = queue + "/batch/1"

	store := s.GetStorage().GetBatchStore()
	require.NoError(t, store.Create(ctx, entity.Batch{ID: id, Queue: queue, Contains: []string{id + "/req"}, Dependencies: []string{}, State: entity.BatchStateCreated, Version: 1}))
	require.Equal(t, 1, s.countActiveBatchRows(queue, id), "Create should record active membership")

	require.NoError(t, store.UpdateState(ctx, id, 1, 2, entity.BatchStateSucceeded))

	active, err := store.ListActive(ctx, queue)
	require.NoError(t, err)
	require.Empty(t, active, "terminal batch should not be listed as active")
	require.Equal(t, 0, s.countActiveBatchRows(queue, id), "ListActive should self-heal the stale membership row")
}

// TestActiveBatch_SkipsDanglingMembershipWithoutDeleting verifies that ListActive
// skips a membership row whose batch does not exist but does NOT delete it: it may
// belong to an in-flight Create that hasn't written its batch row yet.
func (s *MySQLStorageIntegrationSuite) TestActiveBatch_SkipsDanglingMembershipWithoutDeleting() {
	t := s.T()
	ctx := context.Background()
	const queue = "bq-dangling-skip"
	const id = queue + "/batch/ghost"

	_, err := s.db.ExecContext(ctx, "INSERT INTO active_batch (queue, batch_id) VALUES (?, ?)", queue, id)
	require.NoError(t, err)

	active, err := s.GetStorage().GetBatchStore().ListActive(ctx, queue)
	require.NoError(t, err)
	require.Empty(t, active, "dangling membership should not surface a batch")
	require.Equal(t, 1, s.countActiveBatchRows(queue, id), "ListActive must NOT delete a missing-batch membership (it may belong to an in-flight create)")
}

// TestActiveBatch_CreateKeepsMembershipOnDuplicate verifies that a duplicate Create
// (ErrAlreadyExists) does NOT delete the membership row, which belongs to the live
// existing batch.
func (s *MySQLStorageIntegrationSuite) TestActiveBatch_CreateKeepsMembershipOnDuplicate() {
	t := s.T()
	ctx := context.Background()
	const queue = "bq-create-dup"
	const id = queue + "/batch/1"
	store := s.GetStorage().GetBatchStore()

	b := entity.Batch{ID: id, Queue: queue, Contains: []string{id + "/req"}, Dependencies: []string{}, State: entity.BatchStateCreated, Version: 1}
	require.NoError(t, store.Create(ctx, b))
	require.Equal(t, 1, s.countActiveBatchRows(queue, id))

	require.ErrorIs(t, store.Create(ctx, b), storage.ErrAlreadyExists)
	require.Equal(t, 1, s.countActiveBatchRows(queue, id), "duplicate Create must NOT delete the existing batch's membership")

	active, err := store.ListActive(ctx, queue)
	require.NoError(t, err)
	require.Len(t, active, 1, "the original batch must remain active")
}

// TestActiveBatch_CreateKeepsMembershipOnFailedInsert verifies that a non-duplicate
// batch-insert failure leaves the membership row in place rather than deleting it
// (the batch row may have committed despite the error). The failure is induced by
// dropping the batch table (restored afterwards).
func (s *MySQLStorageIntegrationSuite) TestActiveBatch_CreateKeepsMembershipOnFailedInsert() {
	t := s.T()
	ctx := context.Background()
	const queue = "bq-create-fail"
	const id = queue + "/batch/1"

	var tbl, ddl string
	require.NoError(t, s.db.QueryRowContext(ctx, "SHOW CREATE TABLE batch").Scan(&tbl, &ddl))
	_, err := s.db.ExecContext(ctx, "DROP TABLE batch")
	require.NoError(t, err)
	defer func() {
		_, derr := s.db.ExecContext(context.Background(), ddl)
		require.NoError(t, derr, "must restore the batch table for subsequent tests")
	}()

	err = s.GetStorage().GetBatchStore().Create(ctx, entity.Batch{ID: id, Queue: queue, Contains: []string{id + "/req"}, Dependencies: []string{}, State: entity.BatchStateCreated, Version: 1})
	require.Error(t, err, "Create should fail when the batch table is missing")
	require.NotErrorIs(t, err, storage.ErrAlreadyExists, "the failure must be a non-duplicate error")

	require.Equal(t, 1, s.countActiveBatchRows(queue, id), "Create must NOT delete the membership on a failed insert: the batch row may have committed despite the error")
}
