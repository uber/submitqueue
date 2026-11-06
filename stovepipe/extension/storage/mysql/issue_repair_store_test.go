// Copyright (c) 2026 Uber Technologies, Inc.
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
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestIssueRepairPendingLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewIssueRepairStore(db, "queue")
	issue := entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task", UnitID: "unit"}
	pending := entity.IssueRepair{Issue: issue, ExecutorID: "executor", Version: 1}
	mock.ExpectExec("INSERT INTO issue_repair").WithArgs("queue", "build", "task", "unit", "executor", "", "", []byte("{}"), 1).WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, store.Create(context.Background(), pending))
	mock.ExpectExec("INSERT INTO issue_repair").WillReturnError(&mysql.MySQLError{Number: 1062})
	require.ErrorIs(t, store.Create(context.Background(), pending), storage.ErrAlreadyExists)

	metadata := map[string]string{"state": `{"attempt":2,"workflow":"run/1"}`}
	metadataJSON := []byte(`{"state":"{\"attempt\":2,\"workflow\":\"run/1\"}"}`)
	pending.Metadata = metadata
	mock.ExpectExec("UPDATE issue_repair").WithArgs("", "", metadataJSON, 2, "queue", "build", "task", "unit", "executor", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, store.Update(context.Background(), pending, 1, 2))
	require.Equal(t, int32(1), pending.Version)

	mock.ExpectQuery("SELECT fix_id, skip_reason, metadata, version FROM issue_repair").WithArgs("queue", "build", "task", "unit", "executor").WillReturnRows(sqlmock.NewRows([]string{"fix_id", "skip_reason", "metadata", "version"}).AddRow("", "", metadataJSON, 2))
	actual, err := store.Get(context.Background(), issue, "executor")
	require.NoError(t, err)
	require.Equal(t, metadata, actual.Metadata)
	actual.FixID = "shared-fix"
	mock.ExpectExec("UPDATE issue_repair").WithArgs("shared-fix", "", metadataJSON, 3, "queue", "build", "task", "unit", "executor", 2).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, store.Update(context.Background(), actual, 2, 3))

	mock.ExpectQuery("SELECT fix_id, skip_reason, metadata, version FROM issue_repair").WithArgs("queue", "build", "task", "unit", "other").WillReturnRows(sqlmock.NewRows([]string{"fix_id", "skip_reason", "metadata", "version"}))
	_, err = store.Get(context.Background(), issue, "other")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIssueRepairUpdateConflicts(t *testing.T) {
	for _, skipped := range []bool{false, true} {
		t.Run(map[bool]string{false: "submitted", true: "skipped"}[skipped], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueRepairStore(db, "queue")
			repair := entity.IssueRepair{Issue: entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task"}, ExecutorID: "executor", Version: 1, FixID: "fix"}
			if skipped {
				repair.FixID, repair.SkipReason = "", "unsupported"
			}
			mock.ExpectExec("UPDATE issue_repair").WithArgs(repair.FixID, repair.SkipReason, []byte("{}"), 2, "queue", "build", "task", "", "executor", 1).WillReturnResult(sqlmock.NewResult(0, 1))
			require.NoError(t, store.Update(context.Background(), repair, 1, 2))
			mock.ExpectExec("UPDATE issue_repair").WithArgs(repair.FixID, repair.SkipReason, []byte("{}"), 3, "queue", "build", "task", "", "executor", 2).WillReturnResult(sqlmock.NewResult(0, 0))
			require.ErrorIs(t, store.Update(context.Background(), repair, 2, 3), storage.ErrVersionMismatch)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueRepairEmptyMetadata(t *testing.T) {
	for _, tt := range []struct {
		name     string
		metadata map[string]string
		stored   string
	}{
		{name: "nil", stored: "{}"},
		{name: "empty", metadata: map[string]string{}, stored: "{}"},
		{name: "JSON null", stored: "null"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueRepairStore(db, "queue")
			issue := entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task"}
			repair := entity.IssueRepair{Issue: issue, ExecutorID: "executor", Metadata: tt.metadata, Version: 1}
			mock.ExpectExec("INSERT INTO issue_repair").WithArgs("queue", "build", "task", "", "executor", "", "", []byte("{}"), 1).WillReturnResult(sqlmock.NewResult(1, 1))
			require.NoError(t, store.Create(context.Background(), repair))
			require.Equal(t, tt.metadata, repair.Metadata)
			mock.ExpectQuery("SELECT fix_id, skip_reason, metadata, version FROM issue_repair").WillReturnRows(sqlmock.NewRows([]string{"fix_id", "skip_reason", "metadata", "version"}).AddRow("", "", tt.stored, 1))
			actual, err := store.Get(context.Background(), issue, "executor")
			require.NoError(t, err)
			require.Equal(t, map[string]string{}, actual.Metadata)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueRepairInvalidMetadata(t *testing.T) {
	for _, stored := range []string{`{`, `[]`, `{"state":{"attempt":2}}`} {
		t.Run(stored, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueRepairStore(db, "queue")
			mock.ExpectQuery("SELECT fix_id, skip_reason, metadata, version FROM issue_repair").WillReturnRows(sqlmock.NewRows([]string{"fix_id", "skip_reason", "metadata", "version"}).AddRow("fix", "", stored, 1))
			_, err = store.Get(context.Background(), entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task"}, "executor")
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueRepairValidation(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewIssueRepairStore(db, "queue")
	for _, repair := range []entity.IssueRepair{
		{},
		{Issue: entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task"}, ExecutorID: "executor"},
		{Issue: entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task"}, ExecutorID: "executor", FixID: "fix", SkipReason: "skip", Version: 1},
	} {
		require.Error(t, store.Create(context.Background(), repair))
	}
	repair := entity.IssueRepair{Issue: entity.OpenIssue{FirstSeenBuildID: "build", TaskID: "task"}, ExecutorID: "executor", Version: 1}
	require.Error(t, store.Update(context.Background(), repair, 0, 2))
	require.Error(t, store.Update(context.Background(), repair, 1, 0))
}
