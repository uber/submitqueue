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
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func testMetrics() tally.Scope {
	return tally.NoopScope
}

func setupSignatureStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, tango.SignatureStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewSignatureStore(db, testMetrics(), "monorepo")

	return db, mock, store
}

func testTargetSignature() tango.TargetSignature {
	return tango.TargetSignature{
		Queue:        "monorepo",
		BatchID:      "1",
		BaseRevision: "9f2c1d0e3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d",
		Targets: []tango.SignatureTarget{
			{ID: 10, Distance: 0},
			{ID: 20, Distance: 1},
		},
		CreatedAt: 1700000000000,
	}
}

func TestTargetSignatureStore_Create(t *testing.T) {
	signature := testTargetSignature()
	mismatched := signature
	mismatched.Queue = "other-queue"

	tests := []struct {
		name      string
		signature tango.TargetSignature
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name:      "success",
			signature: signature,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_signature").
					WithArgs("monorepo", "1", signature.BaseRevision, encodeSignatureTargets(signature.Targets), signature.CreatedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			name:      "empty signature is written as an empty value",
			signature: tango.TargetSignature{Queue: "monorepo", BatchID: "2", BaseRevision: signature.BaseRevision, CreatedAt: 5},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_signature").
					WithArgs("monorepo", "2", signature.BaseRevision, []byte{}, int64(5)).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			name:      "duplicate batch",
			signature: signature,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_signature").
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name:      "insert error",
			signature: signature,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_signature").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name:      "queue differs from the bound queue",
			signature: mismatched,
			setup:     func(mock sqlmock.Sqlmock) {},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSignatureStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Create(context.Background(), tt.signature)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTargetSignatureStore_Get(t *testing.T) {
	want := testTargetSignature()
	columns := []string{"base_revision", "targets", "created_at"}

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		want      tango.TargetSignature
		wantFound bool
		wantErr   bool
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT base_revision, targets, created_at FROM target_signature").
					WithArgs("monorepo", want.BatchID).
					WillReturnRows(sqlmock.NewRows(columns).
						AddRow(want.BaseRevision, encodeSignatureTargets(want.Targets), want.CreatedAt))
			},
			want:      want,
			wantFound: true,
		},
		{
			name: "found with no targets",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT base_revision, targets, created_at FROM target_signature").
					WithArgs("monorepo", want.BatchID).
					WillReturnRows(sqlmock.NewRows(columns).
						AddRow(want.BaseRevision, []byte{}, want.CreatedAt))
			},
			want: tango.TargetSignature{
				Queue:        want.Queue,
				BatchID:      want.BatchID,
				BaseRevision: want.BaseRevision,
				CreatedAt:    want.CreatedAt,
			},
			wantFound: true,
		},
		{
			name: "not found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT base_revision, targets, created_at FROM target_signature").
					WithArgs("monorepo", want.BatchID).
					WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT base_revision, targets, created_at FROM target_signature").
					WithArgs("monorepo", want.BatchID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "stored targets cannot be decoded",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT base_revision, targets, created_at FROM target_signature").
					WithArgs("monorepo", want.BatchID).
					WillReturnRows(sqlmock.NewRows(columns).
						AddRow(want.BaseRevision, []byte{0x01, 0x02, 0x03}, want.CreatedAt))
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSignatureStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, found, err := store.Get(context.Background(), want.BatchID)

			if tt.wantErr {
				require.Error(t, err)
				assert.False(t, found)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantFound, found)
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
