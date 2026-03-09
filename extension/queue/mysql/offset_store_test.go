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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const (
	testConsumerGroup  = "test-consumer"
	testSubscriberName = "test-subscriber"
)

func setupoffsetStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, offsetStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newOffsetStore(db, testMetrics())

	return db, mock, store
}

func TestOffsetStore_Initialize(t *testing.T) {
	db, mock, store := setupoffsetStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"

	mock.ExpectExec("INSERT IGNORE INTO queue_offsets").
		WithArgs(testConsumerGroup, topic, partitionKey, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := store.Initialize(ctx, topic, partitionKey, testConsumerGroup)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOffsetStore_GetAckedOffset(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(mock sqlmock.Sqlmock)
		expectedOffset int64
		wantErr        bool
	}{
		{
			name: "offset found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"offset_acked"}).AddRow(int64(100))
				mock.ExpectQuery("SELECT offset_acked FROM queue_offsets").
					WithArgs(testConsumerGroup, "test_topic", "part1").
					WillReturnRows(rows)
			},
			expectedOffset: 100,
			wantErr:        false,
		},
		{
			name: "offset not found returns zero",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT offset_acked FROM queue_offsets").
					WithArgs(testConsumerGroup, "test_topic", "part1").
					WillReturnError(sql.ErrNoRows)
			},
			expectedOffset: 0,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupoffsetStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"

			tt.setup(mock)

			offset, err := store.GetAckedOffset(ctx, topic, partitionKey, testConsumerGroup)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedOffset, offset)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestOffsetStore_UpdateAckedOffset(t *testing.T) {
	db, mock, store := setupoffsetStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	offset := int64(150)

	mock.ExpectExec("UPDATE queue_offsets").
		WithArgs(offset, sqlmock.AnyArg(), testConsumerGroup, topic, partitionKey, offset).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := store.UpdateAckedOffset(ctx, topic, partitionKey, offset, testConsumerGroup)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOffsetStore_GetMinAckedOffset(t *testing.T) {
	tests := []struct {
		name       string
		minOffset  int64
		queryErr   bool
		wantOffset int64
		wantFound  bool
		wantErr    bool
	}{
		{
			name:       "returns min offset across consumer groups",
			minOffset:  10,
			wantOffset: 10,
			wantFound:  true,
		},
		{
			name:      "no offset rows returns not found",
			minOffset: 0,
			wantFound: false,
		},
		{
			name:      "zero offset returns not found",
			minOffset: 0,
			wantFound: false,
		},
		{
			name:     "query error",
			queryErr: true,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupoffsetStoreTest(t)
			defer db.Close()

			if tt.queryErr {
				mock.ExpectQuery("SELECT COALESCE\\(MIN\\(offset_acked\\), 0\\) FROM queue_offsets").
					WithArgs("test_topic", "part-1").
					WillReturnError(fmt.Errorf("db error"))
			} else {
				mock.ExpectQuery("SELECT COALESCE\\(MIN\\(offset_acked\\), 0\\) FROM queue_offsets").
					WithArgs("test_topic", "part-1").
					WillReturnRows(sqlmock.NewRows([]string{"min"}).AddRow(tt.minOffset))
			}

			offset, found, err := store.GetMinAckedOffset(context.Background(), "test_topic", "part-1")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantFound, found)
				require.Equal(t, tt.wantOffset, offset)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
