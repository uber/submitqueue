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
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"
)

func newTestDeliveryStateStoreWithMock(t *testing.T) (deliveryStateStore, *sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	store := newDeliveryStateStore(db, zaptest.NewLogger(t).Sugar(), tally.NoopScope)
	return store, db, mock
}

func TestDeliveryStateStore_MarkDelivered(t *testing.T) {
	tests := []struct {
		name           string
		execErr        bool
		queryErr       bool
		wantRetryCount int
	}{
		{
			name:           "success new insert returns 0",
			wantRetryCount: 0,
		},
		{
			name:           "success redelivery returns incremented count",
			wantRetryCount: 3,
		},
		{
			name:    "exec error",
			execErr: true,
		},
		{
			name:     "query error on retry_count SELECT",
			queryErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			if tt.execErr {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5), sqlmock.AnyArg()).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))

				if tt.queryErr {
					mock.ExpectQuery("SELECT retry_count FROM queue_delivery_state").
						WithArgs("group-1", "orders", "part-1", int64(5)).
						WillReturnError(assert.AnError)
				} else {
					mock.ExpectQuery("SELECT retry_count FROM queue_delivery_state").
						WithArgs("group-1", "orders", "part-1", int64(5)).
						WillReturnRows(sqlmock.NewRows([]string{"retry_count"}).AddRow(tt.wantRetryCount))
				}
			}

			retryCount, err := store.MarkDelivered(context.Background(), "group-1", "orders", "part-1", 5, 30000)

			if tt.execErr || tt.queryErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantRetryCount, retryCount)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_ExtendVisibility(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "success",
			wantErr: false,
		},
		{
			name:    "db error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectExec("UPDATE queue_delivery_state").
					WithArgs(sqlmock.AnyArg(), "group-1", "orders", "part-1", int64(5)).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("UPDATE queue_delivery_state").
					WithArgs(sqlmock.AnyArg(), "group-1", "orders", "part-1", int64(5)).
					WillReturnResult(sqlmock.NewResult(0, 1))
			}

			err := store.ExtendVisibility(context.Background(), "group-1", "orders", "part-1", 5, 60000)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkAcked(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "success",
			wantErr: false,
		},
		{
			name:    "db error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5)).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5)).
					WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err := store.MarkAcked(context.Background(), "group-1", "orders", "part-1", 5)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_MarkNacked(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "success",
			wantErr: false,
		},
		{
			name:    "db error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5), sqlmock.AnyArg()).
					WillReturnError(assert.AnError)
			} else {
				mock.ExpectExec("INSERT INTO queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			}

			err := store.MarkNacked(context.Background(), "group-1", "orders", "part-1", 5, 5000)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_GetDeliveryState(t *testing.T) {
	tests := []struct {
		name           string
		acked          bool
		invisibleUntil int64
		retryCount     int
		noRows         bool
		wantErr        bool
		wantFound      bool
	}{
		{
			name:      "no delivery state row returns not found",
			noRows:    true,
			wantFound: false,
		},
		{
			name:           "acked message",
			acked:          true,
			invisibleUntil: 0,
			retryCount:     2,
			wantFound:      true,
		},
		{
			name:           "in-flight message",
			acked:          false,
			invisibleUntil: 9999999999999,
			retryCount:     1,
			wantFound:      true,
		},
		{
			name:           "expired visibility",
			acked:          false,
			invisibleUntil: 1000,
			retryCount:     3,
			wantFound:      true,
		},
		{
			name:    "db error",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectQuery("SELECT acked, invisible_until, retry_count FROM queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5)).
					WillReturnError(assert.AnError)
			} else if tt.noRows {
				mock.ExpectQuery("SELECT acked, invisible_until, retry_count FROM queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5)).
					WillReturnRows(sqlmock.NewRows([]string{"acked", "invisible_until", "retry_count"}))
			} else {
				mock.ExpectQuery("SELECT acked, invisible_until, retry_count FROM queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", int64(5)).
					WillReturnRows(sqlmock.NewRows([]string{"acked", "invisible_until", "retry_count"}).
						AddRow(tt.acked, tt.invisibleUntil, tt.retryCount))
			}

			state, found, err := store.GetDeliveryState(context.Background(), "group-1", "orders", "part-1", 5)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantFound, found)
				if found {
					assert.Equal(t, tt.acked, state.Acked)
					assert.Equal(t, tt.invisibleUntil, state.InvisibleUntil)
					assert.Equal(t, tt.retryCount, state.RetryCount)
				}
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeliveryStateStore_AdvanceWatermark(t *testing.T) {
	tests := []struct {
		name             string
		currentWatermark int64
		offsets          []int64 // message offsets passed in (from messageStore)
		dsRows           []struct {
			offset int64
			acked  bool
		}
		dsQueryErr      bool
		expectWatermark int64
		expectCleanup   bool
	}{
		{
			name:             "no offsets",
			currentWatermark: 5,
			offsets:          nil,
			expectWatermark:  5,
			expectCleanup:    false,
		},
		{
			name:             "acked offsets advance watermark",
			currentWatermark: 5,
			offsets:          []int64{6, 7, 8},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				{offset: 7, acked: true},
				{offset: 8, acked: true},
			},
			expectWatermark: 8,
			expectCleanup:   true,
		},
		{
			name:             "gap in offsets does not block advancement",
			currentWatermark: 5,
			offsets:          []int64{6, 8}, // gap: offset 7 does not exist (AUTO_INCREMENT gap)
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				{offset: 8, acked: true},
			},
			expectWatermark: 8,
			expectCleanup:   true,
		},
		{
			name:             "non-acked offset stops advancement",
			currentWatermark: 5,
			offsets:          []int64{6, 7, 8},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				{offset: 7, acked: false}, // in-flight, not acked
				{offset: 8, acked: true},
			},
			expectWatermark: 6,
			expectCleanup:   true,
		},
		{
			name:             "undelivered message stops advancement",
			currentWatermark: 5,
			offsets:          []int64{6, 7},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: true},
				// offset 7 has no delivery state row (undelivered)
			},
			expectWatermark: 6,
			expectCleanup:   true,
		},
		{
			name:             "first offset not acked means no advancement",
			currentWatermark: 5,
			offsets:          []int64{6},
			dsRows: []struct {
				offset int64
				acked  bool
			}{
				{offset: 6, acked: false}, // in-flight
			},
			expectWatermark: 5,
			expectCleanup:   false,
		},
		{
			name:             "delivery state query error returns current watermark",
			currentWatermark: 5,
			offsets:          []int64{6, 7},
			dsQueryErr:       true,
			expectWatermark:  5,
			expectCleanup:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, db, mock := newTestDeliveryStateStoreWithMock(t)
			defer db.Close()

			// Delivery state query is only issued if there are offsets
			if len(tt.offsets) > 0 {
				dsArgs := make([]driver.Value, 0, 3+len(tt.offsets))
				dsArgs = append(dsArgs, "group-1", "orders", "part-1")
				for _, offset := range tt.offsets {
					dsArgs = append(dsArgs, offset)
				}

				if tt.dsQueryErr {
					mock.ExpectQuery("SELECT message_offset, acked FROM queue_delivery_state").
						WithArgs(dsArgs...).
						WillReturnError(assert.AnError)
				} else {
					dsResultRows := sqlmock.NewRows([]string{"message_offset", "acked"})
					for _, r := range tt.dsRows {
						dsResultRows.AddRow(r.offset, r.acked)
					}
					mock.ExpectQuery("SELECT message_offset, acked FROM queue_delivery_state").
						WithArgs(dsArgs...).
						WillReturnRows(dsResultRows)
				}
			}

			if tt.expectCleanup {
				mock.ExpectExec("DELETE FROM queue_delivery_state").
					WithArgs("group-1", "orders", "part-1", tt.expectWatermark).
					WillReturnResult(sqlmock.NewResult(0, tt.expectWatermark-tt.currentWatermark))
			}

			watermark, err := store.AdvanceWatermark(context.Background(), "group-1", "orders", "part-1", tt.currentWatermark, tt.offsets)

			if tt.dsQueryErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expectWatermark, watermark)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
