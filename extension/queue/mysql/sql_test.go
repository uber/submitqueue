package mysql

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/extension/queue"
)

func TestNewQueue(t *testing.T) {
	t.Run("success with all params", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		q, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
		})

		require.NoError(t, err)
		require.NotNil(t, q)
		assert.NotNil(t, q.Publisher())
		assert.NotNil(t, q.Subscriber())

		err = q.Close()
		assert.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("error when DB ping fails", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing().WillReturnError(sql.ErrConnDone)

		q, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
		})

		require.Error(t, err)
		assert.Nil(t, q)

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestQueue_Publisher(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectPing()

	q, err := NewQueue(Params{
		DB:           db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NewTestScope("test", nil),
	})
	require.NoError(t, err)
	defer q.Close()

	// First call creates publisher
	pub1 := q.Publisher()
	assert.NotNil(t, pub1)

	// Second call returns same publisher (singleton)
	pub2 := q.Publisher()
	assert.Equal(t, pub1, pub2)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueue_Subscriber(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectPing()

	q, err := NewQueue(Params{
		DB:           db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NewTestScope("test", nil),
	})
	require.NoError(t, err)
	defer q.Close()

	// First call creates subscriber
	sub1 := q.Subscriber()
	assert.NotNil(t, sub1)

	// Second call returns same subscriber (singleton)
	sub2 := q.Subscriber()
	assert.Equal(t, sub1, sub2)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueue_Close(t *testing.T) {
	tests := []struct {
		name       string
		setupQueue func(t *testing.T, q queue.Queue)
		wantErr    bool
	}{
		{
			name:       "close without creating publisher or subscriber",
			setupQueue: func(t *testing.T, q queue.Queue) {},
			wantErr:    false,
		},
		{
			name: "close after creating publisher",
			setupQueue: func(t *testing.T, q queue.Queue) {
				_ = q.Publisher()
			},
			wantErr: false,
		},
		{
			name: "close after creating subscriber",
			setupQueue: func(t *testing.T, q queue.Queue) {
				_ = q.Subscriber()
			},
			wantErr: false,
		},
		{
			name: "close after creating both publisher and subscriber",
			setupQueue: func(t *testing.T, q queue.Queue) {
				_ = q.Publisher()
				_ = q.Subscriber()
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			require.NoError(t, err)
			defer db.Close()

			mock.ExpectPing()

			q, err := NewQueue(Params{
				DB:           db,
				Logger:       zaptest.NewLogger(t),
				MetricsScope: tally.NewTestScope("test", nil),
			})
			require.NoError(t, err)

			// Setup queue state
			tt.setupQueue(t, q)

			// Close queue
			err = q.Close()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("close is idempotent", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		q, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
		})
		require.NoError(t, err)

		// Close multiple times
		err = q.Close()
		assert.NoError(t, err)

		err = q.Close()
		assert.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("publisher and subscriber calls after close return same instances", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		q, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
		})
		require.NoError(t, err)

		// Create publisher before close
		pub := q.Publisher()
		assert.NotNil(t, pub)

		// Close queue
		err = q.Close()
		assert.NoError(t, err)

		// Getting publisher/subscriber after close should return the same instances
		// (they were already created, so singleton pattern returns them)
		pub2 := q.Publisher()
		assert.Equal(t, pub, pub2, "should return same publisher instance after close")

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestQueue_Integration(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectPing()

	logger := zaptest.NewLogger(t)
	metricsScope := tally.NewTestScope("test", nil)

	q, err := NewQueue(Params{
		DB:           db,
		Logger:       logger,
		MetricsScope: metricsScope,
	})
	require.NoError(t, err)
	defer q.Close()

	// Verify we can get both publisher and subscriber
	publisher := q.Publisher()
	subscriber := q.Subscriber()

	assert.NotNil(t, publisher)
	assert.NotNil(t, subscriber)

	// Verify they're singletons
	assert.Equal(t, publisher, q.Publisher())
	assert.Equal(t, subscriber, q.Subscriber())

	// Close should succeed
	err = q.Close()
	assert.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}
