package sql

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/extensions/queue"
)

func TestNewQueue(t *testing.T) {
	t.Run("success with all params", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		factory, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
			Config:       DefaultConfig("test-consumer", "test-worker"),
		})

		require.NoError(t, err)
		require.NotNil(t, factory)
		assert.NotNil(t, factory.Publisher())
		assert.NotNil(t, factory.Subscriber())

		err = factory.Close()
		assert.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("error when config is invalid", func(t *testing.T) {
		db, _, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		config := DefaultConfig("", "") // Invalid: empty consumer group and worker ID

		factory, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
			Config:       config,
		})

		require.Error(t, err)
		assert.Nil(t, factory)
	})

	t.Run("error when DB ping fails", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing().WillReturnError(sql.ErrConnDone)

		factory, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
			Config:       DefaultConfig("test-consumer", "test-worker"),
		})

		require.Error(t, err)
		assert.Nil(t, factory)

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestQueue_Publisher(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectPing()

	factory, err := NewQueue(Params{
		DB:           db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NewTestScope("test", nil),
		Config:       DefaultConfig("test-consumer", "test-worker"),
	})
	require.NoError(t, err)
	defer factory.Close()

	// First call creates publisher
	pub1 := factory.Publisher()
	assert.NotNil(t, pub1)

	// Second call returns same publisher (singleton)
	pub2 := factory.Publisher()
	assert.Equal(t, pub1, pub2)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueue_Subscriber(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectPing()

	factory, err := NewQueue(Params{
		DB:           db,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NewTestScope("test", nil),
		Config:       DefaultConfig("test-consumer", "test-worker"),
	})
	require.NoError(t, err)
	defer factory.Close()

	// First call creates subscriber
	sub1 := factory.Subscriber()
	assert.NotNil(t, sub1)

	// Second call returns same subscriber (singleton)
	sub2 := factory.Subscriber()
	assert.Equal(t, sub1, sub2)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueue_Close(t *testing.T) {
	tests := []struct {
		name         string
		setupFactory func(t *testing.T, f queue.Queue)
		wantErr      bool
	}{
		{
			name:         "close without creating publisher or subscriber",
			setupFactory: func(t *testing.T, f queue.Queue) {},
			wantErr:      false,
		},
		{
			name: "close after creating publisher",
			setupFactory: func(t *testing.T, f queue.Queue) {
				_ = f.Publisher()
			},
			wantErr: false,
		},
		{
			name: "close after creating subscriber",
			setupFactory: func(t *testing.T, f queue.Queue) {
				_ = f.Subscriber()
			},
			wantErr: false,
		},
		{
			name: "close after creating both publisher and subscriber",
			setupFactory: func(t *testing.T, f queue.Queue) {
				_ = f.Publisher()
				_ = f.Subscriber()
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

			factory, err := NewQueue(Params{
				DB:           db,
				Logger:       zaptest.NewLogger(t),
				MetricsScope: tally.NewTestScope("test", nil),
				Config:       DefaultConfig("test-consumer", "test-worker"),
			})
			require.NoError(t, err)

			// Setup factory state
			tt.setupFactory(t, factory)

			// Close factory
			err = factory.Close()
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

		factory, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
			Config:       DefaultConfig("test-consumer", "test-worker"),
		})
		require.NoError(t, err)

		// Close multiple times
		err = factory.Close()
		assert.NoError(t, err)

		err = factory.Close()
		assert.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("publisher and subscriber calls after close return same instances", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectPing()

		factory, err := NewQueue(Params{
			DB:           db,
			Logger:       zaptest.NewLogger(t),
			MetricsScope: tally.NewTestScope("test", nil),
			Config:       DefaultConfig("test-consumer", "test-worker"),
		})
		require.NoError(t, err)

		// Create publisher before close
		pub := factory.Publisher()
		assert.NotNil(t, pub)

		// Close factory
		err = factory.Close()
		assert.NoError(t, err)

		// Getting publisher/subscriber after close should return the same instances
		// (they were already created, so singleton pattern returns them)
		pub2 := factory.Publisher()
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
	config := DefaultConfig("test-consumer", "test-worker")

	factory, err := NewQueue(Params{
		DB:           db,
		Logger:       logger,
		MetricsScope: metricsScope,
		Config:       config,
	})
	require.NoError(t, err)
	defer factory.Close()

	// Verify we can get both publisher and subscriber
	publisher := factory.Publisher()
	subscriber := factory.Subscriber()

	assert.NotNil(t, publisher)
	assert.NotNil(t, subscriber)

	// Verify they're singletons
	assert.Equal(t, publisher, factory.Publisher())
	assert.Equal(t, subscriber, factory.Subscriber())

	// Close should succeed
	err = factory.Close()
	assert.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}
